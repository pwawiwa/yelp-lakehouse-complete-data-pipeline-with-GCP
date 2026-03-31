"""
Self-Healing Agent Brain (OpenAI GPT Edition).

Uses OpenAI's function-calling API to run a ReAct-style loop:
Detect → Diagnose → Fix → Retry → Report
"""
import os
import json
import logging
import requests
from openai import OpenAI

logger = logging.getLogger(__name__)

# ── Airflow REST API configuration ───────────────────────────────────────────
# NOTE: Credentials are read at call time (not module load) so that the operator
# can pre-load them from Airflow Variables into the environment before we run.
AIRFLOW_API_BASE = os.getenv("AIRFLOW_API_BASE", "http://host.docker.internal:8080/api/v2")
DAGS_DIR = os.getenv("AIRFLOW_HOME", "/usr/local/airflow") + "/dags"

# In-memory token cache for the duration of one agent run
_cached_token: str | None = None


def _get_bearer_token() -> str:
    """
    Airflow 3.0 requires JWT auth: POST /auth/token -> {access_token}.
    Caches the token in-process for the duration of one healing loop.
    """
    global _cached_token
    if _cached_token:
        return _cached_token

    # Check if a token was explicitly provided via Variable/env
    explicit_token = os.getenv("AIRFLOW_API_TOKEN")
    if explicit_token:
        _cached_token = explicit_token
        return _cached_token

    # Auto-login: exchange username+password for a JWT
    username = os.getenv("AIRFLOW_USERNAME", "admin")
    password = os.getenv("AIRFLOW_PASSWORD", "admin")
    auth_url = f"{AIRFLOW_API_BASE}/auth/token"

    logger.info(f"🔑 Fetching Airflow JWT from {auth_url} as '{username}'...")
    resp = requests.post(
        auth_url,
        json={"username": username, "password": password},
        timeout=15,
    )
    resp.raise_for_status()
    _cached_token = resp.json()["access_token"]
    logger.info("✅ Airflow JWT acquired successfully.")
    return _cached_token


def _auth_headers() -> dict:
    """Return the Authorization header using the cached Bearer token."""
    return {"Authorization": f"Bearer {_get_bearer_token()}"}


# ── Airflow REST API Helpers ──────────────────────────────────────────────────

def _airflow_get(endpoint: str, params: dict | None = None) -> dict:
    url = f"{AIRFLOW_API_BASE}/{endpoint}"
    resp = requests.get(url, headers=_auth_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _airflow_post(endpoint: str, data: dict | None = None) -> dict:
    url = f"{AIRFLOW_API_BASE}/{endpoint}"
    resp = requests.post(url, headers=_auth_headers(), json=data or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ── Tool Definitions (OpenAI Function Calling Schema) ─────────────────────────
# NOTE: OpenAI uses "parameters" (JSON Schema), Anthropic uses "input_schema".

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_failed_dag_runs",
            "description": "Get recent failed DAG runs. Returns list with dag_id, run_id, and execution_date.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "Optional DAG ID to filter by. Omit to scan all DAGs."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max number of runs to return. Default: 25."
                    }
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_task_instances",
            "description": "List task instances for a specific DAG run, showing states (success, failed, etc.).",
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "dag_run_id": {"type": "string"}
                },
                "required": ["dag_id", "dag_run_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_task_logs",
            "description": "Get the execution logs for a specific task instance. Essential for diagnosing failures.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "task_id": {"type": "string"},
                    "dag_run_id": {"type": "string"},
                    "try_number": {
                        "type": "integer",
                        "description": "Task attempt number. Default: 1."
                    }
                },
                "required": ["dag_id", "task_id", "dag_run_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "read_dag_file",
            "description": "Read a DAG Python file from the local dags/ directory. Read before writing.",
            "parameters": {
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "Filename within the dags/ directory (e.g., 'bronze_lake_ingest.py')."
                    }
                },
                "required": ["filename"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "write_dag_file",
            "description": "Write corrected contents to a DAG Python file to apply a fix. Always read first.",
            "parameters": {
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "content": {
                        "type": "string",
                        "description": "The complete corrected file contents."
                    }
                },
                "required": ["filename", "content"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "clear_failed_tasks",
            "description": "Clear (reset) failed task instances for a DAG run so Airflow will retry them automatically.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "dag_run_id": {"type": "string"}
                },
                "required": ["dag_id", "dag_run_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_import_errors",
            "description": "List DAG import errors in Airflow. Use after writing a fix to verify no syntax errors were introduced.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_dag_files",
            "description": "List all Python files in the dags/ directory.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    }
]


# ── Tool Execution Engine ─────────────────────────────────────────────────────

def _execute_tool(name: str, args: dict) -> str:
    """Dispatch a tool call to the appropriate Airflow REST API or filesystem operation."""
    try:
        if name == "get_failed_dag_runs":
            dag_id = args.get("dag_id")
            limit = args.get("limit", 25)
            # Airflow 3.0: use GET with query params for both single DAG and all DAGs
            endpoint = f"dags/{dag_id}/dagRuns" if dag_id else "dags/~/dagRuns"
            data = _airflow_get(
                endpoint,
                params={"state": "failed", "limit": limit, "order_by": "-execution_date"}
            )
            runs = data.get("dag_runs", [])
            result = [
                {
                    "dag_id": r["dag_id"],
                    "dag_run_id": r["dag_run_id"],
                    "state": r["state"],
                    "execution_date": r.get("execution_date")
                }
                for r in runs
            ]
            return json.dumps(result, indent=2, default=str)

        elif name == "get_task_instances":
            data = _airflow_get(
                f"dags/{args['dag_id']}/dagRuns/{args['dag_run_id']}/taskInstances"
            )
            instances = data.get("task_instances", [])
            result = [
                {
                    "task_id": t["task_id"],
                    "state": t["state"],
                    "try_number": t.get("try_number"),
                    "duration": t.get("duration")
                }
                for t in instances
            ]
            return json.dumps(result, indent=2, default=str)

        elif name == "get_task_logs":
            try_num = args.get("try_number", 1)
            data = _airflow_get(
                f"dags/{args['dag_id']}/dagRuns/{args['dag_run_id']}/taskInstances/{args['task_id']}/logs/{try_num}"
            )
            return data if isinstance(data, str) else json.dumps(data, indent=2)

        elif name == "read_dag_file":
            filepath = os.path.join(DAGS_DIR, args["filename"])
            # Security: prevent path traversal attacks
            if not os.path.realpath(filepath).startswith(os.path.realpath(DAGS_DIR)):
                return "Error: Path traversal not allowed."
            with open(filepath, "r") as f:
                return f.read()

        elif name == "write_dag_file":
            filepath = os.path.join(DAGS_DIR, args["filename"])
            if not os.path.realpath(filepath).startswith(os.path.realpath(DAGS_DIR)):
                return "Error: Path traversal not allowed."
            with open(filepath, "w") as f:
                f.write(args["content"])
            logger.info(f"✏️  Agent wrote fix to: {filepath}")
            return f"Successfully wrote fix to {filepath}"

        elif name == "clear_failed_tasks":
            data = _airflow_post(
                f"dags/{args['dag_id']}/clearTaskInstances",
                data={
                    "dag_run_id": args["dag_run_id"],
                    "include_downstream": True,
                    "only_failed": True,
                    "reset_dag_runs": True,
                }
            )
            return json.dumps(data, indent=2)

        elif name == "get_import_errors":
            data = _airflow_get("importErrors")
            return json.dumps(data.get("import_errors", []), indent=2)

        elif name == "list_dag_files":
            files = [f for f in os.listdir(DAGS_DIR) if f.endswith(".py")]
            return json.dumps(sorted(files))

        else:
            return f"Unknown tool: {name}"

    except requests.HTTPError as e:
        return f"Airflow API Error: {e.response.status_code} - {e.response.text}"
    except Exception as e:
        return f"Error ({type(e).__name__}): {e}"


# ── System Prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """
You are a Self-Healing Airflow Agent running inside a production data lakehouse.
Your mission is to detect, diagnose, and automatically repair failed Airflow DAG runs.

## Workflow (Follow this order exactly)
1. **Detect**: Call `get_failed_dag_runs` to find recent failures.
2. **Diagnose**: For each failed run:
   - Call `get_task_instances` to identify which tasks failed.
   - Call `get_task_logs` (on the failed task) to read the error traceback.
   - Call `read_dag_file` to see the current DAG source code.
   - Analyze the logs + source code together to determine the ROOT CAUSE.
3. **Fix**: ONLY if you are >90% confident in your diagnosis:
   - Call `write_dag_file` with the minimal, targeted correction. Do NOT refactor or rewrite unrelated code.
   - Immediately call `get_import_errors` to verify you didn't introduce a syntax error.
   - If there are new import errors, read the file again and fix the syntax error.
4. **Retry**: Call `clear_failed_tasks` to let Airflow automatically re-run the fixed tasks.
5. **Report**: Produce a concise final summary covering:
   - Which DAG runs failed.
   - What the root cause was.
   - What fix was applied (or why you chose not to fix it).
   - Whether a retry was triggered.

## Rules
- **Never guess**. If you cannot confidently identify the root cause from the logs, report it and recommend manual investigation.
- **Never delete** DAGs, connections, pools, or variables.
- **Always read** a file before writing to it.
- **Minimal changes only**. Fix only what is broken.
- If the fix requires infrastructure changes (GCP permissions, secrets, etc.), describe the change needed but do NOT attempt it.
- Do not fix the `self_healing_agent` DAG itself.
"""


# ── Main Agent Loop ───────────────────────────────────────────────────────────

def run_healing_loop(
    dag_id: str | None = None,
    model: str = "gpt-4o",
    max_iterations: int = 20,
) -> str:
    """
    Run the OpenAI ReAct-style healing loop.

    Args:
        dag_id:         If set, only inspect this specific DAG. Otherwise scans all DAGs.
        model:          OpenAI model to use. Default: 'gpt-4o'. Set to 'gpt-5' when available.
        max_iterations: Safety cap to prevent infinite loops.

    Returns:
        A human-readable report of the agent's actions and findings.
    """
    client = OpenAI()  # Uses OPENAI_API_KEY from environment

    user_prompt = (
        f"Check DAG '{dag_id}' for failed runs. Diagnose and apply fixes."
        if dag_id
        else "Scan all DAGs for failed runs. Diagnose and fix any failures you find."
    )

    messages = [{"role": "user", "content": user_prompt}]
    final_report_parts = []

    for i in range(max_iterations):
        logger.info(f"🔄 Agent iteration {i + 1}/{max_iterations}")

        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": SYSTEM_PROMPT}] + messages,
            tools=TOOLS,
            tool_choice="auto",
            max_completion_tokens=4096,
        )

        message = response.choices[0].message
        messages.append(message)  # Append the assistant's full message object

        # If the model has finished (no more tool calls), extract the final report
        if response.choices[0].finish_reason == "stop" or not message.tool_calls:
            if message.content:
                final_report_parts.append(message.content)
            break

        # Process tool calls
        tool_results = []
        for tool_call in message.tool_calls:
            func_name = tool_call.function.name
            func_args = json.loads(tool_call.function.arguments)

            logger.info(f"🔧 Tool: {func_name}({json.dumps(func_args)})")
            result = _execute_tool(func_name, func_args)
            logger.info(f"   ↳ Result (first 300 chars): {result[:300]}...")

            tool_results.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": result,
            })

            # Capture any reasoning text in the report
            if message.content:
                final_report_parts.append(message.content)

        # Append all tool results in a single "tool" turn
        messages.extend(tool_results)

    return "\n\n".join(filter(None, final_report_parts)).strip() or "Agent completed with no findings."
