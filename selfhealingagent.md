This is a sophisticated setup, Ihsan. Reconstructing these files requires stitching together the REST API integration, the Anthropic tool-use loop, and the Airflow deferrable operator logic.

Here are the complete files based on the architecture shown in your screenshots.

1. requirements.txt
This ensures the environment has the necessary SDKs for Claude and API communication.

Plaintext
anthropic>=0.42.0
requests
pendulum
2. dags/self_healing_dag.py
The "Meta-DAG" that runs continuously to monitor your cluster.

Python
from airflow.sdk import DAG, Asset
from pendulum import datetime
from include.selfhealeoperator import SelfHealingOperator

with DAG(
    dag_id="self_healing_agent",
    start_date=datetime(2025, 4, 22),
    schedule="@continuous",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "platform", "retries": 0},
    tags=["self-healing", "platform"],
):

    SelfHealingOperator(
        task_id="detect_and_heal",
        cooldown_seconds=60,
    )
3. include/selfhealeoperator.py
The custom Operator that handles the execution loop and uses the Triggerer to save resources during cooldowns.

Python
import os
import logging
from airflow.models import BaseOperator, Variable
from airflow.triggers.temporal import TimeDeltaTrigger
from include.healeragent import run_healing_loop

logger = logging.getLogger("self_healing_operator")

class SelfHealingOperator(BaseOperator):
    def __init__(
        self,
        *,
        cooldown_seconds: int = 60,
        model: str = "claude-sonnet-4-20250514",
        target_dag_id: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cooldown_seconds = cooldown_seconds
        self.model = model
        self.target_dag_id = target_dag_id

    def execute(self, context):
        """Run the healing loop, then defer for the cooldown period."""
        self._ensure_api_key()

        # Resolve target from variable if not set at init
        target = self.target_dag_id or Variable.get(
            "self_healing_target_dag", default_var=None
        )
        model = Variable.get("self_healing_model", default_var=self.model)

        report = run_healing_loop(dag_id=target, model=model)

        logger.info("=" * 60)
        logger.info("SELF-HEALING AGENT REPORT")
        logger.info("=" * 60)
        logger.info(report)
        logger.info("=" * 60)

        # Push the report to XCom for downstream tasks
        context["ti"].xcom_push(key="report", value=report)
        context["ti"].xcom_push(key="target_dag", value=target or "all")

        # Defer - release the worker slot during cooldown
        self.defer(
            trigger=TimeDeltaTrigger(timedelta(seconds=self.cooldown_seconds)),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        """Called when the trigger fires. Just logs and completes."""
        logger.info(f"Cooldown of {self.cooldown_seconds}s complete.")
        logger.info("DAG will re-trigger via @continuous schedule.")

    def _ensure_api_key(self):
        """Set ANTHROPIC_API_KEY from Airflow variable if not in env."""
        if not os.getenv("ANTHROPIC_API_KEY"):
            api_key = Variable.get("ANTHROPIC_API_KEY", default_var=None)
            if api_key:
                os.environ["ANTHROPIC_API_KEY"] = api_key
4. include/healeragent.py
The "Brain" of the operation. This contains the tool definitions and the Anthropic ReAct loop.

Python
import os
import json
import logging
import requests
import anthropic

logger = logging.getLogger(__name__)

AIRFLOW_AUTH = (
    os.getenv("AIRFLOW_USERNAME", "admin"),
    os.getenv("AIRFLOW_PASSWORD", "admin"),
)
AIRFLOW_API_BASE = os.getenv("AIRFLOW_API_BASE", "http://localhost:8080/api/v1")
DAGS_DIR = os.getenv("AIRFLOW_HOME", "/usr/local/airflow") + "/dags"

# --- API Helpers ---

def _airflow_get(endpoint: str, params: dict | None = None) -> dict:
    url = f"{AIRFLOW_API_BASE}/{endpoint}"
    resp = requests.get(url, auth=AIRFLOW_AUTH, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def _airflow_post(endpoint: str, data: dict | None = None) -> dict:
    url = f"{AIRFLOW_API_BASE}/{endpoint}"
    resp = requests.post(url, auth=AIRFLOW_AUTH, json=data or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()

# --- Tool Definitions for Claude ---

TOOLS = [
    {
        "name": "get_failed_dag_runs",
        "description": "Get recent failed DAG runs. Returns list of failed runs with dag_id, run_id, and execution_date.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string", "description": "Optional DAG ID to filter by."},
                "limit": {"type": "integer", "description": "Max runs to return (default 25)."}
            },
            "required": []
        }
    },
    {
        "name": "get_task_instances",
        "description": "List task instances for a DAG run, showing their state (success, failed, etc.).",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "dag_run_id": {"type": "string"}
            },
            "required": ["dag_id", "dag_run_id"]
        }
    },
    {
        "name": "get_task_logs",
        "description": "Get the logs for a specific task instance try. Returns the log text.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "task_id": {"type": "string"},
                "dag_run_id": {"type": "string"},
                "try_number": {"type": "integer", "description": "Task try number (default 1)."}
            },
            "required": ["dag_id", "task_id", "dag_run_id"]
        }
    },
    {
        "name": "read_dag_file",
        "description": "Read a DAG file from the local dags directory. Returns file contents.",
        "input_schema": {
            "type": "object",
            "properties": {
                "filename": {"type": "string", "description": "The DAG filename (e.g., 'my_dag.py')."}
            },
            "required": ["filename"]
        }
    },
    {
        "name": "write_dag_file",
        "description": "Write updated contents to a DAG file to apply fixes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "filename": {"type": "string"},
                "content": {"type": "string", "description": "The full updated file contents."}
            },
            "required": ["filename", "content"]
        }
    },
    {
        "name": "clear_failed_tasks",
        "description": "Clear failed task instances for a DAG run so Airflow will retry them.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "dag_run_id": {"type": "string"}
            },
            "required": ["dag_id", "dag_run_id"]
        }
    },
    {
        "name": "get_import_errors",
        "description": "List any DAG import errors in Airflow. Useful for detecting syntax errors after a fix.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "list_dag_files",
        "description": "List all Python files in the dags/ directory.",
        "input_schema": {"type": "object", "properties": {}, "required": []}
    }
]

# --- Execution Engine ---

def _execute_tool(name: str, args: dict) -> str:
    try:
        if name == "get_failed_dag_runs":
            dag_id = args.get("dag_id")
            limit = args.get("limit", 25)
            if dag_id:
                data = _airflow_get(f"dags/{dag_id}/dagRuns", params={"state": "failed", "limit": limit, "order_by": "-execution_date"})
            else:
                data = _airflow_post("dags/~/dagRuns/list", data={"states": ["failed"], "page_limit": limit, "order_by": "-execution_date"})
            
            runs = data.get("dag_runs", [])
            result = [{"dag_id": r["dag_id"], "dag_run_id": r["dag_run_id"], "state": r["state"], "execution_date": r.get("execution_date")} for r in runs]
            return json.dumps(result, indent=2, default=str)

        elif name == "get_task_instances":
            data = _airflow_get(f"dags/{args['dag_id']}/dagRuns/{args['dag_run_id']}/taskInstances")
            instances = data.get("task_instances", [])
            result = [{"task_id": t["task_id"], "state": t["state"], "try_number": t.get("try_number"), "duration": t.get("duration")} for t in instances]
            return json.dumps(result, indent=2, default=str)

        elif name == "get_task_logs":
            try_num = args.get("try_number", 1)
            data = _airflow_get(f"dags/{args['dag_id']}/dagRuns/{args['dag_run_id']}/taskInstances/{args['task_id']}/logs/{try_num}")
            return data if isinstance(data, str) else json.dumps(data, indent=2)

        elif name == "read_dag_file":
            filepath = os.path.join(DAGS_DIR, args["filename"])
            if not os.path.realpath(filepath).startswith(os.path.realpath(DAGS_DIR)):
                return "Error: path traversal not allowed."
            with open(filepath, "r") as f:
                return f.read()

        elif name == "write_dag_file":
            filepath = os.path.join(DAGS_DIR, args["filename"])
            if not os.path.realpath(filepath).startswith(os.path.realpath(DAGS_DIR)):
                return "Error: path traversal not allowed."
            with open(filepath, "w") as f:
                f.write(args["content"])
            return f"Successfully wrote {filepath}"

        elif name == "clear_failed_tasks":
            data = _airflow_post(f"dags/{args['dag_id']}/clearTaskInstances", data={
                "dag_run_id": args["dag_run_id"],
                "include_downstream": True,
                "only_failed": True,
                "reset_dag_runs": True
            })
            return json.dumps(data, indent=2)

        elif name == "get_import_errors":
            data = _airflow_get("importErrors")
            errors = data.get("import_errors", [])
            return json.dumps(errors, indent=2)

        elif name == "list_dag_files":
            files = [f for f in os.listdir(DAGS_DIR) if f.endswith(".py")]
            return json.dumps(sorted(files))

        else:
            return f"Unknown tool: {name}"

    except requests.HTTPError as e:
        return f"Airflow API Error: {e.response.status_code} - {e.response.text}"
    except Exception as e:
        return f"Error {type(e).__name__}: {e}"

# --- System Prompt & Main Loop ---

SYSTEM_PROMPT = """
You are a self-healing Airflow agent running inside an Astronomer deployment.
Your job is to detect failed DAG runs, diagnose the root cause from task logs, fix the DAG source code, and clear failed tasks so Airflow retries them.

## Workflow
1. **Detect** - Call get_failed_dag_runs to find recent failures.
2. **Diagnose** - For each failed run:
   - Call get_task_instances to find which tasks failed.
   - Call get_task_logs to read the error output.
   - Call read_dag_file to see the current source code.
   - Determine the root cause from logs + source.
3. **Fix** - If you can confidently identify the bug:
   - Call write_dag_file with the corrected code.
   - Only make minimal, targeted fixes. Do not refactor unrelated code.
   - Call get_import_errors to verify no syntax errors were introduced.
4. **Retry** - Call clear_failed_tasks to let Airflow re-run the fixed tasks.
5. **Report** - Summarize: what failed, why, what you fixed, and whether a retry was triggered.

## Rules
- If you cannot confidently determine the root cause, report the issue and recommend manual investigation. Do NOT guess.
- Never delete DAGs, connections, or variables.
- If the fix requires infrastructure changes, describe what needs to change but do not attempt it.
- Always read the file before writing to it.
- Keep fixes minimal - change only what is broken.
"""

def run_healing_loop(dag_id: str | None = None, model: str = "claude-sonnet-4-20250514", max_iterations: int = 15) -> str:
    client = anthropic.Anthropic()
    
    user_prompt = f"Check the DAG '{dag_id}' for failed runs. Diagnose and fix them." if dag_id else "Scan all DAGs for recent failed runs. Diagnose and fix any failures you find."
    
    messages = [{"role": "user", "content": user_prompt}]
    final_report = ""

    for i in range(max_iterations):
        logger.info(f"Agent iteration {i + 1}/{max_iterations}")
        
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        assistant_content = response.content
        messages.append({"role": "assistant", "content": assistant_content})

        if response.stop_reason == "end_turn":
            for block in assistant_content:
                if hasattr(block, "text"):
                    final_report += block.text + "\n"
            break

        tool_results = []
        for block in assistant_content:
            if block.type == "tool_use":
                logger.info(f"Tool call: {block.name}({json.dumps(block.input)})")
                result = _execute_tool(block.name, block.input)
                logger.info(f"Result: {result[:200]}...")
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })
            elif hasattr(block, "text") and block.text:
                final_report += block.text + "\n"

        if tool_results:
            messages.append({"role": "user", "content": tool_results})
        else:
            break

    return final_report.strip()
Important Checklist for Deployment:
Airflow Variables: Go to the Airflow UI and add ANTHROPIC_API_KEY to Admin -> Variables.

Permissions: If running in Docker, ensure the Airflow worker has write permissions to the volume mounted at /usr/local/airflow/dags.

Triggerer: Make sure you have at least one Triggerer process running, as the SelfHealingOperator is deferrable.