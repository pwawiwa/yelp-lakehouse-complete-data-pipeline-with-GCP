"""
## Self-Healing Airflow Agent

A "Meta-DAG" that continuously monitors all other DAGs in this Airflow deployment.
Powered by OpenAI GPT to detect, diagnose, and repair failures automatically.

### What it does:
1. **Detects** failed DAG runs using the Airflow REST API.
2. **Diagnoses** each failure by reading task logs and DAG source code.
3. **Fixes** the root cause by writing corrected Python code back to the DAGs folder.
4. **Retries** the fixed tasks automatically via the Airflow REST API.
5. **Reports** a full summary of its actions.

### Configuration (Airflow Variables):
- `OPENAI_API_KEY`: Your OpenAI API Key (required).
- `self_healing_target_dag`: Optional. Limit monitoring to a specific DAG ID.
- `self_healing_model`: Optional. Change the GPT model (default: `gpt-4o`).
"""
from datetime import datetime
from airflow.sdk import dag, task
from airflow.datasets import Dataset
from include.self_healing.operator import SelfHealingOperator

@dag(
    dag_id="self_healing_agent",
    start_date=datetime(2025, 1, 1),
    schedule="@continuous",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "platform", "retries": 0},
    tags=["self-healing", "platform", "ai-ops"],
)
def self_healing_agent():
    SelfHealingOperator(
        task_id="detect_and_heal",
        cooldown_seconds=90,  # Wait 90s between healing cycles
    )

self_healing_agent()
