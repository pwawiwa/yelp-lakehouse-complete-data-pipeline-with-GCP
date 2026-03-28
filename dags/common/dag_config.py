"""
Shared DAG configuration and default arguments.

All DAGs in the pipeline should use `default_args` and `dag_config`
from this module for consistent behavior, retries, and alerting.
"""

import os
from datetime import timedelta

from airflow.models import Variable

from dags.common.callbacks import on_failure_callback, on_retry_callback


def get_var(key: str, default: str = "") -> str:
    """
    Safely get configuration.
    Priority: 1. Environment Variable 2. Airflow Variable 3. Default
    """
    # Astro common env vars: GCP_PROJECT_ID, GOOGLE_CLOUD_PROJECT
    env_val = os.getenv(key.upper()) or os.getenv(f"AIRFLOW_VAR_{key.upper()}")
    if env_val:
        return env_val
        
    try:
        return Variable.get(key, default_var=default)
    except Exception:
        return default


# ── Project-wide Variables ────────────────────────────────────────

GCP_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or get_var("gcp_project_id", "yelp-production")
GCP_REGION = get_var("gcp_region", "asia-southeast1")
GCS_BRONZE_BUCKET = get_var("gcs_bronze_bucket", f"{GCP_PROJECT_ID}-bronze")
BQ_BRONZE_DATASET = get_var("bq_bronze_dataset", "bronze")
BQ_SILVER_DATASET = get_var("bq_silver_dataset", "silver")
BQ_GOLD_DATASET = get_var("bq_gold_dataset", "gold")
BQ_ML_DATASET = get_var("bq_ml_dataset", "ml_models")
ALERT_EMAIL = get_var("alert_email", "wira.hutomo2@gmail.com")

YELP_ENTITIES = ["business", "review", "user", "checkin", "tip"]

# ── Default DAG Arguments ─────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}

# ── Common DAG kwargs ─────────────────────────────────────────────

dag_config = {
    "default_args": default_args,
    "catchup": False,
    "max_active_runs": 1,
    "tags": ["data-lakehouse", "yelp", "gcp"],
    "doc_md": """
    ## Data Lakehouse ELT Pipeline
    Part of the GCP Data Lakehouse project using BigLake, BigQuery, 
    and Apache Iceberg. Orchestrated with Airflow on Astronomer (Astro).
    """,
}
