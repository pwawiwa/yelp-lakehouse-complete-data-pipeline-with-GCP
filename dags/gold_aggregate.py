from datetime import datetime
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)

# Configuration imports
from dags.common.dag_config import (
    GCP_PROJECT_ID,
    GCP_REGION,
    BQ_GOLD_DATASET,
    BQ_SILVER_DATASET,
    dag_config,
)

# ── Data-Aware Scheduling: Define Granular Upstream Datasets ────────
# These must match exactly with silver_transform.py and data_validation.py
SILVER_BUSINESSES = Dataset("silver_business")
SILVER_REVIEWS = Dataset("silver_review")
SILVER_USERS = Dataset("silver_user")
VALIDATED_SILVER = Dataset("validated_silver")

GOLD_COMPLETED = Dataset("gold_analytics")

@dag(
    dag_id="gold_aggregate_v2",
    # Trigger only after successful validation of Business, Reviews, and Users
    schedule=(SILVER_BUSINESSES & SILVER_REVIEWS & SILVER_USERS & VALIDATED_SILVER),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath="/usr/local/airflow/include/sql/gold/", # Allows native .sql file loading
    user_defined_macros={
        "project_id": GCP_PROJECT_ID,
        "gold_dataset": BQ_GOLD_DATASET,
        "silver_dataset": BQ_SILVER_DATASET,
    },
    tags=["gold", "analytics", "bigquery"],
)
def gold_aggregate():

    # 1. Infrastructure Setup
    create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_gold_dataset",
        dataset_id=BQ_GOLD_DATASET,
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        if_exists="ignore",
    )

    # 2. Reusable Configuration for BigQuery Jobs
    def get_bq_config(sql_file):
        return {
            "query": {
                "query": sql_file, # Airflow will find this in template_searchpath
                "useLegacySql": False,
            },
            "labels": {
                "env": "prod",
                "dag": "gold_aggregate",
                "layer": "gold"
            }
        }

    # 3. Parallel Aggregation Tasks
    # Using template files directly is much faster for the scheduler
    agg_business = BigQueryInsertJobOperator(
        task_id="agg_business_performance",
        configuration=get_bq_config("business_performance.sql"),
        do_xcom_push=False,
    )

    agg_users = BigQueryInsertJobOperator(
        task_id="agg_user_engagement",
        configuration=get_bq_config("user_engagement.sql"),
        do_xcom_push=False,
    )

    agg_city = BigQueryInsertJobOperator(
        task_id="agg_city_analytics",
        configuration=get_bq_config("city_analytics.sql"),
        do_xcom_push=False,
    )

    agg_ml_features = BigQueryInsertJobOperator(
        task_id="build_ml_features",
        configuration=get_bq_config("ml_features_star_prediction.sql"),
        do_xcom_push=False,
    )

    # 4. Outlets for Downstream Consumers (ML Training)
    @task(outlets=[GOLD_COMPLETED])
    def mark_gold_complete():
        print("Gold layer successfully refreshed and ready for downstream consumption.")

    # ── DAG Dependency Graph ──────────────────────────────────────────
    create_gold_dataset >> [agg_business, agg_users, agg_city, agg_ml_features] >> mark_gold_complete()

gold_aggregate()