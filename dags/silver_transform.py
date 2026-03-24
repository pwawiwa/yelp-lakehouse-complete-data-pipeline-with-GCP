"""
Silver Layer: Transform DAG.

Triggered by Airflow Datasets from Bronze layer (data-aware scheduling).
Performs deduplication, cleaning, PII masking, and loads into
partitioned/clustered BigQuery Silver tables.
"""

from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.datasets import Dataset
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)

from dags.common.dag_config import (
    dag_config,
    GCP_PROJECT_ID,
    GCP_REGION,
    GCS_BRONZE_BUCKET,
    BQ_BRONZE_DATASET,
    BQ_SILVER_DATASET,
    YELP_ENTITIES,
)

# ── Data-Aware Scheduling: Trigger on Bronze Datasets ─────────────
BRONZE_DATASETS = [Dataset(f"bronze_{entity}") for entity in YELP_ENTITIES]
SILVER_DATASETS = {entity: Dataset(f"silver_{entity}") for entity in YELP_ENTITIES}


@dag(
    dag_id="silver_transform",
    schedule=BRONZE_DATASETS,
    start_date=datetime(2024, 1, 1),
    description="Transform Bronze → Silver: dedup, clean, PII mask, partition",
    template_searchpath=["/usr/local/airflow/include/sql/silver"],
    user_defined_macros={
        "project_id": GCP_PROJECT_ID,
        "bronze_dataset": BQ_BRONZE_DATASET,
        "silver_dataset": BQ_SILVER_DATASET,
    },
    **{k: v for k, v in dag_config.items() if k != "tags"},
)
def silver_transform():
    """
    ## Silver Transformation Pipeline

    Triggered automatically when Bronze datasets are updated.

    ### Steps per entity:
    1. **Deduplicate** — Remove duplicate records via ROW_NUMBER()
    2. **Clean** — Type casting, null handling, standardization
    3. **Schema Evolution** — Detect and apply new columns
    4. **PII Masking** — Detect and mask PII via Sensitive Data Protection
    5. **Load Silver** — Write to partitioned/clustered BigQuery tables

    ### Data Flow
    ```
    BigQuery Bronze (external) → Transform → BigQuery Silver (native, partitioned)
    ```
    """

    create_silver_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_silver_dataset",
        dataset_id=BQ_SILVER_DATASET,
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        if_exists="ignore",
    )

    # ── Create Silver Tables & Procedures ─────────────────────────
    create_silver_tables = BigQueryInsertJobOperator(
        task_id="create_silver_tables",
        configuration={
            "query": {
                "query": "create_silver_tables.sql",
                "useLegacySql": False,
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "silver_dataset": BQ_SILVER_DATASET,
        },
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        do_xcom_push=False,
    )

    deploy_stored_procedures = BigQueryInsertJobOperator(
        task_id="deploy_stored_procedures",
        configuration={
            "query": {
                "query": "sp_upsert_business.sql",
                "useLegacySql": False,
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "silver_dataset": BQ_SILVER_DATASET,
        },
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        do_xcom_push=False,
    )

    # ── Transform Reviews ─────────────────────────────────────────
    @task_group(group_id="transform_reviews")
    def transform_reviews_group():
        load = BigQueryInsertJobOperator(
            task_id="dedup_and_load_reviews",
            configuration={
                "query": {
                    "query": "transform_reviews.sql",
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": GCP_PROJECT_ID,
                "bronze_dataset": BQ_BRONZE_DATASET,
                "silver_dataset": BQ_SILVER_DATASET,
            },
            project_id=GCP_PROJECT_ID,
            location=GCP_REGION,
            do_xcom_push=False,
        )

        @task()
        def detect_pii_reviews():
            from include.pii.sensitive_data_protection import inspect_bigquery_table
            return inspect_bigquery_table(
                project_id=GCP_PROJECT_ID,
                dataset_id=BQ_SILVER_DATASET,
                table_id="reviews",
                info_types=["PERSON_NAME", "EMAIL_ADDRESS", "PHONE_NUMBER", "STREET_ADDRESS"],
                sample_row_limit=1000, # FAANG: Reduced sample for cost
            )

        load >> detect_pii_reviews()

    # ── Transform Businesses ──────────────────────────────────────
    @task_group(group_id="transform_businesses")
    def transform_businesses_group():
        BigQueryInsertJobOperator(
            task_id="scd2_merge_businesses",
            configuration={
                "query": {
                    "query": f"CALL `{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.sp_upsert_business`('{GCP_PROJECT_ID}', '{BQ_BRONZE_DATASET}', '{BQ_SILVER_DATASET}')",
                    "useLegacySql": False,
                }
            },
            project_id=GCP_PROJECT_ID,
            location=GCP_REGION,
            do_xcom_push=False,
        )

    # ── Transform Users ───────────────────────────────────────────
    @task_group(group_id="transform_users")
    def transform_users_group():
        load = BigQueryInsertJobOperator(
            task_id="scd2_merge_users",
            configuration={
                "query": {
                    "query": "transform_users.sql",
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": GCP_PROJECT_ID,
                "bronze_dataset": BQ_BRONZE_DATASET,
                "silver_dataset": BQ_SILVER_DATASET,
            },
            project_id=GCP_PROJECT_ID,
            location=GCP_REGION,
            do_xcom_push=False,
        )

        @task()
        def detect_pii_users():
            from include.pii.sensitive_data_protection import inspect_bigquery_table
            return inspect_bigquery_table(
                project_id=GCP_PROJECT_ID,
                dataset_id=BQ_SILVER_DATASET,
                table_id="users",
                info_types=["PERSON_NAME"],
                sample_row_limit=1000,
            )

        load >> detect_pii_users()

    @task(
        outlets=[SILVER_DATASETS[e] for e in YELP_ENTITIES],
    )
    def mark_silver_complete() -> dict:
        """Mark Silver datasets as updated for Gold layer scheduling."""
        return {
            "status": "complete",
            "timestamp": datetime.utcnow().isoformat(),
        }

    # ── Task Dependencies ─────────────────────────────────────────
    (
        create_silver_dataset
        >> create_silver_tables
        >> deploy_stored_procedures
        >> [transform_reviews_group(), transform_businesses_group(), transform_users_group()]
        >> mark_silver_complete()
    )


silver_transform()
