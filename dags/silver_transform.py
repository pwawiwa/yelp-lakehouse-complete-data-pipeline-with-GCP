"""
Silver Layer: Transform DAG.

Triggered by Airflow Datasets from Bronze layer (data-aware scheduling).
Performs deduplication, cleaning, PII masking, and loads into
partitioned/clustered BigQuery Silver tables.
"""

from datetime import datetime, timezone
from airflow.sdk import dag, task, task_group
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

# ── Data-Aware Scheduling: Define Granular Upstream Datasets ────────
SILVER_DATASETS = {
    entity: Dataset(f"silver_{entity}") for entity in YELP_ENTITIES
}
VALIDATED_SILVER = Dataset("validated_silver")
BRONZE_DATASETS = [Dataset(f"bronze_{entity}") for entity in YELP_ENTITIES]


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
    """

    create_silver_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_silver_dataset",
        dataset_id=BQ_SILVER_DATASET,
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        if_exists="ignore",
    )

    # ── Create Silver Tables & Procedures ─────────────────────────
    @task()
    def log_table_creation():
        print(f"🚀 Initializing Silver Layer in {GCP_PROJECT_ID}.{BQ_SILVER_DATASET}...")

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
        @task()
        def log_review_start():
            print("📝 Starting transformation for entity: REVIEWS...")

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
            print("🛡️  Running PII detection (DLP) on 'reviews' table...")
            try:
                from include.pii.sensitive_data_protection import inspect_bigquery_table
                return inspect_bigquery_table(
                    project_id=GCP_PROJECT_ID,
                    dataset_id=BQ_SILVER_DATASET,
                    table_id="reviews",
                    info_types=["PERSON_NAME", "EMAIL_ADDRESS", "PHONE_NUMBER", "STREET_ADDRESS"],
                    sample_row_limit=1000,
                )
            except Exception as e:
                print(f"⚠️  DLP PII detection failed: {e}")
                print("Proceeding without PII audit. (Note: Ensure DLP API is enabled for full auditing)")
                return None

        log_review_start() >> load >> detect_pii_reviews()

    # ── Transform Businesses ──────────────────────────────────────
    @task_group(group_id="transform_businesses")
    def transform_businesses_group():
        @task()
        def log_business_start():
            print("🏢 Starting SCD Type 2 transformation for entity: BUSINESSES...")

        @task()
        def scd2_merge_businesses(ds=None, **kwargs):
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
            from datetime import datetime, timezone
            
            # Robust date detection for Airflow 3.0 / Asset-triggered runs
            run_date = ds or kwargs.get('logical_date', datetime.now(timezone.utc)).strftime('%Y-%m-%d')
            print(f"🏢 Running Business Upsert for date: {run_date}")
            
            hook = BigQueryHook()
            sql = f"CALL `{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.sp_upsert_business`('{GCP_PROJECT_ID}', '{BQ_BRONZE_DATASET}', '{BQ_SILVER_DATASET}', '{run_date}')"
            
            job = hook.insert_job(
                configuration={
                    "query": {
                        "query": sql,
                        "useLegacySql": False,
                    }
                },
                location=GCP_REGION
            )
            job.result()

        log_business_start() >> scd2_merge_businesses()

    # ── Transform Users ───────────────────────────────────────────
    @task_group(group_id="transform_users")
    def transform_users_group():
        @task()
        def log_user_start():
            print("👤 Starting SCD Type 2 transformation for entity: USERS...")

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
            print("🛡️  Running PII detection (DLP) on 'users' table...")
            try:
                from include.pii.sensitive_data_protection import inspect_bigquery_table
                return inspect_bigquery_table(
                    project_id=GCP_PROJECT_ID,
                    dataset_id=BQ_SILVER_DATASET,
                    table_id="users",
                    info_types=["PERSON_NAME"],
                    sample_row_limit=1000,
                )
            except Exception as e:
                print(f"⚠️  DLP PII detection failed: {e}")
                print("Proceeding without PII audit. (Note: Ensure DLP API is enabled for full auditing)")
                return None

        log_user_start() >> load >> detect_pii_users()

    # ── Transform Tips ──────────────────────────────────────────── [NEW]
    @task_group(group_id="transform_tips")
    def transform_tips_group():
        @task()
        def log_tip_start():
            print("💡 Starting transformation for entity: TIPS...")

        load = BigQueryInsertJobOperator(
            task_id="dedup_and_load_tips",
            configuration={
                "query": {
                    "query": "transform_tips.sql",
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
        
        log_tip_start() >> load

    # ── Transform Checkins ──────────────────────────────────────── [NEW]
    @task_group(group_id="transform_checkins")
    def transform_checkins_group():
        @task()
        def log_checkin_start():
            print("📍 Starting transformation for entity: CHECKINS...")

        load = BigQueryInsertJobOperator(
            task_id="dedup_and_load_checkins",
            configuration={
                "query": {
                    "query": "transform_checkins.sql",
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
        
        log_checkin_start() >> load

    # --- STEP 3: POST-SCD VALIDATION ---
    @task_group(group_id="post_scd_validation")
    def post_scd_validation_group():
        @task()
        def check_scd_integrity(**kwargs):
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
            from datetime import datetime, timezone
            
            run_date = kwargs.get('ds', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
            print(f"✅ Running Strict Post-SCD SQL Audits for partition {run_date}...")
            # Force use_legacy_sql=False to match the #standardSQL prefix in queries
            hook = BigQueryHook(use_legacy_sql=False)
            
            # 1. Validate SCD2 Dimensions (Users, Businesses)
            for dim in ['users', 'businesses']:
                pk = 'user_id' if dim == 'users' else 'business_id'
                
                # Use dataset.table format (no backticks unless ID contains dashes/special chars)
                # Force Standard SQL dialect using #standardSQL prefix for hook compatibility
                table_path = f"{BQ_SILVER_DATASET}.{dim}"
                
                # SQL-based NULL count check
                null_query = f"SELECT COUNT(*) FROM {table_path} WHERE {pk} IS NULL"
                res_null = hook.get_first(null_query)
                if res_null and res_null[0] > 0:
                    raise ValueError(f"SCD2 Integrity Failed! {res_null[0]} NULL primary keys found in {dim}.")
                print(f"   - {dim}: 0 NULL keys found. ✅")
                    
                # SQL-based SCD2 Active Uniqueness check
                unique_query = f"""
                    SELECT COUNT(*) FROM (
                        SELECT {pk}, COUNT(*) as active_count
                        FROM {table_path}
                        WHERE is_current = TRUE
                        GROUP BY {pk}
                        HAVING active_count > 1
                    )
                """
                res_unique = hook.get_first(unique_query)
                if res_unique and res_unique[0] > 0:
                    raise ValueError(f"SCD2 Integrity violated in {dim}: {res_unique[0]} entities have multiple active records.")
                print(f"   - {dim}: Active uniqueness verified. ✅")

            # 2. Validate Facts (Reviews, Tips, Checkins) for the current partition
            for fact in ['reviews', 'tips', 'checkins']:
                if fact == 'reviews':
                    pk_condition = 'review_id IS NULL'
                    group_expr = 'review_id'
                elif fact == 'tips':
                    pk_condition = 'user_id IS NULL OR business_id IS NULL OR date IS NULL'
                    group_expr = "CONCAT(user_id, '|', business_id, '|', CAST(date AS STRING))"
                else:
                    pk_condition = 'business_id IS NULL OR date IS NULL'
                    group_expr = "CONCAT(business_id, '|', CAST(date AS STRING))"
                
                date_field = '_processed_at'
                table_path = f"{BQ_SILVER_DATASET}.{fact}"

                # SQL-based NULL check for current batch
                null_query = f"""
                    SELECT COUNT(*) 
                    FROM {table_path} 
                    WHERE ({pk_condition}) AND DATE({date_field}) = '{run_date}'
                """
                res_null = hook.get_first(null_query)
                if res_null and res_null[0] > 0:
                    raise ValueError(f"Validation Failed! {res_null[0]} NULL composite keys found in {fact} batch {run_date}.")
                print(f"   - {fact}: 0 NULL composite keys in batch {run_date}. ✅")
                    
                # SQL-based Uniqueness check for current batch
                unique_query = f"""
                    SELECT COUNT(*) FROM (
                        SELECT {group_expr}, COUNT(*) as batch_count
                        FROM {table_path}
                        WHERE DATE({date_field}) = '{run_date}'
                        GROUP BY 1
                        HAVING batch_count > 1
                    )
                """
                res_unique = hook.get_first(unique_query)
                if res_unique and res_unique[0] > 0:
                    raise ValueError(f"Validation Failed! Duplicate keys found in {fact} for batch {run_date}.")
                print(f"   - {fact}: Uniqueness verified for batch {run_date}. ✅")
            
            print("🚀 Native SQL strictly validated the Silver layer successfully!")

        check_scd_integrity()

    @task(
        outlets=[SILVER_DATASETS[e] for e in YELP_ENTITIES] + [VALIDATED_SILVER],
    )
    def mark_silver_complete() -> dict:
        print("🎉 Silver Transformation & Validation Complete. Notifying downstream Gold layer.")
        return {
            "status": "complete",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # ── Task Dependencies ─────────────────────────────────────────
    (
        create_silver_dataset
        >> log_table_creation()
        >> create_silver_tables
        >> deploy_stored_procedures
        >> [
            transform_reviews_group(), 
            transform_businesses_group(), 
            transform_users_group(),
            transform_tips_group(),
            transform_checkins_group()
        ]
        >> post_scd_validation_group()
        >> mark_silver_complete()
    )


silver_transform()
