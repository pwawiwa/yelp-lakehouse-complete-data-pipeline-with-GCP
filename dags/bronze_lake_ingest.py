from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task
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
    YELP_ENTITIES,
)

# ── Airflow Datasets (for data-aware scheduling) ─────────────────
BRONZE_DATASETS = {
    entity: Dataset(f"bronze_{entity}") for entity in YELP_ENTITIES
}

def get_jakarta_date(dt=None, **kwargs):
    """
    Standardizes date into Jakarta timezone for GCS path logic.
    Resilient to Airflow 3.0 TaskSDK context changes.
    """
    import pendulum
    from datetime import datetime

    # Try to find a valid date in various possible locations
    final_dt = dt
    if not final_dt or str(final_dt) == 'None' or 'Undefined' in str(final_dt):
        final_dt = kwargs.get('logical_date') or kwargs.get('data_interval_end')

    if not final_dt or str(final_dt) == 'None' or 'Undefined' in str(final_dt):
        final_dt = pendulum.now('UTC')

    # Convert to pendulum if it's a string or basic datetime
    if isinstance(final_dt, str):
        try:
            final_dt = pendulum.parse(final_dt)
        except:
            final_dt = pendulum.now('UTC')

    return pendulum.instance(final_dt).in_timezone('Asia/Jakarta').strftime('%Y-%m-%d')


@dag(
    dag_id="bronze_lake_ingest",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    description="Ingest Yelp GCS JSONs into BigQuery BigLake Bronze External Tables",
    user_defined_macros={"jakarta_date": get_jakarta_date},
    tags=["bronze", "ingestion", "biglake", *dag_config["tags"]],
    **{k: v for k, v in dag_config.items() if k != "tags"},
)
def bronze_lake_ingest():
    """
    ## Bronze Batch Ingestion Pipeline (Pure Lakehouse)

    1. Discovers which Yelp JSON files exist in GCS
    2. Creates BigQuery BigLake External Tables over GCS JSON (DDL)
    3. Produces Airflow Datasets for Silver layer
    """

    create_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bronze_dataset",
        dataset_id=BQ_BRONZE_DATASET,
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        if_exists="ignore",
    )

    @task()
    def get_available_entities() -> list[str]:
        """Discover which Yelp entities have been uploaded to GCS."""
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        hook = GCSHook()
        available = []
        for entity in YELP_ENTITIES:
            # Check if prefix exists (using prefix check to avoid full list)
            prefix = f"yelp/raw/entity={entity}/"
            blobs = hook.list(GCS_BRONZE_BUCKET, prefix=prefix, max_results=1)
            if blobs:
                available.append(entity)
        
        if not available:
            raise FileNotFoundError(f"No data found in gs://{GCS_BRONZE_BUCKET}/yelp/raw/")
        return available

    @task()
    def get_biglake_ddls(entities: list[str]) -> list[dict]:
        ddls = []
        for entity in entities:
            table_id = f"{GCP_PROJECT_ID}.{BQ_BRONZE_DATASET}.{entity}"
            source_uri_prefix = f"gs://{GCS_BRONZE_BUCKET}/yelp/raw/entity={entity}/"
            source_uri = f"{source_uri_prefix}*"
            connection_id = f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/connections/biglake-connection"
            
            # MINIMALIST DDL:
            # We omit the problematic Hive partitioning OPTIONS keys.
            # Because your GCS URIs follow the 'dt=YYYY-MM-DD' pattern, 
            # and we define 'WITH PARTITION COLUMNS (dt STRING)', BigQuery 
            # will automatically infer the partitioning schema from the paths.
            
            # SCHEMA MAPPING (Fixes JSON parsing errors like 'None' in boolean fields)
            schema_clause = ""
            if entity == "business":
                schema_clause = """(
                    business_id STRING, name STRING, address STRING, city STRING, state STRING, 
                    postal_code STRING, latitude FLOAT64, longitude FLOAT64, stars FLOAT64, 
                    review_count INT64, is_open INT64, attributes JSON, hours JSON, categories STRING
                )"""
            elif entity == "user":
                schema_clause = """(
                    user_id STRING, name STRING, review_count INT64, yelping_since STRING, 
                    useful INT64, funny INT64, cool INT64, elite STRING, friends STRING, 
                    fans INT64, average_stars FLOAT64, compliment_hot INT64, compliment_more INT64, 
                    compliment_profile INT64, compliment_cute INT64, compliment_list INT64, 
                    compliment_note INT64, compliment_plain INT64, compliment_cool INT64, 
                    compliment_funny INT64, compliment_writer INT64, compliment_photos INT64
                )"""
            
            ddls.append({
                "query": {
                    "query": f"""
                        CREATE OR REPLACE EXTERNAL TABLE `{table_id}`
                        {schema_clause}
                        WITH PARTITION COLUMNS (dt STRING)
                        WITH CONNECTION `{connection_id}`
                        OPTIONS (
                            format = 'JSON',
                            uris = ['{source_uri}'],
                            hive_partition_uri_prefix = '{source_uri_prefix}',
                            max_bad_records = 100000,
                            ignore_unknown_values = TRUE
                        )
                    """,
                    "useLegacySql": False,
                }
            })
        return ddls

    @task(
        outlets=[BRONZE_DATASETS[e] for e in YELP_ENTITIES],
    )
    def mark_datasets_complete(entities: list[str]) -> dict:
        return {
            "entities_loaded": entities,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @task()
    def validate_bronze_data(entities: list[str], **kwargs):
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        
        # Standardize on Jakarta time for partitioning
        target_date = get_jakarta_date(**kwargs)
        print(f"🔍 Running Pre-SCD Check: Light Validation (Nulls & Duplicates) in Bronze (Date: {target_date})...")
        hook = BigQueryHook()
        
        for entity in entities:
            if entity == "user":
                group_cols = "user_id"
                null_cond = "user_id IS NULL"
                dup_filter = "AND user_id IS NOT NULL"
            elif entity == "business":
                group_cols = "business_id"
                null_cond = "business_id IS NULL"
                dup_filter = "AND business_id IS NOT NULL"
            elif entity == "checkin":
                group_cols = "business_id"
                null_cond = "business_id IS NULL"
                dup_filter = "AND business_id IS NOT NULL"
            elif entity == "review":
                group_cols = "review_id"
                null_cond = "user_id IS NULL OR business_id IS NULL"
                dup_filter = "AND review_id IS NOT NULL"
            elif entity == "tip":
                group_cols = "user_id, business_id, date"
                null_cond = "user_id IS NULL OR business_id IS NULL"
                dup_filter = "AND user_id IS NOT NULL AND business_id IS NOT NULL AND date IS NOT NULL"

            query = f"""
            WITH KeyStats AS (
                SELECT 
                    {group_cols},
                    COUNT(1) as row_count,
                    SUM(CASE WHEN {null_cond} THEN 1 ELSE 0 END) as null_count
                FROM `{BQ_BRONZE_DATASET}.{entity}`
                WHERE dt = '{target_date}'
                GROUP BY {group_cols}
            )
            SELECT 
                SUM(null_count) as total_nulls,
                COUNTIF(row_count > 1 {dup_filter}) as total_duplicates
            FROM KeyStats
            """
            
            df = hook.get_df(query, dialect='standard')
            nulls = int(df.iloc[0]['total_nulls']) if not df['total_nulls'].isna().all() else 0
            duplicates = int(df.iloc[0]['total_duplicates']) if not df['total_duplicates'].isna().all() else 0
            
            if nulls > 0 or duplicates > 0:
                print(f"  ⚠️ LIGHT VALIDATION WARNING | {entity.upper()}: Found {nulls} NULL records and {duplicates} duplicate keys. Ingestion proceeding as analysis source-of-truth.")
            else:
                print(f"  ✅ {entity.upper()}: Clean. 0 NULL records, 0 duplicates.")


    # ── DAG Flow ──────────────────────────────────────────────────
    available_entities = get_available_entities()
    ddls = get_biglake_ddls(available_entities)
    
    create_biglake_tables = BigQueryInsertJobOperator.partial(
        task_id="create_biglake_tables",
        location=GCP_REGION,
    ).expand(
        configuration=ddls,
    )
    
    validation_task = validate_bronze_data(available_entities)
    create_bronze_dataset >> available_entities >> create_biglake_tables >> validation_task >> mark_datasets_complete(available_entities)

# Instantiate the DAG
bronze_lake_ingest()
