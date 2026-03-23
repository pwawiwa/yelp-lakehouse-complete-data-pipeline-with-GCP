from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
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


# ── Raw Bronze Schemas (Explicitly define to avoid auto-inference issues) ──
BRONZE_RAW_SCHEMAS = {
    "business": """
        business_id STRING, name STRING, address STRING, city STRING, state STRING, 
        postal_code STRING, latitude FLOAT64, longitude FLOAT64, stars FLOAT64, 
        review_count INT64, is_open INT64, categories STRING, 
        attributes JSON, hours JSON
    """,
    "review": """
        review_id STRING, user_id STRING, business_id STRING, stars FLOAT64, 
        useful INT64, funny INT64, cool INT64, text STRING, date TIMESTAMP
    """,
    "user": """
        user_id STRING, name STRING, review_count INT64, yelping_since TIMESTAMP, 
        useful INT64, funny INT64, cool INT64, elite STRING, friends STRING, 
        fans INT64, average_stars FLOAT64, compliment_hot INT64, 
        compliment_more INT64, compliment_profile INT64, compliment_cute INT64, 
        compliment_list INT64, compliment_note INT64, compliment_plain INT64, 
        compliment_cool INT64, compliment_funny INT64, compliment_writer INT64, 
        compliment_photos INT64
    """,
    "checkin": "business_id STRING, date STRING",
    "tip": "user_id STRING, business_id STRING, text STRING, date TIMESTAMP, compliment_count INT64"
}


@dag(
    dag_id="bronze_lake_ingest",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    description="Batch ingest Yelp JSON files to GCS Bronze layer with BigLake tables",
    tags=["bronze", "ingestion", "batch", "biglake", "pure-lakehouse", *dag_config["tags"]],
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
        from google.cloud import storage

        client = storage.Client(project=GCP_PROJECT_ID)
        bucket = client.bucket(GCS_BRONZE_BUCKET)

        available = []
        for entity in YELP_ENTITIES:
            # Check if prefix exists
            blobs = list(client.list_blobs(bucket, prefix=f"yelp/raw/entity={entity}/", max_results=1))
            if blobs:
                available.append(entity)
        
        if not available:
            raise FileNotFoundError(f"No data found in gs://{GCS_BRONZE_BUCKET}/yelp/raw/")
        return available

    # Use BigQueryInsertJobOperator for creating BigLake External Tables
    # We generate the DDL dynamically within the mapping
    @task()
    def get_biglake_ddls(entities: list[str]) -> list[dict]:
        ddls = []
        for entity in entities:
            table_id = f"{GCP_PROJECT_ID}.{BQ_BRONZE_DATASET}.{entity}"
            source_uri = f"gs://{GCS_BRONZE_BUCKET}/yelp/raw/entity={entity}/*.json"
            connection_id = f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/connections/biglake-connection"
            
            schema = BRONZE_RAW_SCHEMAS.get(entity, "")
            schema_clause = f"({schema})" if schema else ""

            ddls.append({
                "query": {
                    "query": f"""
                        CREATE OR REPLACE EXTERNAL TABLE `{table_id}`
                        {schema_clause}
                        WITH CONNECTION `{connection_id}`
                        OPTIONS (
                            format = 'JSON',
                            uris = ['{source_uri}'],
                            max_bad_records = 10000,
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
            "timestamp": datetime.utcnow().isoformat(),
        }

    # ── DAG Flow ──────────────────────────────────────────────────
    available_entities = get_available_entities()
    ddls = get_biglake_ddls(available_entities)
    
    create_biglake_tables = BigQueryInsertJobOperator.partial(
        task_id="create_biglake_tables",
        location=GCP_REGION,
    ).expand(
        configuration=ddls,
    )
    
    create_bronze_dataset >> available_entities >> create_biglake_tables >> mark_datasets_complete(available_entities)


# Instantiate the DAG
bronze_lake_ingest()
