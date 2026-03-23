from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
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


@dag(
    dag_id="bronze_batch_ingest",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    description="Batch ingest Yelp JSON files to GCS Bronze layer with BigLake tables",
    tags=["bronze", "ingestion", "batch", "biglake", *dag_config["tags"]],
    **{k: v for k, v in dag_config.items() if k != "tags"},
)
def bronze_batch_ingest():
    """
    ## Bronze Batch Ingestion Pipeline (Pure Lakehouse)

    1. Discovers which Yelp JSON files exist in GCS (manually uploaded)
    2. Creates BigQuery BigLake External Tables over the GCS data
    3. Produces Airflow Datasets to trigger Silver layer

    ### Data Flow
    ```
    gs://bronze-bucket/yelp/raw/ → BigQuery BigLake External Tables
    ```
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
            # Check if there are any files for this entity using prefix
            blobs = list(client.list_blobs(bucket, prefix=f"yelp/raw/entity={entity}/", max_results=1))
            if blobs:
                available.append(entity)
                print(f"✅ Found data for entity: {entity} in GCS")
            else:
                print(f"⚠️ No data found in GCS for entity: {entity}")
        
        if not available:
            raise FileNotFoundError(
                f"No Yelp dataset files found in GCS bucket {GCS_BRONZE_BUCKET}. "
                "Please upload the JSON files manually to "
                f"gs://{GCS_BRONZE_BUCKET}/yelp/raw/entity=<entity_name>/"
            )
        return available

    # Dynamic task mapping for External Table creation
    create_external_tables = BigQueryCreateExternalTableOperator.partial(
        dataset_id=BQ_BRONZE_DATASET,
        project_id=GCP_PROJECT_ID,
        source_format="NEWLINE_DELIMITED_JSON",
        connection_id=f"{GCP_PROJECT_ID}.{GCP_REGION}.biglake_connection",
    ).expand(
        table_id=[f"{e}" for e in YELP_ENTITIES],
        source_uris=[[f"gs://{GCS_BRONZE_BUCKET}/yelp/raw/entity={e}/*.json"] for e in YELP_ENTITIES]
    )


    @task(
        outlets=[
            BRONZE_DATASETS[e] for e in YELP_ENTITIES
        ],
    )
    def mark_datasets_complete(entities: list[str]) -> dict:
        """Mark Bronze datasets as updated for data-aware scheduling."""
        return {
            "entities_loaded": entities,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "complete",
        }

    # ── DAG Flow ──────────────────────────────────────────────────
    available_entities = get_available_entities()

    create_bronze_dataset >> available_entities >> create_external_tables >> mark_datasets_complete(available_entities)


# Instantiate the DAG
bronze_batch_ingest()
