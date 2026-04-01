from datetime import datetime, timezone
import os

from airflow.sdk import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from dags.common.dag_config import (
    dag_config,
    GCP_PROJECT_ID,
    GCP_REGION,
    GCS_BRONZE_BUCKET,
    BQ_SILVER_DATASET,
)

# ── Configuration ─────────────────────────────────────────────────
EXCEL_FILE_PATH = "data/Testing Sandbox.xlsx"
GCS_PREFIX = "sandbox"
BQ_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.sandbox_accounts"

def get_jakarta_date(**kwargs):
    """Standardizes date into Jakarta timezone."""
    import pendulum
    # TaskSDK context handling
    final_dt = kwargs.get('logical_date') or kwargs.get('data_interval_end') or pendulum.now('UTC')
    return pendulum.instance(final_dt).in_timezone('Asia/Jakarta').strftime('%Y-%m-%d')

# ── DAG Definition ────────────────────────────────────────────────
@dag(
    dag_id="sandbox_excel_lakehouse",
    schedule="@once", 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sandbox", "excel", "lakehouse"],
    **{k: v for k, v in dag_config.items() if k not in ["tags", "schedule", "catchup"]},
)
def sandbox_excel_lakehouse():
    """
    ## Excel-to-BigQuery Data Lakehouse Pipeline
    1. Uploads raw Excel to GCS Bronze
    2. Converts Excel to Parquet for efficiency
    3. Loads Parquet into BigQuery Silver Layer
    """

    @task()
    def upload_raw_to_gcs(**kwargs):
        """Upload the original Excel file to GCS for audit/lineage."""
        dt = get_jakarta_date(**kwargs)
        gcs_hook = GCSHook()
        destination_path = f"{GCS_PREFIX}/raw/dt={dt}/Testing Sandbox.xlsx"
        
        print(f"Uploading to gs://{GCS_BRONZE_BUCKET}/{destination_path}")
        gcs_hook.upload(
            bucket_name=GCS_BRONZE_BUCKET, object_name=destination_path, filename=EXCEL_FILE_PATH
        )
        return destination_path

    @task()
    def convert_to_parquet(**kwargs):
        """Convert Excel to Parquet locally and upload to GCS."""
        import pandas as pd
        
        dt = get_jakarta_date(**kwargs)
        parquet_local_path = "/tmp/sandbox_data_v2.parquet"
        gcs_destination = f"{GCS_PREFIX}/parquet/dt={dt}/data.parquet"
        
        # 1. Read Excel
        df = pd.read_excel(EXCEL_FILE_PATH)
        df['_processed_at'] = datetime.now(timezone.utc)
        
        # 2. Save as Parquet
        df.to_parquet(parquet_local_path, index=False, engine='pyarrow')
        
        # 3. Upload to GCS
        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=GCS_BRONZE_BUCKET, object_name=gcs_destination, filename=parquet_local_path
        )
        
        if os.path.exists(parquet_local_path):
            os.remove(parquet_local_path)
            
        return f"gs://{GCS_BRONZE_BUCKET}/{gcs_destination}"

    load_to_bq = BigQueryInsertJobOperator(
        task_id="load_parquet_to_bq",
        configuration={
            "query": {
                "query": """
                    LOAD DATA OVERWRITE `{{ params.table_id }}`
                    FROM FILES (
                      format = 'PARQUET',
                      uris = ['{{ task_instance.xcom_pull(task_ids='convert_to_parquet') }}']
                    )
                """,
                "useLegacySql": False,
            }
        },
        params={"table_id": BQ_TABLE_ID},
    )

    # ── Dependencies ──────────────────────────────────────────────
    raw_path = upload_raw_to_gcs()
    parquet_path = convert_to_parquet()
    
    raw_path >> parquet_path >> load_to_bq

# Instantiate the DAG
sandbox_excel_lakehouse()
