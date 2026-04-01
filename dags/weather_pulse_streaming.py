from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateTopicOperator,
    PubSubCreateSubscriptionOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.models.baseoperator import BaseOperator
from dags.common.dag_config import dag_config, GCP_PROJECT_ID, GCP_REGION

import os
import sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INCLUDE_DIR = os.path.join(os.path.dirname(CURRENT_DIR), "include")
if INCLUDE_DIR not in sys.path:
    sys.path.append(INCLUDE_DIR)

# ── Configuration (Managed Cluster) ──────────────────────────
WEATHER_TOPIC        = "weather-stream"
WEATHER_SUBSCRIPTION = "weather-sub"
GOLD_STREAMING_DATASET = "gold_streaming"
SPARK_SCRIPT_PATH    = "gs://yelp-production-scripts/streaming/spark_weather_processor.py"
DATAPROC_CLUSTER     = "weather-pulse-cluster"
CLUSTER_CONFIG = {
    "gce_cluster_config": {
        "zone_uri": "asia-southeast1-c",
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",  # 4 vCPUs, 15GB RAM
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",  # 2x2 = 4 vCPUs
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    # TOTAL: 8 vCPUs
}

SPARK_PROPERTIES = {
    "spark.sql.shuffle.partitions":     "4",
    "spark.sql.streaming.checkpointLocation": f"gs://yelp-production-scripts/checkpoints/weather",
}


default_args = {
    **dag_config["default_args"],
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

# ── DAG 1: Infrastructure + Streaming Job ───────────────────────
with DAG(
    dag_id="weather_pulse_streaming",
    schedule="@once",
    description="Orchestrate Yelp Weather Pulse: Pub/Sub -> Dataproc Serverless -> BigQuery",
    default_args=default_args,
    tags=["streaming", "weather", "dataproc", "serverless"],
    catchup=False,
) as dag:

    create_topic = PubSubCreateTopicOperator(
        task_id="create_weather_topic",
        topic=WEATHER_TOPIC,
        project_id=GCP_PROJECT_ID,
        fail_if_exists=False,
    )

    create_subscription = PubSubCreateSubscriptionOperator(
        task_id="create_weather_subscription",
        topic=WEATHER_TOPIC,
        subscription=WEATHER_SUBSCRIPTION,
        project_id=GCP_PROJECT_ID,
        fail_if_exists=False,
    )

    create_gold_streaming_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_gold_streaming_dataset",
        dataset_id=GOLD_STREAMING_DATASET,
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        if_exists="ignore",
    )

    # ── Orchestration: Managed Optimized Cluster ────────────────
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER,
        cluster_config=CLUSTER_CONFIG,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_spark_weather_job",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "reference": {"project_id": GCP_PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": SPARK_SCRIPT_PATH,
                "args": [
                    f"--project_id={GCP_PROJECT_ID}",
                    f"--subscription={WEATHER_SUBSCRIPTION}",
                ],
                "properties": SPARK_PROPERTIES,
            },
        },
        asynchronous=True,  # Fire-and-forget for streaming
    )

    create_topic >> create_subscription >> create_cluster >> submit_job
    create_gold_streaming_dataset >> create_cluster


# ── DAG 2: Producer — publish a single batch per run ────────────
with DAG(
    dag_id="weather_producer_ingest",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        **dag_config["default_args"],
        "execution_timeout": timedelta(minutes=4),  # must finish before next run
        "retries": 1,
    },
    tags=["streaming", "producer"],
) as producer_dag:

    def publish_single_batch(**context):
        """
        Publish ONE batch of weather events and return.
        Do NOT loop with sleeps inside an Airflow task — the scheduler
        handles the cadence via the cron schedule.
        """
        from include.streaming.weather_producer import publish_batch  # use a single-shot fn
        publish_batch()  # publish N messages and return immediately

    ingest_batch = PythonOperator(
        task_id="ingest_weather_batch",
        python_callable=publish_single_batch,
    )