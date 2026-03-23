"""
Validation DAG: Triggers Dataflow validation pipeline and Soda checks.

Runs data quality validation using two approaches:
1. Dataflow DLQ pipeline (Apache Beam ParDo/DoFn with side outputs)
2. Soda Core checks (schema, freshness, uniqueness validations)
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)

from dags.common.dag_config import (
    dag_config,
    GCP_PROJECT_ID,
    GCP_REGION,
    BQ_SILVER_DATASET,
    YELP_ENTITIES,
)

# ── Data-Aware Scheduling: Trigger on Silver Datasets ──────────────
SILVER_DATASETS = [Dataset(f"silver_{entity}") for entity in YELP_ENTITIES]
VALIDATED_SILVER = Dataset("validated_silver")


@dag(
    dag_id="data_validation",
    schedule=SILVER_DATASETS,
    start_date=datetime(2024, 1, 1),
    description="Data quality validation: Soda Core standard checks",
    tags=["validation", "quality", "soda", *dag_config["tags"]],
    **{k: v for k, v in dag_config.items() if k != "tags"},
)
def data_validation():
    """
    ## Data Validation Pipeline (Consolidated)

    Uses Soda Core to perform schema, freshness, and distribution checks
    on the Silver layer tables before Gold layer aggregation.

    ### Why this is Zero-Waste:
    - **No Dataflow overhead**: Eliminated 6m startup latency.
    - **Native SQL validation**: Soda runs directly in BigQuery.
    - **Single Gate**: Replaces multiple redundant validation loops.
    """

    # ── Soda Data Quality Scan ──────────────────────────────────────

    @task(outlets=[VALIDATED_SILVER])
    def run_soda_scan():
        """Run Soda Core data quality checks using the Soda Python SDK."""
        from soda.scan import Scan
        import os

        # Initialize Soda Scan
        scan = Scan()
        scan.set_verbose()
        scan.set_data_source_name("silver_bq")

        # Set variables for YAML interpolation
        scan.add_variables({
            "GCP_PROJECT_ID": GCP_PROJECT_ID,
            "BQ_SILVER_DATASET": BQ_SILVER_DATASET,
        })

        # Add configuration and checks
        config_path = "/usr/local/airflow/include/soda/config.yml"
        checks_path = "/usr/local/airflow/include/soda/checks/silver_tables.yml"
        
        scan.add_configuration_yaml_file(config_path)
        scan.add_sodacl_yaml_file(checks_path)

        # Execute scan
        exit_code = scan.execute()

        # Handle results and alerting
        scan.assert_no_error_logs()
        scan.assert_no_checks_fail()
        
        print(f"✅ Soda Scan completed with exit code: {exit_code}")
        return {"exit_code": exit_code}

    run_soda_scan()


data_validation()
