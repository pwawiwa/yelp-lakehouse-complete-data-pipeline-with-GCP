"""
Gold Layer: Aggregation DAG.

Triggered by Silver Datasets (data-aware scheduling).
Creates business analytics aggregations, materialized views,
and ML feature tables.
"""

from datetime import datetime

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
    BQ_SILVER_DATASET,
    BQ_GOLD_DATASET,
    YELP_ENTITIES,
)

# ── Data-Aware Scheduling: Trigger on Validated Silver ─────────────
VALIDATED_SILVER = [Dataset("validated_silver")]
GOLD_DATASET = Dataset("gold_analytics")


def _read_sql(filename: str) -> str:
    """Read and template a SQL file."""
    with open(f"/usr/local/airflow/include/sql/gold/{filename}") as f:
        return (
            f.read()
            .replace("{{ project_id }}", GCP_PROJECT_ID)
            .replace("{{ silver_dataset }}", BQ_SILVER_DATASET)
            .replace("{{ gold_dataset }}", BQ_GOLD_DATASET)
        )


@dag(
    dag_id="gold_aggregate",
    schedule=VALIDATED_SILVER,
    start_date=datetime(2024, 1, 1),
    description="Aggregate Silver → Gold: business analytics, user engagement, ML features",
    tags=["gold", "aggregation", "analytics", *dag_config["tags"]],
    **{k: v for k, v in dag_config.items() if k != "tags"},
)
def gold_aggregate():
    """
    ## Gold Aggregation Pipeline

    Triggered automatically when Silver datasets are updated.

    ### Aggregation Tables:
    1. **Business Performance** — Stars, review trends, engagement scores
    2. **User Engagement** — Activity metrics, elite analysis, user tiers
    3. **City Analytics** — City-level BI dashboard metrics
    4. **ML Features** — Feature table for star prediction model
    """

    create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_gold_dataset",
        dataset_id=BQ_GOLD_DATASET,
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        if_exists="ignore",
    )

    agg_business = BigQueryInsertJobOperator(
        task_id="agg_business_performance",
        configuration={
            "query": {
                "query": _read_sql("business_performance.sql"),
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
    )

    agg_users = BigQueryInsertJobOperator(
        task_id="agg_user_engagement",
        configuration={
            "query": {
                "query": _read_sql("user_engagement.sql"),
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
    )

    agg_city = BigQueryInsertJobOperator(
        task_id="agg_city_analytics",
        configuration={
            "query": {
                "query": _read_sql("city_analytics.sql"),
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
    )

    build_ml_features = BigQueryInsertJobOperator(
        task_id="build_ml_features",
        configuration={
            "query": {
                "query": _read_sql("ml_features_star_prediction.sql"),
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
    )

    @task(outlets=[GOLD_DATASET])
    def mark_gold_complete() -> dict:
        """Mark Gold dataset as updated for downstream consumers (ML pipeline)."""
        return {
            "status": "complete",
            "timestamp": datetime.utcnow().isoformat(),
            "tables_refreshed": [
                "business_performance",
                "user_engagement",
                "city_analytics",
                "ml_features_star_prediction",
            ],
        }

    # All aggregations run in parallel, then mark complete
    create_gold_dataset >> [agg_business, agg_users, agg_city, build_ml_features] >> mark_gold_complete()


gold_aggregate()
