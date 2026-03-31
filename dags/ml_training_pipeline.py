"""
ML Training Pipeline DAG.

Triggered by Gold Dataset (data-aware scheduling).
Creates BigQuery ML models using CREATE MODEL, evaluates them,
and generates predictions.
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
    BQ_ML_DATASET,
)

# ── Data-Aware Scheduling ────────────────────────────────────────
GOLD_DATASET = Dataset("gold_analytics")


def _read_sql(filename: str) -> str:
    """Read and template a ML SQL file."""
    with open(f"/usr/local/airflow/include/sql/ml/{filename}") as f:
        return (
            f.read()
            .replace("{{ project_id }}", GCP_PROJECT_ID)
            .replace("{{ silver_dataset }}", BQ_SILVER_DATASET)
            .replace("{{ gold_dataset }}", BQ_GOLD_DATASET)
            .replace("{{ ml_dataset }}", BQ_ML_DATASET)
        )


@dag(
    dag_id="ml_training_pipeline",
    schedule=[GOLD_DATASET],
    start_date=datetime(2024, 1, 1),
    description="BigQuery ML: Train, evaluate, and predict with CREATE MODEL",
    tags=["ml", "bigquery-ml", "training", *dag_config["tags"]],
    **{k: v for k, v in dag_config.items() if k != "tags"},
)
def ml_training_pipeline():
    """
    ## ML Training Pipeline

    Triggered when Gold analytics tables are refreshed.

    ### Models:
    1. **Star Rating Prediction** — Linear Regression with TRANSFORM
    2. **Elite User Prediction** — Logistic Regression
    3. **Business Clustering** — K-Means (6 segments)

    ### Steps:
    1. Create ML dataset
    2. Train all three models (parallel)
    3. Evaluate model performance 
    4. Generate predictions and store results
    """

    create_ml_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_ml_dataset",
        dataset_id=BQ_ML_DATASET,
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        if_exists="ignore",
    )

    # ── Split CREATE MODEL statements into individual queries ─────

    @task()
    def train_star_rating_model():
        """Train linear regression model for star rating prediction."""
        from google.cloud import bigquery
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        query = f"""
        CREATE OR REPLACE MODEL `{GCP_PROJECT_ID}.{BQ_ML_DATASET}.star_rating_model`
        OPTIONS(
            model_type = 'BOOSTED_TREE_REGRESSOR',
            input_label_cols = ['label'],
            data_split_method = 'AUTO_SPLIT',
            max_iterations = 20,
            early_stop = TRUE,
            min_rel_progress = 0.001,
            l2_reg = 0.001,
            enable_global_explain = TRUE
        ) AS
        SELECT
            label,
            business_avg_stars,
            business_review_count,
            business_is_open,
            business_category_group,
            user_review_count,
            user_avg_stars,
            user_fans,
            user_is_elite,
            review_text_length,
            review_word_count,
            review_day_of_week,
            review_month
        FROM `{GCP_PROJECT_ID}.{BQ_GOLD_DATASET}.ml_features_star_prediction`
        """
        
        job = client.query(query)
        job.result()
        print("✅ Star rating model trained successfully")

    @task()
    def train_elite_user_model():
        """Train logistic regression model for elite user prediction."""
        from google.cloud import bigquery
        
        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE MODEL `{GCP_PROJECT_ID}.{BQ_ML_DATASET}.elite_user_model`
        OPTIONS(
            model_type = 'BOOSTED_TREE_CLASSIFIER',
            input_label_cols = ['is_elite'],
            auto_class_weights = TRUE,
            data_split_method = 'AUTO_SPLIT',
            max_iterations = 20,
            early_stop = TRUE,
            enable_global_explain = TRUE
        ) AS
        SELECT
            CASE WHEN elite IS NOT NULL AND LENGTH(elite) > 0 THEN 1 ELSE 0 END AS is_elite,
            review_count,
            average_stars,
            fans,
            useful AS useful_votes,
            funny AS funny_votes,
            cool AS cool_votes,
            COALESCE(compliment_hot,0) + COALESCE(compliment_more,0) + 
            COALESCE(compliment_profile,0) + COALESCE(compliment_cute,0) + 
            COALESCE(compliment_list,0) + COALESCE(compliment_note,0) + 
            COALESCE(compliment_plain,0) + COALESCE(compliment_cool,0) + 
            COALESCE(compliment_funny,0) + COALESCE(compliment_writer,0) + 
            COALESCE(compliment_photos,0) AS total_compliments,
            DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), yelping_since, DAY) AS days_on_platform
        FROM `{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.users`
        WHERE user_id IS NOT NULL AND review_count > 0
          AND is_current = TRUE
        """

        job = client.query(query)
        job.result()
        print("✅ Elite user model trained successfully")

    @task()
    def train_business_clusters():
        """Train K-Means clustering model for business segmentation."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE MODEL `{GCP_PROJECT_ID}.{BQ_ML_DATASET}.business_clusters`
        OPTIONS(
            model_type = 'KMEANS',
            num_clusters = 6,
            max_iterations = 50,
            standardize_features = TRUE
        ) AS
        SELECT
            stars AS avg_stars,
            review_count,
            CASE WHEN is_open = 1 THEN 1.0 ELSE 0.0 END AS is_open_float,
            latitude,
            longitude
        FROM `{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.businesses`
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND review_count IS NOT NULL
          AND is_current = TRUE
        """

        job = client.query(query)
        job.result()
        print("✅ Business clustering model trained successfully")

    @task()
    def evaluate_models() -> dict:
        """Evaluate all trained models and return metrics."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)
        results = {}

        # Evaluate star rating model
        eval_query = f"""
        SELECT * FROM ML.EVALUATE(MODEL `{GCP_PROJECT_ID}.{BQ_ML_DATASET}.star_rating_model`)
        """
        df = client.query(eval_query).to_dataframe()
        results["star_rating"] = df.to_dict(orient="records")[0] if len(df) > 0 else {}
        print(f"📊 Star Rating Model: {results['star_rating']}")

        # Evaluate elite user model
        eval_query = f"""
        SELECT * FROM ML.EVALUATE(MODEL `{GCP_PROJECT_ID}.{BQ_ML_DATASET}.elite_user_model`)
        """
        df = client.query(eval_query).to_dataframe()
        results["elite_user"] = df.to_dict(orient="records")[0] if len(df) > 0 else {}
        print(f"📊 Elite User Model: {results['elite_user']}")

        # Evaluate clustering model
        eval_query = f"""
        SELECT * FROM ML.EVALUATE(MODEL `{GCP_PROJECT_ID}.{BQ_ML_DATASET}.business_clusters`)
        """
        df = client.query(eval_query).to_dataframe()
        results["business_clusters"] = df.to_dict(orient="records")[0] if len(df) > 0 else {}
        print(f"📊 Business Clusters: {results['business_clusters']}")

        return results

    @task()
    def generate_predictions():
        """Generate predictions using the star rating model and save to Gold."""
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID)

        query = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_GOLD_DATASET}.star_predictions` AS
        SELECT
            review_id,
            label AS actual_stars,
            predicted_label AS predicted_stars,
            ABS(label - predicted_label) AS prediction_error
        FROM ML.PREDICT(
            MODEL `{GCP_PROJECT_ID}.{BQ_ML_DATASET}.star_rating_model`,
            (SELECT * FROM `{GCP_PROJECT_ID}.{BQ_GOLD_DATASET}.ml_features_star_prediction` LIMIT 10000)
        )
        """

        job = client.query(query)
        job.result()
        print(f"✅ Generated predictions: {job.num_dml_affected_rows} rows")

    # ── DAG Flow ──────────────────────────────────────────────────
    create_ml_dataset >> [
        train_star_rating_model(),
        train_elite_user_model(),
        train_business_clusters(),
    ] >> evaluate_models() >> generate_predictions()


ml_training_pipeline()
