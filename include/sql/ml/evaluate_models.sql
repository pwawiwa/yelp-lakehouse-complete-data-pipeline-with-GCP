-- ─────────────────────────────────────────────────────────────────────
-- Evaluate all trained BigQuery ML models
-- ─────────────────────────────────────────────────────────────────────

-- 1. Star Rating Prediction: Evaluate regression metrics
SELECT
    'star_rating_model' AS model_name,
    *
FROM ML.EVALUATE(MODEL `{{ project_id }}.{{ ml_dataset }}.star_rating_model`);

-- 2. Elite User Prediction: Evaluate classification metrics  
SELECT
    'elite_user_model' AS model_name,
    *
FROM ML.EVALUATE(MODEL `{{ project_id }}.{{ ml_dataset }}.elite_user_model`);

-- 3. Business Clustering: Evaluate clustering metrics
SELECT
    'business_clusters' AS model_name,
    *
FROM ML.EVALUATE(MODEL `{{ project_id }}.{{ ml_dataset }}.business_clusters`);
