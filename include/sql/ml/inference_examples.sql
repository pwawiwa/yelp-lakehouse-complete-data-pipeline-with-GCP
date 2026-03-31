-- ─────────────────────────────────────────────────────────────────────
-- BigQuery ML: Inference & Prediction Examples
-- ─────────────────────────────────────────────────────────────────────

-- 1. Star Rating Prediction (Linear Regression)
-- Predict how many stars a review will receive based on business and user features.
SELECT
    review_id,
    label AS actual_stars,
    predicted_label AS predicted_stars,
    ABS(label - predicted_label) AS prediction_error
FROM ML.PREDICT(
    MODEL `{{ project_id }}.{{ ml_dataset }}.star_rating_model`,
    (
        SELECT * 
        FROM `{{ project_id }}.{{ gold_dataset }}.ml_features_star_prediction` 
        LIMIT 100
    )
);


-- 2. Elite User Prediction (Logistic Regression)
-- Predict the probability of a user becoming "Elite" in the future.
SELECT
    user_id,
    name,
    predicted_is_elite,
    -- Get the probability of the positive class (is_elite = 1)
    probs.prob AS probability_of_elite
FROM ML.PREDICT(
    MODEL `{{ project_id }}.{{ ml_dataset }}.elite_user_model`,
    (
        SELECT
            user_id,
            name,
            review_count,
            average_stars,
            fans,
            useful AS useful_votes,
            funny AS funny_votes,
            cool AS cool_votes,
            compliment_hot + compliment_more + compliment_profile +
            compliment_cute + compliment_list + compliment_note +
            compliment_plain + compliment_cool + compliment_funny +
            compliment_writer + compliment_photos AS total_compliments,
            DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), yelping_since, DAY) AS days_on_platform
        FROM `{{ project_id }}.{{ silver_dataset }}.users`
        WHERE is_current = TRUE
        LIMIT 100
    )
), UNNEST(predicted_is_elite_probs) as probs
WHERE probs.label = 1;


-- 3. Business Segmentation (K-Means Clustering)
-- Identify which market segment (cluster) a specific business belongs to.
SELECT
    business_id,
    name,
    CENTROID_ID AS cluster_id,
    stars,
    review_count
FROM ML.PREDICT(
    MODEL `{{ project_id }}.{{ ml_dataset }}.business_clusters`,
    (
        SELECT
            business_id,
            name,
            stars AS avg_stars,
            review_count,
            CASE WHEN is_open = 1 THEN 1.0 ELSE 0.0 END AS is_open_float,
            latitude,
            longitude
        FROM `{{ project_id }}.{{ silver_dataset }}.businesses`
        WHERE is_current = TRUE
        LIMIT 100
    )
);


-- 4. Global Explanations (Feature Importance)
-- Understand which features are driving the model's decisions globally.
SELECT
    *
FROM
    ML.GLOBAL_EXPLAIN(MODEL `{{ project_id }}.{{ ml_dataset }}.star_rating_model`);


-- 5. Targeted Inference (Specific Row)
-- To infer for a specific user or business, filter the subquery.
SELECT
    user_id,
    predicted_is_elite,
    probs.prob AS probability
FROM ML.PREDICT(
    MODEL `{{ project_id }}.{{ ml_dataset }}.elite_user_model`,
    (
        SELECT * FROM `{{ project_id }}.{{ silver_dataset }}.users`
        WHERE user_id = 'PUT_USER_ID_HERE'
          AND is_current = TRUE
    )
), UNNEST(predicted_is_elite_probs) as probs
WHERE probs.label = 1;


-- 6. Ad-hoc Inference (One-Shot / Manual Inputs)
-- You can manually provide features without querying a table.
-- Important: Column names must match the model's expected input feature names.
SELECT
    *
FROM ML.PREDICT(
    MODEL `{{ project_id }}.{{ ml_dataset }}.star_rating_model`,
    (
        SELECT
            4.5 AS business_avg_stars,
            120 AS business_review_count,
            1 AS business_is_open,
            'Restaurants' AS business_category_group,
            50 AS user_review_count,
            4.0 AS user_avg_stars,
            5 AS user_fans,
            1 AS user_is_elite,
            200 AS review_text_length,
            40 AS review_word_count,
            'Monday' AS review_day_of_week,
            'March' AS review_month
    )
);
