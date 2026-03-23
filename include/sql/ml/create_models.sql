-- ─────────────────────────────────────────────────────────────────────
-- BigQuery ML: Star Rating Prediction (Linear Regression)
-- with TRANSFORM clause for in-database feature preprocessing
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE MODEL `{{ project_id }}.{{ ml_dataset }}.star_rating_model`

TRANSFORM(
    -- Target
    label,
    -- Numeric features (scaled)
    ML.STANDARD_SCALER(business_avg_stars)       OVER() AS scaled_business_avg_stars,
    ML.STANDARD_SCALER(business_review_count)    OVER() AS scaled_business_review_count,
    ML.STANDARD_SCALER(user_review_count)        OVER() AS scaled_user_review_count,
    ML.STANDARD_SCALER(user_avg_stars)           OVER() AS scaled_user_avg_stars,
    ML.STANDARD_SCALER(user_fans)                OVER() AS scaled_user_fans,
    ML.STANDARD_SCALER(review_text_length)       OVER() AS scaled_review_text_length,
    ML.STANDARD_SCALER(review_word_count)        OVER() AS scaled_review_word_count,
    ML.STANDARD_SCALER(user_days_on_platform)    OVER() AS scaled_user_days_on_platform,
    -- Categorical features (one-hot encoded)
    ML.ONE_HOT_ENCODER(business_category)        OVER() AS encoded_business_category,
    ML.ONE_HOT_ENCODER(business_state)           OVER() AS encoded_business_state,
    -- Binary features
    business_is_open,
    user_is_elite,
    -- Temporal features
    review_day_of_week,
    review_month
)

OPTIONS(
    model_type = 'LINEAR_REG',
    input_label_cols = ['label'],
    data_split_method = 'AUTO_SPLIT',
    max_iterations = 20,
    learn_rate_strategy = 'LINE_SEARCH',
    early_stop = TRUE,
    min_rel_progress = 0.001,
    ls_init_learn_rate = 0.1,
    l2_reg = 0.001,
    enable_global_explain = TRUE
) AS

SELECT *
FROM `{{ project_id }}.{{ gold_dataset }}.ml_features_star_prediction`;


-- ─────────────────────────────────────────────────────────────────────
-- BigQuery ML: Elite User Prediction (Logistic Regression)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE MODEL `{{ project_id }}.{{ ml_dataset }}.elite_user_model`

OPTIONS(
    model_type = 'LOGISTIC_REG',
    input_label_cols = ['is_elite'],
    auto_class_weights = TRUE,
    data_split_method = 'AUTO_SPLIT',
    max_iterations = 20,
    early_stop = TRUE,
    enable_global_explain = TRUE
) AS

SELECT
    CASE 
        WHEN elite IS NOT NULL AND LENGTH(elite) > 0 THEN 1 
        ELSE 0 
    END                                             AS is_elite,
    review_count,
    average_stars,
    fans,
    useful AS useful_votes,
    funny AS funny_votes,
    cool AS cool_votes,
    compliment_hot + compliment_more + compliment_profile +
    compliment_cute + compliment_list + compliment_note +
    compliment_plain + compliment_cool + compliment_funny +
    compliment_writer + compliment_photos             AS total_compliments,
    CASE
        WHEN friends IS NOT NULL AND friends != 'None'
        THEN ARRAY_LENGTH(SPLIT(friends, ','))
        ELSE 0
    END                                             AS friend_count,
    DATE_DIFF(CURRENT_DATE(), yelping_since, DAY)    AS days_on_platform
FROM `{{ project_id }}.{{ silver_dataset }}.users`
WHERE user_id IS NOT NULL
    AND review_count > 0;


-- ─────────────────────────────────────────────────────────────────────
-- BigQuery ML: Business Clustering (K-Means)
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE MODEL `{{ project_id }}.{{ ml_dataset }}.business_clusters`

OPTIONS(
    model_type = 'KMEANS',
    num_clusters = 6,
    max_iterations = 50,
    standardize_features = TRUE
) AS

SELECT
    stars                                           AS avg_stars,
    review_count,
    CASE WHEN is_open = 1 THEN 1.0 ELSE 0.0 END    AS is_open_float,
    latitude,
    longitude
FROM `{{ project_id }}.{{ silver_dataset }}.businesses`
WHERE latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND review_count IS NOT NULL;
