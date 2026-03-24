-- ─────────────────────────────────────────────────────────────────────
-- ML Feature Table: Review Star Rating Prediction Features
-- Optimized for Vertex AI / BigQuery ML with SCD Type 2 Safety
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `{{ project_id }}.{{ gold_dataset }}.ml_features_star_prediction` AS
SELECT
    r.review_id,
    
    -- TARGET: The star rating we want to predict
    r.stars                                         AS label,

    -- 1. Business Features (Snapshot from the current version)
    b.stars                                         AS business_avg_stars,
    b.review_count                                  AS business_review_count,
    b.is_open                                       AS business_is_open,
    CASE 
        WHEN b.categories LIKE '%Restaurants%' THEN 'Restaurant'
        WHEN b.categories LIKE '%Shopping%'    THEN 'Shopping'
        WHEN b.categories LIKE '%Food%'        THEN 'Food'
        WHEN b.categories LIKE '%Health%'      THEN 'Health'
        WHEN b.categories LIKE '%Beauty%'      THEN 'Beauty'
        WHEN b.categories LIKE '%Home%'        THEN 'Home Services'
        WHEN b.categories LIKE '%Nightlife%'   THEN 'Nightlife'
        ELSE 'Other'
    END                                             AS business_category_group,
    b.city                                          AS business_city,
    b.state                                         AS business_state,

    -- 2. User Features (Snapshot from the current version)
    u.review_count                                  AS user_review_count,
    u.average_stars                                 AS user_avg_stars,
    u.fans                                          AS user_fans,
    u.useful                                        AS user_useful_votes,
    CASE 
        WHEN u.elite IS NOT NULL AND LENGTH(u.elite) > 0 THEN 1 
        ELSE 0 
    END                                             AS user_is_elite,
    DATE_DIFF(CURRENT_DATE(), u.yelping_since, DAY) AS user_tenure_days,

    -- 3. Review Content Features (Text Metadata)
    LENGTH(r.text)                                  AS review_text_length,
    ARRAY_LENGTH(SPLIT(r.text, ' '))                AS review_word_count,
    r.useful                                        AS review_useful_votes,
    r.funny                                         AS review_funny_votes,
    r.cool                                          AS review_cool_votes,

    -- 4. Temporal Features (Seasonality and Behavior)
    EXTRACT(DAYOFWEEK FROM r.date)                  AS review_day_of_week,
    EXTRACT(MONTH FROM r.date)                      AS review_month,
    EXTRACT(YEAR FROM r.date)                       AS review_year,
    
    -- Metadata for tracking
    CURRENT_TIMESTAMP()                             AS _feature_generated_at

FROM `{{ project_id }}.{{ silver_dataset }}.reviews` AS r
-- JOIN 1: Current Business Profile
JOIN `{{ project_id }}.{{ silver_dataset }}.businesses` AS b
    ON r.business_id = b.business_id
    AND b.is_current = TRUE -- CRITICAL: Prevents duplicate training rows from SCD2 history
    
-- JOIN 2: Current User Profile
JOIN `{{ project_id }}.{{ silver_dataset }}.users` AS u
    ON r.user_id = u.user_id
    AND u.is_current = TRUE -- CRITICAL: Prevents duplicate training rows from SCD2 history

WHERE r.stars IS NOT NULL
    AND r.text IS NOT NULL
    AND LENGTH(r.text) > 0;