-- ─────────────────────────────────────────────────────────────────────
-- ML Feature Table: Review Star Rating Prediction Features
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `{{ project_id }}.{{ gold_dataset }}.ml_features_star_prediction` AS
SELECT
    r.review_id,
    -- TARGET: Star rating to predict
    r.stars                                         AS label,
    -- Business features
    b.stars                                         AS business_avg_stars,
    b.review_count                                  AS business_review_count,
    b.is_open                                       AS business_is_open,
    CASE 
        WHEN b.categories LIKE '%Restaurant%' THEN 'Restaurant'
        WHEN b.categories LIKE '%Shopping%' THEN 'Shopping'
        WHEN b.categories LIKE '%Food%' THEN 'Food'
        WHEN b.categories LIKE '%Health%' THEN 'Health'
        WHEN b.categories LIKE '%Beauty%' THEN 'Beauty'
        WHEN b.categories LIKE '%Home%' THEN 'Home Services'
        WHEN b.categories LIKE '%Nightlife%' THEN 'Nightlife'
        ELSE 'Other'
    END                                             AS business_category,
    b.city                                          AS business_city,
    b.state                                         AS business_state,
    -- User features
    u.review_count                                  AS user_review_count,
    u.average_stars                                 AS user_avg_stars,
    u.fans                                          AS user_fans,
    u.useful                                        AS user_useful_votes,
    CASE 
        WHEN u.elite IS NOT NULL AND LENGTH(u.elite) > 0 THEN 1 
        ELSE 0 
    END                                             AS user_is_elite,
    DATE_DIFF(CURRENT_DATE(), u.yelping_since, DAY) AS user_days_on_platform,
    -- Review text features
    LENGTH(r.text)                                  AS review_text_length,
    ARRAY_LENGTH(SPLIT(r.text, ' '))                AS review_word_count,
    r.useful                                        AS review_useful_votes,
    r.funny                                        AS review_funny_votes,
    r.cool                                         AS review_cool_votes,
    -- Temporal features
    EXTRACT(DAYOFWEEK FROM r.date)                  AS review_day_of_week,
    EXTRACT(MONTH FROM r.date)                      AS review_month,
    EXTRACT(YEAR FROM r.date)                       AS review_year
FROM `{{ project_id }}.{{ silver_dataset }}.reviews` AS r
JOIN `{{ project_id }}.{{ silver_dataset }}.businesses` AS b
    ON r.business_id = b.business_id
JOIN `{{ project_id }}.{{ silver_dataset }}.users` AS u
    ON r.user_id = u.user_id
WHERE r.stars IS NOT NULL
    AND r.text IS NOT NULL
    AND LENGTH(r.text) > 10
    AND b.is_current = TRUE
    AND u.is_current = TRUE;
