-- ─────────────────────────────────────────────────────────────────────
-- Gold Layer: User Engagement Analytics
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `{{ project_id }}.{{ gold_dataset }}.user_engagement` AS
SELECT
    u.user_id,
    u.name,
    u.review_count,
    u.yelping_since,
    u.fans,
    u.average_stars,
    -- Elite status
    CASE 
        WHEN u.elite IS NOT NULL AND LENGTH(u.elite) > 0 THEN TRUE 
        ELSE FALSE 
    END                                             AS is_elite,
    ARRAY_LENGTH(SPLIT(u.elite, ','))                AS elite_years_count,
    -- Engagement metrics
    u.useful + u.funny + u.cool                      AS total_votes_received,
    SAFE_DIVIDE(
        u.useful + u.funny + u.cool,
        u.review_count
    )                                               AS avg_votes_per_review,
    -- Compliment score
    u.compliment_hot + u.compliment_more + u.compliment_profile +
    u.compliment_cute + u.compliment_list + u.compliment_note +
    u.compliment_plain + u.compliment_cool + u.compliment_funny +
    u.compliment_writer + u.compliment_photos        AS total_compliments,
    -- Friend network size
    CASE
        WHEN u.friends IS NOT NULL AND u.friends != 'None'
        THEN ARRAY_LENGTH(SPLIT(u.friends, ','))
        ELSE 0
    END                                             AS friend_count,
    -- Activity duration
    DATE_DIFF(CURRENT_DATE(), u.yelping_since, DAY)  AS days_on_platform,
    SAFE_DIVIDE(
        u.review_count,
        GREATEST(DATE_DIFF(CURRENT_DATE(), u.yelping_since, DAY), 1)
    ) * 365                                         AS reviews_per_year,
    -- User tier
    CASE
        WHEN u.review_count >= 500 AND u.fans >= 50 THEN 'Power User'
        WHEN u.review_count >= 100 AND u.fans >= 10 THEN 'Active User'
        WHEN u.review_count >= 10 THEN 'Regular User'
        ELSE 'Casual User'
    END                                             AS user_tier,
    CURRENT_TIMESTAMP()                              AS _refreshed_at
FROM `{{ project_id }}.{{ silver_dataset }}.users` AS u
WHERE u.user_id IS NOT NULL
  AND u.is_current = TRUE;
