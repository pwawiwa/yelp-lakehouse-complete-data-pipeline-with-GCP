CREATE OR REPLACE TABLE `{{ project_id }}.{{ gold_dataset }}.business_performance` AS
WITH review_agg AS (
    SELECT 
        business_id,
        COUNT(review_id) AS total_reviews,
        AVG(stars) AS avg_review_stars,
        SUM(useful) AS total_useful_votes,
        COUNTIF(stars >= 4) AS positive_reviews
    FROM `{{ project_id }}.{{ silver_dataset }}.reviews`
    GROUP BY 1
),
tip_agg AS (
    SELECT 
        business_id,
        COUNT(DISTINCT user_id) AS unique_tippers
    FROM `{{ project_id }}.{{ silver_dataset }}.tips`
    GROUP BY 1
)
SELECT
    b.business_id,
    b.name,
    b.stars AS snapshot_stars,
    r.total_reviews,
    r.avg_review_stars,
    t.unique_tippers,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM `{{ project_id }}.{{ silver_dataset }}.businesses` AS b
LEFT JOIN review_agg r ON b.business_id = r.business_id
LEFT JOIN tip_agg t ON b.business_id = t.business_id
WHERE b.is_current = TRUE;