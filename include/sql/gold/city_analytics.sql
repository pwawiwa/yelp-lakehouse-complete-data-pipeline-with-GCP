-- ─────────────────────────────────────────────────────────────────────
-- Gold Layer: City-Level Analytics for BI Dashboards
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `{{ project_id }}.{{ gold_dataset }}.city_analytics` AS
SELECT
    b.city,
    b.state,
    COUNT(DISTINCT b.business_id)                   AS total_businesses,
    COUNTIF(b.is_open = 1)                          AS open_businesses,
    COUNTIF(b.is_open = 0)                          AS closed_businesses,
    SAFE_DIVIDE(COUNTIF(b.is_open = 0), COUNT(DISTINCT b.business_id)) AS closure_rate,
    AVG(b.stars)                                    AS avg_business_stars,
    SUM(b.review_count)                             AS total_reviews,
    AVG(b.review_count)                             AS avg_reviews_per_business,
    -- Top categories (most common)
    APPROX_TOP_COUNT(
        b.categories,
        5
    )                                               AS top_category_combinations,
    -- Geographic center
    AVG(b.latitude)                                 AS center_latitude,
    AVG(b.longitude)                                AS center_longitude,
    -- Review quality
    AVG(r_agg.avg_review_stars)                     AS avg_review_quality,
    SUM(r_agg.total_engagement)                     AS total_engagement,
    CURRENT_TIMESTAMP()                             AS _refreshed_at
FROM `{{ project_id }}.{{ silver_dataset }}.businesses` AS b
LEFT JOIN (
    SELECT
        business_id,
        AVG(stars) AS avg_review_stars,
        SUM(useful + funny + cool) AS total_engagement
    FROM `{{ project_id }}.{{ silver_dataset }}.reviews`
    GROUP BY business_id
) AS r_agg
    ON b.business_id = r_agg.business_id
WHERE b.city IS NOT NULL
  AND b.is_current = TRUE
GROUP BY b.city, b.state
HAVING total_businesses >= 5
ORDER BY total_businesses DESC;
