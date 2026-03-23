-- ─────────────────────────────────────────────────────────────────────
-- Gold Layer: Business Performance Aggregations
-- ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `{{ project_id }}.{{ gold_dataset }}.business_performance` AS
SELECT
    b.business_id,
    b.name,
    b.city,
    b.state,
    b.categories,
    b.stars                                         AS avg_stars,
    b.review_count,
    b.is_open,
    -- Review aggregations
    COUNT(r.review_id)                              AS total_reviews,
    AVG(r.stars)                                    AS avg_review_stars,
    STDDEV(r.stars)                                 AS stddev_review_stars,
    MIN(r.date)                                     AS first_review_date,
    MAX(r.date)                                     AS last_review_date,
    DATE_DIFF(MAX(r.date), MIN(r.date), DAY)        AS review_span_days,
    SUM(r.useful)                                   AS total_useful_votes,
    SUM(r.funny)                                    AS total_funny_votes,
    SUM(r.cool)                                     AS total_cool_votes,
    -- Engagement score
    SAFE_DIVIDE(
        SUM(r.useful) + SUM(r.funny) + SUM(r.cool),
        COUNT(r.review_id)
    )                                               AS avg_engagement_per_review,
    -- Review trend (positive vs negative)
    COUNTIF(r.stars >= 4)                            AS positive_reviews,
    COUNTIF(r.stars <= 2)                            AS negative_reviews,
    SAFE_DIVIDE(
        COUNTIF(r.stars >= 4),
        COUNT(r.review_id)
    )                                               AS positive_ratio,
    -- Tip count
    COUNT(DISTINCT t.user_id)                        AS unique_tippers,
    CURRENT_TIMESTAMP()                              AS _refreshed_at
FROM `{{ project_id }}.{{ silver_dataset }}.businesses` AS b
LEFT JOIN `{{ project_id }}.{{ silver_dataset }}.reviews` AS r
    ON b.business_id = r.business_id
LEFT JOIN `{{ project_id }}.{{ silver_dataset }}.tips` AS t
    ON b.business_id = t.business_id
WHERE b.is_current = TRUE
GROUP BY
    b.business_id, b.name, b.city, b.state, b.categories,
    b.stars, b.review_count, b.is_open;
