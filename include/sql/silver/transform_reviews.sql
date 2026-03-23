-- ─────────────────────────────────────────────────────────────────────
-- FAANG-Level Review Transformation (SCD Type 1)
-- Optimized with Partition Pruning and Incremental Logic
-- ─────────────────────────────────────────────────────────────────────

MERGE INTO `{{ project_id }}.{{ silver_dataset }}.reviews` AS target
USING (
    -- Step 1: Deduplicate and prepare source data
    -- Only scan recently ingested files if possible (incremental logic)
    SELECT
        review_id, user_id, business_id,
        SAFE_CAST(stars AS INT64) AS stars,
        SAFE_CAST(useful AS INT64) AS useful,
        SAFE_CAST(funny AS INT64) AS funny,
        SAFE_CAST(cool AS INT64) AS cool,
        -- Regex-based PII Masking
        REGEXP_REPLACE(
            REGEXP_REPLACE(text, r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '[EMAIL]'),
            r'\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', '[PHONE]'
        ) AS text,
        SAFE_CAST(date AS DATE) AS date,
        CURRENT_TIMESTAMP() AS _ingested_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY date DESC) AS _rn
        FROM `{{ project_id }}.{{ bronze_dataset }}.review`
        -- External Table: Full refresh or use _FILE_NAME for advanced incrementals
    )
    WHERE _rn = 1
) AS source
ON target.review_id = source.review_id
   -- FAANG Optimization: Partition Pruning on target
   -- BigQuery only scans the last 7 days of partitions for the join
   AND target.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 

WHEN MATCHED THEN
    UPDATE SET
        user_id         = source.user_id,
        business_id     = source.business_id,
        stars           = source.stars,
        useful          = source.useful,
        funny           = source.funny,
        cool            = source.cool,
        text            = source.text,
        date            = source.date,
        _processed_at   = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (review_id, user_id, business_id, stars, useful, funny, cool,
            text, date, _ingested_at, _source_file, _schema_version, _processed_at)
    VALUES (source.review_id, source.user_id, source.business_id, source.stars,
            source.useful, source.funny, source.cool, source.text, source.date,
            source._ingested_at, 'bronze_external_table', 1, CURRENT_TIMESTAMP());
