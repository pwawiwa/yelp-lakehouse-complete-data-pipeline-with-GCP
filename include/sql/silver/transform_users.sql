-- ─────────────────────────────────────────────────────────────────────
-- FAANG-Level User Transformation (SCD Type 2) - IDEMPOTENT VERSION
-- Optimized for PII Compliance and Single-Pass MERGE
-- ─────────────────────────────────────────────────────────────────────

-- Step 1: Create local stage with hash_diff and deduplication
CREATE OR REPLACE TEMP TABLE stage_user AS
SELECT
    *,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
        review_count, yelping_since, useful, funny, cool,
        elite, friends, fans, average_stars
    ))) AS hash_diff
FROM (
    SELECT 
        *, 
        -- Deduplicate: Take the latest record based on yelping_since or ingestion time
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY yelping_since DESC) AS _rn
    FROM `{{ project_id }}.{{ bronze_dataset }}.user`
)
WHERE _rn = 1;

-- Step 2: Set a unified processing timestamp
DECLARE processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

-- Step 3: Single-Pass SCD Type 2 MERGE
MERGE INTO `{{ project_id }}.{{ silver_dataset }}.users` AS target
USING (
    -- Action 1: UPDATE (To expire old records)
    SELECT user_id, hash_diff, 'UPDATE' AS _action, s.* EXCEPT(user_id, hash_diff)
    FROM stage_user s
    
    UNION ALL
    
    -- Action 2: INSERT (Only if it's actually a NEW user or a CHANGE in data)
    SELECT user_id, hash_diff, 'INSERT' AS _action, s.* EXCEPT(user_id, hash_diff)
    FROM stage_user s
    WHERE NOT EXISTS (
        SELECT 1 FROM `{{ project_id }}.{{ silver_dataset }}.users` t
        WHERE t.user_id = s.user_id 
          AND t.hash_diff = s.hash_diff 
          AND t.is_current = TRUE
    )
) AS source
ON target.user_id = source.user_id 
   AND target.is_current = TRUE
   AND source._action = 'UPDATE'

-- 1. Expire existing rows where attributes changed
WHEN MATCHED AND target.hash_diff != source.hash_diff THEN
    UPDATE SET
        is_current = FALSE,
        valid_to   = processing_time,
        _processed_at = processing_time

-- 2. Insert new versions
WHEN NOT MATCHED BY TARGET AND source._action = 'INSERT' THEN
    INSERT (
        user_id, name, review_count, yelping_since, useful, funny, cool,
        elite, friends, fans, average_stars,
        is_current, valid_from, valid_to, hash_diff,
        _ingested_at, _source_file, _schema_version, _processed_at
    )
    VALUES (
        source.user_id,
        -- PII Masking: Redact name to first initial for privacy compliance
        IF(LENGTH(source.name) > 0, CONCAT(LEFT(source.name, 1), '.'), 'Anonymous'),
        SAFE_CAST(source.review_count AS INT64),
        SAFE_CAST(source.yelping_since AS DATE),
        SAFE_CAST(source.useful AS INT64),
        SAFE_CAST(source.funny AS INT64),
        SAFE_CAST(source.cool AS INT64),
        source.elite,
        source.friends,
        SAFE_CAST(source.fans AS INT64),
        SAFE_CAST(source.average_stars AS FLOAT64),
        TRUE,                           -- is_current
        processing_time,                -- valid_from
        TIMESTAMP('9999-12-31'),        -- valid_to
        source.hash_diff,
        processing_time,
        'bronze_external_table',
        1,
        processing_time
    );