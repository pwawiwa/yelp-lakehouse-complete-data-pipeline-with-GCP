-- ─────────────────────────────────────────────────────────────────────
-- FAANG-Level User Transformation (SCD Type 2)
-- Optimized with Single-Pass MERGE and Hash-Diff Change Detection
-- ─────────────────────────────────────────────────────────────────────

-- Step 1: Create local stage with hash_diff and deduplication
-- This ensures GCS Bronze JSON is scanned exactly ONCE.
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
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY yelping_since DESC) AS _rn
    FROM `{{ project_id }}.{{ bronze_dataset }}.user`
)
WHERE _rn = 1;

-- Step 2: Single-Pass SCD Type 2 MERGE
MERGE INTO `{{ project_id }}.{{ silver_dataset }}.users` AS target
USING (
    -- Data for "INACTIVATE" old records
    SELECT user_id, hash_diff, 'UPDATE' AS _action, s.* EXCEPT(user_id, hash_diff)
    FROM stage_user s
    UNION ALL
    -- Data for "INSERT" new/changed records
    SELECT user_id, hash_diff, 'INSERT' AS _action, s.* EXCEPT(user_id, hash_diff)
    FROM stage_user s
) AS source
ON target.user_id = source.user_id 
   AND target.is_current = TRUE
   AND source._action = 'UPDATE'

-- 1. Expire existing rows where attributes changed
-- Using hash_diff for O(1) comparison
WHEN MATCHED AND IFNULL(target.hash_diff, -1) != source.hash_diff THEN
    UPDATE SET
        is_current = FALSE,
        valid_to   = CURRENT_TIMESTAMP(),
        _processed_at = CURRENT_TIMESTAMP()

-- 2. Insert new versions (both brand new IDs and changed versions)
WHEN NOT MATCHED BY TARGET AND source._action = 'INSERT' THEN
    INSERT (
        user_id, name, review_count, yelping_since, useful, funny, cool,
        elite, friends, fans, average_stars,
        compliment_hot, compliment_more, compliment_profile,
        compliment_cute, compliment_list, compliment_note,
        compliment_plain, compliment_cool, compliment_funny,
        compliment_writer, compliment_photos,
        is_current, valid_from, valid_to, hash_diff,
        _ingested_at, _source_file, _schema_version, _processed_at
    )
    VALUES (
        source.user_id,
        -- PII Masking: keep first initial
        CASE 
            WHEN LENGTH(source.name) > 1 THEN CONCAT(SUBSTR(source.name, 1, 1), '***')
            ELSE '***'
        END,
        SAFE_CAST(source.review_count AS INT64),
        SAFE_CAST(source.yelping_since AS DATE),
        SAFE_CAST(source.useful AS INT64),
        SAFE_CAST(source.funny AS INT64),
        SAFE_CAST(source.cool AS INT64),
        SAFE_CAST(source.elite AS STRING),
        source.friends,
        SAFE_CAST(source.fans AS INT64),
        SAFE_CAST(source.average_stars AS FLOAT64),
        SAFE_CAST(source.compliment_hot AS INT64),
        SAFE_CAST(source.compliment_more AS INT64),
        SAFE_CAST(source.compliment_profile AS INT64),
        SAFE_CAST(source.compliment_cute AS INT64),
        SAFE_CAST(source.compliment_list AS INT64),
        SAFE_CAST(source.compliment_note AS INT64),
        SAFE_CAST(source.compliment_plain AS INT64),
        SAFE_CAST(source.compliment_cool AS INT64),
        SAFE_CAST(source.compliment_funny AS INT64),
        SAFE_CAST(source.compliment_writer AS INT64),
        SAFE_CAST(source.compliment_photos AS INT64),
        TRUE,                           -- is_current
        CURRENT_TIMESTAMP(),            -- valid_from
        TIMESTAMP('9999-12-31'),        -- valid_to
        source.hash_diff,
        CURRENT_TIMESTAMP(),
        'bronze_external_table',
        1,
        CURRENT_TIMESTAMP()
    );
