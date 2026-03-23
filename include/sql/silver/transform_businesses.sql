-- ─────────────────────────────────────────────────────────────────────
-- FAANG-Level Business Transformation (SCD Type 2)
-- Optimized with Single-Pass MERGE and Hash-Diff Change Detection
-- ─────────────────────────────────────────────────────────────────────

-- Step 1: Create local stage with hash_diff and deduplication
-- This ensures GCS Bronze JSON is scanned exactly ONCE.
CREATE OR REPLACE TEMP TABLE stage_business AS
SELECT
    *,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
        name, address, city, state, postal_code,
        SAFE_CAST(stars AS FLOAT64),
        SAFE_CAST(review_count AS INT64),
        SAFE_CAST(is_open AS INT64),
        categories
    ))) AS hash_diff
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY business_id ORDER BY name) AS _rn
    FROM `{{ project_id }}.{{ bronze_dataset }}.business`
)
WHERE _rn = 1;

-- Step 2: Single-Pass SCD Type 2 MERGE
-- Handles both record expiration and new version insertion
MERGE INTO `{{ project_id }}.{{ silver_dataset }}.businesses` AS target
USING (
    -- Data for "INACTIVATE" old records
    SELECT business_id, hash_diff, 'UPDATE' AS _action, s.* EXCEPT(business_id, hash_diff)
    FROM stage_business s
    UNION ALL
    -- Data for "INSERT" new/changed records
    SELECT business_id, hash_diff, 'INSERT' AS _action, s.* EXCEPT(business_id, hash_diff)
    FROM stage_business s
) AS source
-- Join on Business ID and Current flag
ON target.business_id = source.business_id 
   AND target.is_current = TRUE
   -- Optimization: Use the source action to distinguish
   AND source._action = 'UPDATE'

-- 1. Expire existing rows where attributes changed
WHEN MATCHED AND IFNULL(target.hash_diff, -1) != source.hash_diff THEN
    UPDATE SET
        is_current = FALSE,
        valid_to   = CURRENT_TIMESTAMP(),
        _processed_at = CURRENT_TIMESTAMP()

-- 2. Insert new versions (both brand new IDs and changed versions)
WHEN NOT MATCHED BY TARGET AND source._action = 'INSERT' THEN
    INSERT (
        business_id, name, address, city, state, postal_code,
        latitude, longitude, stars, review_count, is_open,
        categories, attributes_json, hours_json,
        is_current, valid_from, valid_to,
        hash_diff,
        _ingested_at, _source_file, _schema_version, _processed_at
    )
    VALUES (
        source.business_id, source.name, source.address, source.city, source.state, source.postal_code,
        SAFE_CAST(source.latitude AS FLOAT64), SAFE_CAST(source.longitude AS FLOAT64),
        SAFE_CAST(source.stars AS FLOAT64), SAFE_CAST(source.review_count AS INT64),
        SAFE_CAST(source.is_open AS INT64),
        source.categories, TO_JSON_STRING(source.attributes), TO_JSON_STRING(source.hours),
        TRUE,                           -- is_current
        CURRENT_TIMESTAMP(),            -- valid_from
        TIMESTAMP('9999-12-31'),        -- valid_to
        source.hash_diff,
        CURRENT_TIMESTAMP(),
        'bronze_external_table',
        1,
        CURRENT_TIMESTAMP()
    );
