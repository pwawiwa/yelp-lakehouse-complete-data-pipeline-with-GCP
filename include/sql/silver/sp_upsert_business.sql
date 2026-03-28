CREATE OR REPLACE PROCEDURE `{{ project_id }}.{{ silver_dataset }}.sp_upsert_business`(
    p_project_id STRING,
    p_bronze_dataset STRING,
    p_silver_dataset STRING,
    p_dt STRING
)
BEGIN
    /*
    -- ─────────────────────────────────────────────────────────────────────
    -- FAANG-Level Stored Procedure for Business Transformation (SCD 2)
    -- Optimized with Single-Pass MERGE, Hash-Diff, and Re-run Safety
    -- ─────────────────────────────────────────────────────────────────────
    */

    -- Step 1: Use a static timestamp for the batch to ensure consistency
    DECLARE processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Step 2: Create local stage with hash_diff and deduplication
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
        -- Deduplication: Use review_count as a proxy for the latest update
        SELECT *, ROW_NUMBER() OVER (PARTITION BY business_id ORDER BY review_count DESC, stars DESC) AS _rn
        FROM `{{ project_id }}.{{ bronze_dataset }}.business`
        WHERE dt = COALESCE(p_dt, '2026-03-28')
    )
    WHERE _rn = 1;

    -- Step 3: Single-Pass SCD Type 2 MERGE
    -- Handles both record expiration and new version insertion
    MERGE INTO `{{ project_id }}.{{ silver_dataset }}.businesses` AS target
    USING (
        -- Action 1: UPDATE (To expire old records)
        SELECT business_id, hash_diff, 'UPDATE' AS _action, s.* EXCEPT(business_id, hash_diff)
        FROM stage_business s
        
        UNION ALL
        
        -- Action 2: INSERT (Only if it's actually a NEW business or a CHANGE in data)
        SELECT business_id, hash_diff, 'INSERT' AS _action, s.* EXCEPT(business_id, hash_diff)
        FROM stage_business s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM `{{ project_id }}.{{ silver_dataset }}.businesses` t
            WHERE TRIM(t.business_id) = TRIM(s.business_id) 
              AND t.hash_diff = s.hash_diff 
              AND t.is_current = TRUE
        )
    ) AS source
    ON target.business_id = source.business_id 
       AND target.is_current = TRUE
       AND source._action = 'UPDATE'

    -- 1. Expire existing rows where attributes changed
    -- If hashes are equal, this block is skipped (Idempotency)
    WHEN MATCHED AND target.hash_diff != source.hash_diff THEN
        UPDATE SET
            is_current = FALSE,
            valid_to   = processing_time,
            _processed_at = processing_time

    -- 2. Insert new versions (Only if they passed the NOT EXISTS filter)
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
            processing_time,                -- valid_from
            TIMESTAMP('9999-12-31'),        -- valid_to
            source.hash_diff,
            processing_time,
            'bronze_external_table',
            1,
            processing_time
        );
END;
