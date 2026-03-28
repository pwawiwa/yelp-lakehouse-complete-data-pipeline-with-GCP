-- ─────────────────────────────────────────────────────────────────────
-- Tips Transformation (Incremental + PII Masking)
-- ─────────────────────────────────────────────────────────────────────

DECLARE processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

MERGE INTO `{{ project_id }}.{{ silver_dataset }}.tips` AS target
USING (
    SELECT
        user_id,
        business_id,
        -- PII Masking (Email & Phone)
        REGEXP_REPLACE(
            REGEXP_REPLACE(text, r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '[EMAIL]'),
            r'\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', '[PHONE]'
        ) AS clean_text,
        SAFE_CAST(date AS DATE) AS tip_date,
        SAFE_CAST(compliment_count AS INT64) AS compliment_count,
        processing_time AS _ingested_at
    FROM (
        -- Deduplication (Partition by casted DATE to ensure uniqueness matches the ON condition granularity)
        SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, business_id, SAFE_CAST(date AS DATE) ORDER BY date DESC) AS _rn
        FROM `{{ project_id }}.{{ bronze_dataset }}.tip`
        WHERE dt = '{{ ds | default(macros.datetime.now().strftime("%Y-%m-%d")) }}'
    )
    WHERE _rn = 1
) AS source
ON target.user_id = source.user_id 
   AND target.business_id = source.business_id 
   AND target.date = source.tip_date

WHEN MATCHED THEN
    UPDATE SET
        text = source.clean_text,
        compliment_count = source.compliment_count,
        _processed_at = processing_time

WHEN NOT MATCHED THEN
    INSERT (user_id, business_id, text, date, compliment_count, _ingested_at, _source_file, _schema_version, _processed_at)
    VALUES (source.user_id, source.business_id, source.clean_text, source.tip_date, source.compliment_count, source._ingested_at, 'bronze_external_table', 1, processing_time);
