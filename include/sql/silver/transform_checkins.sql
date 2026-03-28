-- ─────────────────────────────────────────────────────────────────────
-- Checkins Transformation (Incremental)
-- ─────────────────────────────────────────────────────────────────────

DECLARE processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

MERGE INTO `{{ project_id }}.{{ silver_dataset }}.checkins` AS target
USING (
    SELECT
        business_id,
        CAST(TRIM(checkin_date) AS TIMESTAMP) AS date,
        processing_time AS _ingested_at
    FROM `{{ project_id }}.{{ bronze_dataset }}.checkin`, 
         UNNEST(SPLIT(date, ',')) AS checkin_date
    WHERE dt = '{{ ds | default(macros.datetime.now().strftime("%Y-%m-%d")) }}'
      AND TRIM(checkin_date) != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY business_id, TRIM(checkin_date) ORDER BY business_id) = 1
) AS source
ON target.business_id = source.business_id 
   AND target.date = source.date

WHEN NOT MATCHED THEN
    INSERT (business_id, date, _ingested_at, _source_file, _schema_version, _processed_at)
    VALUES (source.business_id, source.date, source._ingested_at, 'bronze_external_table', 1, processing_time);
