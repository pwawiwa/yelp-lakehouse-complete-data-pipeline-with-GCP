-- ─────────────────────────────────────────────────────────────────────
-- Deduplicate Reviews
-- Keeps the latest record per review_id based on _ingested_at
-- ─────────────────────────────────────────────────────────────────────

MERGE INTO `{{ project_id }}.{{ silver_dataset }}.reviews` AS target
USING (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY review_id
                ORDER BY _ingested_at DESC
            ) AS _row_num
        FROM `{{ project_id }}.{{ bronze_dataset }}.reviews`
    )
    WHERE _row_num = 1
) AS source
ON target.review_id = source.review_id
WHEN MATCHED AND source._ingested_at > target._ingested_at THEN
    UPDATE SET
        user_id         = source.user_id,
        business_id     = source.business_id,
        stars           = source.stars,
        useful          = source.useful,
        funny           = source.funny,
        cool            = source.cool,
        text            = source.text,
        date            = source.date,
        _ingested_at    = source._ingested_at,
        _source_file    = source._source_file,
        _schema_version = source._schema_version,
        _processed_at   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (review_id, user_id, business_id, stars, useful, funny, cool,
            text, date, _ingested_at, _source_file, _schema_version, _processed_at)
    VALUES (source.review_id, source.user_id, source.business_id, source.stars,
            source.useful, source.funny, source.cool, source.text, source.date,
            source._ingested_at, source._source_file, source._schema_version,
            CURRENT_TIMESTAMP());
