-- ─────────────────────────────────────────────────────────────────────
-- FAANG-Level Review Transformation (SCD Type 1 + Enrichment)
-- Optimized with Text Metadata and Advanced Cleaning
-- ─────────────────────────────────────────────────────────────────────

-- Step 1: Use a static timestamp for the batch to ensure consistency
DECLARE processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

MERGE INTO `{{ project_id }}.{{ silver_dataset }}.reviews` AS target
USING (
    SELECT
        review_id, 
        user_id, 
        business_id,
        SAFE_CAST(stars AS INT64) AS stars,
        SAFE_CAST(useful AS INT64) AS useful,
        SAFE_CAST(funny AS INT64) AS funny,
        SAFE_CAST(cool AS INT64) AS cool,
        -- 1. PII Masking (Email & Phone)
        REGEXP_REPLACE(
            REGEXP_REPLACE(text, r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '[EMAIL]'),
            r'\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', '[PHONE]'
        ) AS clean_text,
        -- 2. Text Enrichment: Word Count & Character Length
        ARRAY_LENGTH(SPLIT(text, ' ')) AS word_count,
        LENGTH(text) AS char_length,
        -- 3. Simple Sentiment Proxy (Count '!' and '?')
        LENGTH(REGEXP_REPLACE(text, r'[^!]', '')) AS exclamation_count,
        SAFE_CAST(CAST(date AS TIMESTAMP) AS DATE) AS review_date,
        processing_time AS _ingested_at
    FROM (
        -- Deduplication: Ensure we only take the latest version of a review
        SELECT *, ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY date DESC) AS _rn
        FROM `{{ project_id }}.{{ bronze_dataset }}.review`
        WHERE dt = '{{ jakarta_date(dag_run.logical_date) }}'
    )
    WHERE _rn = 1
) AS source
ON target.review_id = source.review_id

-- If the review exists, update the metrics (useful/funny/cool) which change over time
WHEN MATCHED THEN
    UPDATE SET
        stars           = source.stars,
        useful          = source.useful,
        funny           = source.funny,
        cool            = source.cool,
        text            = source.clean_text,
        word_count      = source.word_count,
        _processed_at   = processing_time

-- If it's a brand new review
WHEN NOT MATCHED THEN
    INSERT (
        review_id, user_id, business_id, stars, useful, funny, cool,
        text, word_count, char_length, exclamation_count, 
        date, _ingested_at, _source_file, _schema_version, _processed_at
    )
    VALUES (
        source.review_id, source.user_id, source.business_id, source.stars, 
        source.useful, source.funny, source.cool, source.clean_text, 
        source.word_count, source.char_length, source.exclamation_count,
        source.review_date, source._ingested_at, 'bronze_external_table', 1, processing_time
    );