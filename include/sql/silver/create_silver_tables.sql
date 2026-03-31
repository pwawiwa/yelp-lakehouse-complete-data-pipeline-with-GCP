-- ─────────────────────────────────────────────────────────────────────
-- Create Silver Layer Tables with Partitioning & Clustering
-- ─────────────────────────────────────────────────────────────────────

-- Silver Reviews: Partitioned by date, clustered by business_id and stars
CREATE TABLE IF NOT EXISTS `{{ project_id }}.{{ silver_dataset }}.reviews` (
    review_id       STRING      NOT NULL,
    user_id         STRING      NOT NULL,
    business_id     STRING      NOT NULL,
    stars           INT64,
    useful          INT64,
    funny           INT64,
    cool            INT64,
    text                STRING,           -- PII-masked via DLP
    word_count          INT64,
    char_length         INT64,
    exclamation_count   INT64,
    date                DATE,
    _ingested_at        TIMESTAMP   NOT NULL,
    _source_file        STRING,
    _schema_version     INT64       NOT NULL,
    _processed_at       TIMESTAMP
)
PARTITION BY DATE_TRUNC(date, MONTH)
CLUSTER BY business_id, stars
OPTIONS (
    description = 'Silver layer: Cleansed and PII-masked Yelp reviews',
    labels = [('layer', 'silver'), ('entity', 'review')]
);

-- Silver Businesses: SCD Type 2, Clustered by city, state, categories  
CREATE TABLE IF NOT EXISTS `{{ project_id }}.{{ silver_dataset }}.businesses` (
    business_id     STRING      NOT NULL,
    name            STRING,
    address         STRING,           -- PII-masked via DLP
    city            STRING,
    state           STRING,
    postal_code     STRING,
    latitude        FLOAT64,
    longitude       FLOAT64,
    stars           FLOAT64,
    review_count    INT64,
    is_open         INT64,
    categories      STRING,
    attributes_json STRING,
    hours_json      STRING,
    data_quality_tag STRING,
    -- SCD Type 2 tracking columns
    is_current      BOOLEAN     NOT NULL,
    valid_from      TIMESTAMP   NOT NULL,
    valid_to        TIMESTAMP   NOT NULL,
    hash_diff       INT64       NOT NULL,  -- FAANG Optimization: Hash for change detection
    _ingested_at    TIMESTAMP   NOT NULL,
    _source_file    STRING,
    _schema_version INT64       NOT NULL,
    _processed_at   TIMESTAMP
)
CLUSTER BY business_id, is_current    -- FAANG Optimization: Cluster by ID + current flag
OPTIONS (
    description = 'Silver layer: SCD Type 2 Cleansed Yelp businesses',
    labels = [('layer', 'silver'), ('entity', 'business'), ('scd', 'type2')]
);


-- Silver Users: SCD Type 2, Partitioned by yelping_since month, clustered by review_count
CREATE TABLE IF NOT EXISTS `{{ project_id }}.{{ silver_dataset }}.users` (
    user_id             STRING      NOT NULL,
    name                STRING,           -- PII-masked via DLP
    review_count        INT64,
    yelping_since       DATE,
    useful              INT64,
    funny               INT64,
    cool                INT64,
    elite               STRING,
    friends             STRING,           -- PII-masked via DLP
    fans                INT64,
    average_stars       FLOAT64,
    compliment_hot      INT64,
    compliment_more     INT64,
    compliment_profile  INT64,
    compliment_cute     INT64,
    compliment_list     INT64,
    compliment_note     INT64,
    compliment_plain    INT64,
    compliment_cool     INT64,
    compliment_funny    INT64,
    compliment_writer   INT64,
    compliment_photos   INT64,
    user_loyalty_tier   STRING,
    -- SCD Type 2 tracking columns
    is_current          BOOLEAN     NOT NULL,
    valid_from          TIMESTAMP   NOT NULL,
    valid_to            TIMESTAMP   NOT NULL,
    hash_diff           INT64       NOT NULL,  -- FAANG Optimization: Hash for change detection
    _ingested_at        TIMESTAMP   NOT NULL,
    _source_file        STRING,
    _schema_version     INT64       NOT NULL,
    _processed_at       TIMESTAMP
)
PARTITION BY DATE_TRUNC(yelping_since, MONTH)
CLUSTER BY review_count
OPTIONS (
    description = 'Silver layer: SCD Type 2 Cleansed and PII-masked Yelp users',
    labels = [('layer', 'silver'), ('entity', 'user'), ('scd', 'type2')]
);


-- Silver Checkins: Clustered by business_id
CREATE TABLE IF NOT EXISTS `{{ project_id }}.{{ silver_dataset }}.checkins` (
    business_id     STRING      NOT NULL,
    date            TIMESTAMP,
    _ingested_at    TIMESTAMP   NOT NULL,
    _source_file    STRING,
    _schema_version INT64       NOT NULL,
    _processed_at   TIMESTAMP
)
CLUSTER BY business_id
OPTIONS (
    description = 'Silver layer: Cleansed Yelp check-ins',
    labels = [('layer', 'silver'), ('entity', 'checkin')]
);

-- Silver Tips: Partitioned by date, clustered by business_id
CREATE TABLE IF NOT EXISTS `{{ project_id }}.{{ silver_dataset }}.tips` (
    user_id           STRING      NOT NULL,
    business_id       STRING      NOT NULL,
    text              STRING,           -- PII-masked via DLP
    date              DATE,
    compliment_count  INT64,
    _ingested_at      TIMESTAMP   NOT NULL,
    _source_file      STRING,
    _schema_version   INT64       NOT NULL,
    _processed_at     TIMESTAMP
)
PARTITION BY DATE_TRUNC(date, MONTH)
CLUSTER BY business_id
OPTIONS (
    description = 'Silver layer: Cleansed and PII-masked Yelp tips',
    labels = [('layer', 'silver'), ('entity', 'tip')]
);
