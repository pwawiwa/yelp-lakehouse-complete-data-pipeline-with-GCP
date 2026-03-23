"""
Yelp Dataset Schema Definitions.

Provides BigQuery schema definitions and Iceberg schema mappings
for all 5 Yelp entities: business, review, user, checkin, tip.

Supports schema evolution by tracking schema versions.
"""

from google.cloud.bigquery import SchemaField

# ── Schema Version Tracking ───────────────────────────────────────
# Increment when adding new columns. Never remove columns (backward compat).
SCHEMA_VERSIONS = {
    "business": 1,
    "review": 1,
    "user": 1,
    "checkin": 1,
    "tip": 1,
}

# ── BigQuery Schema: Business ─────────────────────────────────────

BUSINESS_SCHEMA = [
    SchemaField("business_id", "STRING", mode="REQUIRED", description="Unique 22-char business ID"),
    SchemaField("name", "STRING", mode="REQUIRED", description="Business name"),
    SchemaField("address", "STRING", mode="NULLABLE", description="Full street address"),
    SchemaField("city", "STRING", mode="NULLABLE", description="City name"),
    SchemaField("state", "STRING", mode="NULLABLE", description="2-char state code"),
    SchemaField("postal_code", "STRING", mode="NULLABLE", description="Postal code"),
    SchemaField("latitude", "FLOAT64", mode="NULLABLE", description="Latitude coordinate"),
    SchemaField("longitude", "FLOAT64", mode="NULLABLE", description="Longitude coordinate"),
    SchemaField("stars", "FLOAT64", mode="NULLABLE", description="Average star rating (1.0-5.0)"),
    SchemaField("review_count", "INT64", mode="NULLABLE", description="Number of reviews"),
    SchemaField("is_open", "INT64", mode="NULLABLE", description="0=closed, 1=open"),
    SchemaField("categories", "STRING", mode="NULLABLE", description="Comma-separated category list"),
    # Nested JSON fields flattened
    SchemaField("attributes_json", "STRING", mode="NULLABLE", description="Raw attributes JSON"),
    SchemaField("hours_json", "STRING", mode="NULLABLE", description="Raw hours JSON"),
    # Metadata
    SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED", description="Ingestion timestamp"),
    SchemaField("_source_file", "STRING", mode="NULLABLE", description="Source file path"),
    SchemaField("_schema_version", "INT64", mode="REQUIRED", description="Schema version"),
]

# ── BigQuery Schema: Review ───────────────────────────────────────

REVIEW_SCHEMA = [
    SchemaField("review_id", "STRING", mode="REQUIRED", description="Unique 22-char review ID"),
    SchemaField("user_id", "STRING", mode="REQUIRED", description="User ID who wrote the review"),
    SchemaField("business_id", "STRING", mode="REQUIRED", description="Business being reviewed"),
    SchemaField("stars", "INT64", mode="NULLABLE", description="Star rating (1-5)"),
    SchemaField("useful", "INT64", mode="NULLABLE", description="Useful votes received"),
    SchemaField("funny", "INT64", mode="NULLABLE", description="Funny votes received"),
    SchemaField("cool", "INT64", mode="NULLABLE", description="Cool votes received"),
    SchemaField("text", "STRING", mode="NULLABLE", description="Full review text (may contain PII)"),
    SchemaField("date", "DATE", mode="NULLABLE", description="Review date"),
    # Metadata
    SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED", description="Ingestion timestamp"),
    SchemaField("_source_file", "STRING", mode="NULLABLE", description="Source file path"),
    SchemaField("_schema_version", "INT64", mode="REQUIRED", description="Schema version"),
]

# ── BigQuery Schema: User ─────────────────────────────────────────

USER_SCHEMA = [
    SchemaField("user_id", "STRING", mode="REQUIRED", description="Unique 22-char user ID"),
    SchemaField("name", "STRING", mode="NULLABLE", description="User first name (PII)"),
    SchemaField("review_count", "INT64", mode="NULLABLE", description="Number of reviews written"),
    SchemaField("yelping_since", "DATE", mode="NULLABLE", description="Date user joined Yelp"),
    SchemaField("useful", "INT64", mode="NULLABLE", description="Total useful votes received"),
    SchemaField("funny", "INT64", mode="NULLABLE", description="Total funny votes received"),
    SchemaField("cool", "INT64", mode="NULLABLE", description="Total cool votes received"),
    SchemaField("elite", "STRING", mode="NULLABLE", description="Comma-separated list of elite years"),
    SchemaField("friends", "STRING", mode="NULLABLE", description="Comma-separated friend user IDs"),
    SchemaField("fans", "INT64", mode="NULLABLE", description="Number of fans"),
    SchemaField("average_stars", "FLOAT64", mode="NULLABLE", description="Average review star rating"),
    SchemaField("compliment_hot", "INT64", mode="NULLABLE"),
    SchemaField("compliment_more", "INT64", mode="NULLABLE"),
    SchemaField("compliment_profile", "INT64", mode="NULLABLE"),
    SchemaField("compliment_cute", "INT64", mode="NULLABLE"),
    SchemaField("compliment_list", "INT64", mode="NULLABLE"),
    SchemaField("compliment_note", "INT64", mode="NULLABLE"),
    SchemaField("compliment_plain", "INT64", mode="NULLABLE"),
    SchemaField("compliment_cool", "INT64", mode="NULLABLE"),
    SchemaField("compliment_funny", "INT64", mode="NULLABLE"),
    SchemaField("compliment_writer", "INT64", mode="NULLABLE"),
    SchemaField("compliment_photos", "INT64", mode="NULLABLE"),
    # Metadata
    SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED", description="Ingestion timestamp"),
    SchemaField("_source_file", "STRING", mode="NULLABLE", description="Source file path"),
    SchemaField("_schema_version", "INT64", mode="REQUIRED", description="Schema version"),
]

# ── BigQuery Schema: Checkin ──────────────────────────────────────

CHECKIN_SCHEMA = [
    SchemaField("business_id", "STRING", mode="REQUIRED", description="Business being checked into"),
    SchemaField("date", "STRING", mode="NULLABLE", description="Comma-separated checkin timestamps"),
    # Metadata
    SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED", description="Ingestion timestamp"),
    SchemaField("_source_file", "STRING", mode="NULLABLE", description="Source file path"),
    SchemaField("_schema_version", "INT64", mode="REQUIRED", description="Schema version"),
]

# ── BigQuery Schema: Tip ──────────────────────────────────────────

TIP_SCHEMA = [
    SchemaField("user_id", "STRING", mode="REQUIRED", description="User ID who wrote the tip"),
    SchemaField("business_id", "STRING", mode="REQUIRED", description="Business being tipped"),
    SchemaField("text", "STRING", mode="NULLABLE", description="Tip text (may contain PII)"),
    SchemaField("date", "DATE", mode="NULLABLE", description="Tip date"),
    SchemaField("compliment_count", "INT64", mode="NULLABLE", description="Number of compliments"),
    # Metadata
    SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED", description="Ingestion timestamp"),
    SchemaField("_source_file", "STRING", mode="NULLABLE", description="Source file path"),
    SchemaField("_schema_version", "INT64", mode="REQUIRED", description="Schema version"),
]

# ── Schema Registry ───────────────────────────────────────────────

SCHEMAS = {
    "business": BUSINESS_SCHEMA,
    "review": REVIEW_SCHEMA,
    "user": USER_SCHEMA,
    "checkin": CHECKIN_SCHEMA,
    "tip": TIP_SCHEMA,
}

# ── PII Columns (for Sensitive Data Protection) ───────────────────

PII_COLUMNS = {
    "business": ["name", "address"],
    "review": ["text"],
    "user": ["name", "friends"],
    "checkin": [],
    "tip": ["text"],
}

# ── Partition & Clustering Config ─────────────────────────────────

PARTITIONING_CONFIG = {
    "business": {
        "partition_field": None,  # Small table, no partitioning needed
        "cluster_fields": ["city", "state", "categories"],
    },
    "review": {
        "partition_field": "date",
        "partition_type": "MONTH",
        "cluster_fields": ["business_id", "stars"],
    },
    "user": {
        "partition_field": "yelping_since",
        "partition_type": "MONTH",
        "cluster_fields": ["review_count"],
    },
    "checkin": {
        "partition_field": None,
        "cluster_fields": ["business_id"],
    },
    "tip": {
        "partition_field": "date",
        "partition_type": "DAY",
        "cluster_fields": ["business_id"],
    },
}


def get_schema(entity: str) -> list:
    """Get BigQuery schema for a Yelp entity."""
    if entity not in SCHEMAS:
        raise ValueError(f"Unknown entity: {entity}. Must be one of {list(SCHEMAS.keys())}")
    return SCHEMAS[entity]


def get_pii_columns(entity: str) -> list:
    """Get list of PII-containing columns for a Yelp entity."""
    return PII_COLUMNS.get(entity, [])


def get_partition_config(entity: str) -> dict:
    """Get partitioning and clustering config for a Yelp entity."""
    return PARTITIONING_CONFIG.get(entity, {})
