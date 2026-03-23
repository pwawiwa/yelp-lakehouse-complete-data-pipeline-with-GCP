"""
Schema Evolution Handler.

Detects and applies schema changes between incoming data and existing
BigQuery/Iceberg table schemas. Supports:
- Adding new columns (never drops columns — backward compatible)
- Type widening (INT64 → FLOAT64, STRING expansion)
- Audit logging of all schema changes
"""

import logging
from datetime import datetime

from google.cloud import bigquery

logger = logging.getLogger(__name__)

# ── Type Widening Rules ───────────────────────────────────────────
# Maps (old_type, new_type) → allowed? Widening is always safe.
SAFE_TYPE_WIDENINGS = {
    ("INT64", "FLOAT64"): True,
    ("INT64", "NUMERIC"): True,
    ("FLOAT64", "NUMERIC"): True,
    ("INT64", "STRING"): True,
    ("FLOAT64", "STRING"): True,
    ("BOOL", "STRING"): True,
    ("DATE", "TIMESTAMP"): True,
}


def detect_schema_changes(
    existing_schema: list[bigquery.SchemaField],
    incoming_fields: dict[str, str],
) -> dict:
    """
    Compare an existing BigQuery schema with incoming data fields
    to detect new columns and type mismatches.

    Args:
        existing_schema: Current BigQuery table schema.
        incoming_fields: Dict of {field_name: bq_type} from incoming data.

    Returns:
        Dict with:
            - new_columns: List of (name, type) tuples for new columns
            - type_changes: List of (name, old_type, new_type) for type mismatches
            - is_compatible: Whether evolution can be applied safely
    """
    existing_fields = {field.name: field for field in existing_schema}

    new_columns = []
    type_changes = []

    for field_name, field_type in incoming_fields.items():
        if field_name.startswith("_"):
            # Skip metadata columns
            continue

        if field_name not in existing_fields:
            new_columns.append((field_name, field_type))
            logger.info(f"Schema evolution: New column detected: {field_name} ({field_type})")
        else:
            existing_type = existing_fields[field_name].field_type
            if existing_type != field_type:
                type_changes.append((field_name, existing_type, field_type))
                logger.warning(
                    f"Schema evolution: Type mismatch for {field_name}: "
                    f"{existing_type} → {field_type}"
                )

    # Check if all type changes are safe widenings
    is_compatible = all(
        SAFE_TYPE_WIDENINGS.get((old_t, new_t), False)
        for _, old_t, new_t in type_changes
    )

    return {
        "new_columns": new_columns,
        "type_changes": type_changes,
        "is_compatible": is_compatible and len(type_changes) == 0 or all(
            SAFE_TYPE_WIDENINGS.get((old_t, new_t), False)
            for _, old_t, new_t in type_changes
        ),
    }


def apply_schema_evolution(
    project_id: str,
    dataset_id: str,
    table_id: str,
    changes: dict,
    dry_run: bool = False,
) -> list[str]:
    """
    Apply detected schema changes to a BigQuery table.

    Only adds new columns (never drops). Type widenings are not
    automatically applied — they require manual review.

    Args:
        project_id: GCP project ID.
        dataset_id: BigQuery dataset ID.
        table_id: BigQuery table ID.
        changes: Output from detect_schema_changes().
        dry_run: If True, only generate SQL without executing.

    Returns:
        List of ALTER TABLE statements executed (or to be executed).
    """
    statements = []

    table_ref = f"`{project_id}.{dataset_id}.{table_id}`"

    # Add new columns
    for col_name, col_type in changes.get("new_columns", []):
        stmt = f"ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS `{col_name}` {col_type}"
        statements.append(stmt)

    if not dry_run and statements:
        client = bigquery.Client(project=project_id)
        for stmt in statements:
            logger.info(f"Executing schema evolution: {stmt}")
            client.query(stmt).result()

    # Log schema change event
    _log_schema_change(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        changes=changes,
        statements=statements,
        dry_run=dry_run,
    )

    return statements


def infer_bq_type(value) -> str:
    """
    Infer BigQuery type from a Python value.

    Args:
        value: A Python value from parsed JSON.

    Returns:
        BigQuery type string.
    """
    if value is None:
        return "STRING"  # Default to STRING for null values
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, dict):
        return "JSON"
    if isinstance(value, list):
        return "STRING"  # Store as JSON string
    return "STRING"


def infer_schema_from_records(
    records: list[dict],
    sample_size: int = 1000,
) -> dict[str, str]:
    """
    Infer BigQuery schema from a sample of JSON records.

    Scans multiple records to handle fields that may be missing
    in some records but present in others.

    Args:
        records: List of parsed JSON records.
        sample_size: Number of records to sample.

    Returns:
        Dict of {field_name: bq_type}.
    """
    field_types: dict[str, set] = {}

    for record in records[:sample_size]:
        for key, value in record.items():
            if key not in field_types:
                field_types[key] = set()
            field_types[key].add(infer_bq_type(value))

    # Resolve type conflicts (use widest type)
    resolved = {}
    for field, types in field_types.items():
        types.discard("STRING")  # STRING is the fallback
        if not types:
            resolved[field] = "STRING"
        elif len(types) == 1:
            resolved[field] = types.pop()
        else:
            # Multiple types detected — use STRING as safe fallback
            resolved[field] = "STRING"
            logger.warning(
                f"Mixed types for field '{field}': {types}. Using STRING."
            )

    return resolved


def _log_schema_change(
    project_id: str,
    dataset_id: str,
    table_id: str,
    changes: dict,
    statements: list[str],
    dry_run: bool,
) -> None:
    """Log schema change event for auditing."""
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "table": f"{project_id}.{dataset_id}.{table_id}",
        "new_columns": changes.get("new_columns", []),
        "type_changes": changes.get("type_changes", []),
        "is_compatible": changes.get("is_compatible", True),
        "statements_executed": statements,
        "dry_run": dry_run,
    }

    logger.info(f"SCHEMA_EVOLUTION_EVENT|{event}")
