"""
JSON Handling Utilities for Yelp Dataset.

Handles JSON flattening, nested field extraction, malformed JSON recovery,
and type coercion for mixed-type JSON fields common in the Yelp dataset.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def flatten_json(
    record: dict,
    parent_key: str = "",
    separator: str = "_",
    max_depth: int = 3,
) -> dict:
    """
    Flatten a nested JSON object into a single-level dict.

    Args:
        record: The nested JSON dictionary to flatten.
        parent_key: Prefix for nested keys (used in recursion).
        separator: Separator character for concatenated keys.
        max_depth: Maximum depth to flatten. Beyond this, store as JSON string.

    Returns:
        A flat dictionary with concatenated keys.

    Example:
        >>> flatten_json({"a": {"b": 1, "c": {"d": 2}}})
        {"a_b": 1, "a_c_d": 2}
    """
    items = {}
    for key, value in record.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key

        if isinstance(value, dict) and max_depth > 0:
            items.update(
                flatten_json(value, new_key, separator, max_depth - 1)
            )
        elif isinstance(value, list):
            # Store lists as JSON strings for BigQuery STRING columns
            items[new_key] = json.dumps(value) if value else None
        else:
            items[new_key] = value

    return items


def safe_parse_json(
    line: str, source_file: str = "", line_number: int = 0
) -> tuple[dict | None, str | None]:
    """
    Safely parse a JSON line, returning the parsed dict or error info.

    Args:
        line: Raw JSON string to parse.
        source_file: Source file path for error context.
        line_number: Line number for error context.

    Returns:
        Tuple of (parsed_dict, error_message).
        If parsing succeeds, error_message is None.
        If parsing fails, parsed_dict is None.
    """
    try:
        line = line.strip()
        if not line:
            return None, "Empty line"

        record = json.loads(line)
        if not isinstance(record, dict):
            return None, f"Expected JSON object, got {type(record).__name__}"

        return record, None

    except json.JSONDecodeError as e:
        error_msg = (
            f"JSON parse error at {source_file}:{line_number}: {str(e)}"
        )
        logger.warning(error_msg)
        return None, error_msg


def extract_nested_json_field(
    record: dict, field_name: str
) -> str | None:
    """
    Extract a nested field from a record and return it as a JSON string.

    Used for Yelp's `attributes` and `hours` fields which are nested dicts
    that we store as JSON strings in BigQuery for later parsing.

    Args:
        record: The source record.
        field_name: The field name to extract.

    Returns:
        JSON string representation, or None if field is missing/None.
    """
    value = record.get(field_name)
    if value is None:
        return None
    if isinstance(value, str):
        # Already a string (some Yelp records have stringified JSON)
        return value
    return json.dumps(value)


def coerce_types(record: dict, entity: str) -> dict:
    """
    Coerce mixed-type fields to their expected types.

    The Yelp dataset has inconsistencies (e.g., stars as int vs float,
    dates as strings). This normalizes them.

    Args:
        record: The raw record dict.
        entity: Entity type (business, review, user, checkin, tip).

    Returns:
        Record with coerced types.
    """
    coerced = record.copy()

    # Common: ensure metadata fields
    coerced.setdefault("_ingested_at", datetime.now(timezone.utc).isoformat())
    coerced.setdefault("_schema_version", 1)

    if entity == "business":
        coerced["stars"] = _safe_float(coerced.get("stars"))
        coerced["latitude"] = _safe_float(coerced.get("latitude"))
        coerced["longitude"] = _safe_float(coerced.get("longitude"))
        coerced["review_count"] = _safe_int(coerced.get("review_count"))
        coerced["is_open"] = _safe_int(coerced.get("is_open"))
        # Flatten nested JSON fields to strings
        coerced["attributes_json"] = extract_nested_json_field(
            record, "attributes"
        )
        coerced["hours_json"] = extract_nested_json_field(record, "hours")
        # Remove original nested fields
        coerced.pop("attributes", None)
        coerced.pop("hours", None)

    elif entity == "review":
        coerced["stars"] = _safe_int(coerced.get("stars"))
        coerced["useful"] = _safe_int(coerced.get("useful"))
        coerced["funny"] = _safe_int(coerced.get("funny"))
        coerced["cool"] = _safe_int(coerced.get("cool"))
        coerced["date"] = _safe_date(coerced.get("date"))

    elif entity == "user":
        coerced["review_count"] = _safe_int(coerced.get("review_count"))
        coerced["useful"] = _safe_int(coerced.get("useful"))
        coerced["funny"] = _safe_int(coerced.get("funny"))
        coerced["cool"] = _safe_int(coerced.get("cool"))
        coerced["fans"] = _safe_int(coerced.get("fans"))
        coerced["average_stars"] = _safe_float(coerced.get("average_stars"))
        coerced["yelping_since"] = _safe_date(coerced.get("yelping_since"))
        # Compliment fields
        for key in [k for k in coerced if k.startswith("compliment_")]:
            coerced[key] = _safe_int(coerced.get(key))

    elif entity == "tip":
        coerced["date"] = _safe_date(coerced.get("date"))
        coerced["compliment_count"] = _safe_int(
            coerced.get("compliment_count")
        )

    return coerced


def _safe_float(value: Any) -> float | None:
    """Convert value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> int | None:
    """Convert value to int, returning None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_date(value: Any) -> str | None:
    """
    Normalize a date value to YYYY-MM-DD format.
    Yelp dates come as 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'.
    """
    if value is None:
        return None
    try:
        date_str = str(value).strip()
        # Handle 'YYYY-MM-DD HH:MM:SS' format
        if " " in date_str:
            date_str = date_str.split(" ")[0]
        # Validate format
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except (ValueError, TypeError):
        return None
