"""
GCS Helper Utilities.

Wrappers around google-cloud-storage for uploading, downloading,
and managing Yelp dataset files in GCS with partitioned paths.
"""

import json
import logging
from datetime import datetime

from google.cloud import storage

logger = logging.getLogger(__name__)


def get_gcs_client() -> storage.Client:
    """Get or create a GCS client."""
    return storage.Client()


def upload_file_to_gcs(
    local_path: str,
    bucket_name: str,
    gcs_path: str,
    content_type: str = "application/json",
) -> str:
    """
    Upload a local file to GCS.

    Args:
        local_path: Path to the local file.
        bucket_name: GCS bucket name.
        gcs_path: Destination path in the bucket.
        content_type: MIME type of the file.

    Returns:
        GCS URI (gs://bucket/path).
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(local_path, content_type=content_type)

    gcs_uri = f"gs://{bucket_name}/{gcs_path}"
    logger.info(f"Uploaded {local_path} → {gcs_uri}")
    return gcs_uri


def upload_json_lines_to_gcs(
    records: list[dict],
    bucket_name: str,
    gcs_path: str,
) -> str:
    """
    Upload a list of records as NDJSON (newline-delimited JSON) to GCS.

    Args:
        records: List of dictionaries to serialize.
        bucket_name: GCS bucket name.
        gcs_path: Destination path in the bucket.

    Returns:
        GCS URI (gs://bucket/path).
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    ndjson = "\n".join(json.dumps(r) for r in records)
    blob.upload_from_string(ndjson, content_type="application/json")

    gcs_uri = f"gs://{bucket_name}/{gcs_path}"
    logger.info(f"Uploaded {len(records)} records → {gcs_uri}")
    return gcs_uri


def generate_partitioned_path(
    entity: str,
    date: datetime | None = None,
    prefix: str = "yelp",
    file_format: str = "json",
) -> str:
    """
    Generate a partitioned GCS path for Hive-style partitioning.

    Args:
        entity: Yelp entity type (business, review, user, etc.).
        date: Partition date. Defaults to today.
        prefix: Top-level prefix in the bucket.
        file_format: File extension.

    Returns:
        Partitioned path like: yelp/entity=review/date=2024-01-15/data.json
    """
    if date is None:
        date = datetime.utcnow()

    date_str = date.strftime("%Y-%m-%d")
    timestamp = date.strftime("%Y%m%d_%H%M%S")

    return (
        f"{prefix}/"
        f"entity={entity}/"
        f"date={date_str}/"
        f"data_{timestamp}.{file_format}"
    )


def list_gcs_files(
    bucket_name: str,
    prefix: str,
    delimiter: str | None = None,
) -> list[str]:
    """
    List files in a GCS bucket under a given prefix.

    Args:
        bucket_name: GCS bucket name.
        prefix: Path prefix to filter by.
        delimiter: Optional delimiter for pseudo-directory listing.

    Returns:
        List of blob names matching the prefix.
    """
    client = get_gcs_client()
    blobs = client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)
    return [blob.name for blob in blobs]


def download_gcs_to_string(bucket_name: str, gcs_path: str) -> str:
    """
    Download a GCS file to a string.

    Args:
        bucket_name: GCS bucket name.
        gcs_path: Path within the bucket.

    Returns:
        File contents as string.
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return blob.download_as_text()


def check_gcs_file_exists(bucket_name: str, gcs_path: str) -> bool:
    """Check if a file exists in GCS."""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return blob.exists()
