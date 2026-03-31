"""
PII Detection and Masking via GCP Sensitive Data Protection (DLP).

Provides functions to:
- Inspect BigQuery tables for PII using 150+ InfoTypes
- De-identify PII columns using tokenization/masking
- Create column-level policy tags for data masking
- Use DLP_DETERMINISTIC_ENCRYPT for reversible tokenization
"""

import logging
from typing import Optional

from google.cloud import dlp_v2
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# ── InfoTypes for PII Detection ───────────────────────────────────
# These cover common PII types found in Yelp-like user-generated data
DEFAULT_INFO_TYPES = [
    "PERSON_NAME",
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "STREET_ADDRESS",
    "CREDIT_CARD_NUMBER",
    "US_SOCIAL_SECURITY_NUMBER",
    "DATE_OF_BIRTH",
    "IP_ADDRESS",
    "URL",
]


def get_dlp_client() -> dlp_v2.DlpServiceClient:
    """Get or create a DLP client."""
    return dlp_v2.DlpServiceClient()


def inspect_bigquery_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    info_types: list[str] | None = None,
    max_findings: int = 1000,
    sample_row_limit: int = 10000,
) -> dict:
    """
    Inspect a BigQuery table for PII using Sensitive Data Protection.

    Args:
        project_id: GCP project ID.
        dataset_id: BigQuery dataset ID.
        table_id: BigQuery table ID.
        info_types: List of InfoType names to scan for. Defaults to common PII.
        max_findings: Maximum number of findings to return.
        sample_row_limit: Number of rows to sample for inspection.

    Returns:
        Dict with inspection results summary.
    """
    client = get_dlp_client()

    if info_types is None:
        info_types = DEFAULT_INFO_TYPES

    inspect_config = {
        "info_types": [{"name": it} for it in info_types],
        "min_likelihood": dlp_v2.Likelihood.LIKELY,
        "limits": {"max_findings_per_request": max_findings},
        "include_quote": False,
    }

    storage_config = {
        "big_query_options": {
            "table_reference": {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "table_id": table_id,
            },
            "rows_limit": sample_row_limit,
            "sample_method": dlp_v2.BigQueryOptions.SampleMethod.RANDOM_START,
        }
    }

    inspect_job = {
        "inspect_config": inspect_config,
        "storage_config": storage_config,
    }

    # Use specific location matching the BQ dataset for job reliability
    parent = f"projects/{project_id}/locations/asia-southeast1"

    response = client.create_dlp_job(
        request={
            "parent": parent,
            "inspect_job": inspect_job,
        }
    )

    logger.info(f"DLP inspection job created: {response.name}")
    return {"job_name": response.name, "state": response.state.name}


def create_deidentify_template(
    project_id: str,
    template_id: str,
    columns_to_mask: list[str],
    masking_character: str = "*",
    kms_key_name: Optional[str] = None,
) -> str:
    """
    Create a DLP de-identification template.

    Supports two modes:
    - Character masking (default): Replaces characters with masking_character
    - Deterministic encryption: Uses KMS key for reversible tokenization

    Args:
        project_id: GCP project ID.
        template_id: Unique template identifier.
        columns_to_mask: List of column names to de-identify.
        masking_character: Character to use for masking.
        kms_key_name: KMS key for deterministic encryption. If None, uses masking.

    Returns:
        Template resource name.
    """
    client = get_dlp_client()
    # Use specific location matching the BQ dataset
    parent = f"projects/{project_id}/locations/asia-southeast1"

    if kms_key_name:
        # Deterministic encryption (reversible)
        primitive_transformation = {
            "crypto_deterministic_config": {
                "crypto_key": {
                    "kms_wrapped": {
                        "crypto_key_name": kms_key_name,
                        "wrapped_key": b"",  # Will be auto-generated
                    }
                },
                "surrogate_info_type": {"name": "YELP_PII_TOKEN"},
            }
        }
    else:
        # Character masking (irreversible)
        primitive_transformation = {
            "character_mask_config": {
                "masking_character": masking_character,
                "number_to_mask": 0,  # 0 = mask all characters
            }
        }

    # Build field transformations for each column
    field_transformations = []
    for column in columns_to_mask:
        field_transformations.append(
            {
                "fields": [{"name": column}],
                "primitive_transformation": primitive_transformation,
            }
        )

    deidentify_config = {
        "record_transformations": {
            "field_transformations": field_transformations,
        }
    }

    template = {
        "deidentify_config": deidentify_config,
        "display_name": f"Yelp PII De-identification - {template_id}",
        "description": f"De-identifies PII columns: {', '.join(columns_to_mask)}",
    }

    response = client.create_deidentify_template(
        request={
            "parent": parent,
            "deidentify_template": template,
            "template_id": template_id,
        }
    )

    logger.info(f"Created de-identify template: {response.name}")
    return response.name


def deidentify_bigquery_table(
    project_id: str,
    source_dataset: str,
    source_table: str,
    dest_dataset: str,
    dest_table: str,
    deidentify_template_name: str,
    info_types: list[str] | None = None,
) -> dict:
    """
    De-identify a BigQuery table and write results to a new table.

    Args:
        project_id: GCP project ID.
        source_dataset: Source BigQuery dataset.
        source_table: Source BigQuery table.
        dest_dataset: Destination BigQuery dataset.
        dest_table: Destination BigQuery table.
        deidentify_template_name: Full resource name of the de-identify template.
        info_types: List of InfoType names to detect. Defaults to common PII.

    Returns:
        Dict with job details.
    """
    client = get_dlp_client()
    # Use specific location matching the BQ dataset
    parent = f"projects/{project_id}/locations/asia-southeast1"

    if info_types is None:
        info_types = DEFAULT_INFO_TYPES

    inspect_config = {
        "info_types": [{"name": it} for it in info_types],
        "min_likelihood": dlp_v2.Likelihood.LIKELY,
    }

    storage_config = {
        "big_query_options": {
            "table_reference": {
                "project_id": project_id,
                "dataset_id": source_dataset,
                "table_id": source_table,
            }
        }
    }

    actions = [
        {
            "deidentify": {
                "cloud_storage_output": None,
                "transformation_details_storage_config": None,
            },
            "save_findings": {
                "output_config": {
                    "table": {
                        "project_id": project_id,
                        "dataset_id": dest_dataset,
                        "table_id": dest_table,
                    }
                }
            },
        }
    ]

    inspect_job = {
        "inspect_config": inspect_config,
        "storage_config": storage_config,
        "actions": actions,
    }

    response = client.create_dlp_job(
        request={
            "parent": parent,
            "inspect_job": inspect_job,
        }
    )

    logger.info(f"DLP de-identify job created: {response.name}")
    return {"job_name": response.name, "state": response.state.name}


def apply_column_level_security(
    project_id: str,
    dataset_id: str,
    table_id: str,
    pii_columns: list[str],
    taxonomy_id: str = "yelp_pii_taxonomy",
    policy_tag: str = "pii_sensitive",
) -> None:
    """
    Apply column-level security using BigQuery policy tags.

    This creates policy tags that control access to PII columns.
    Users with 'Fine-Grained Reader' role see actual values;
    others see masked data based on the data masking rule.

    Args:
        project_id: GCP project ID.
        dataset_id: BigQuery dataset ID.
        table_id: BigQuery table ID.
        pii_columns: List of column names to protect.
        taxonomy_id: Policy tag taxonomy identifier.
        policy_tag: Policy tag name.
    """
    bq_client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = bq_client.get_table(table_ref)

    # Update schema to add policy tags to PII columns
    new_schema = []
    for field in table.schema:
        if field.name in pii_columns:
            # Add policy tag reference
            policy_tag_ref = (
                f"projects/{project_id}/locations/asia-southeast1/"
                f"taxonomies/{taxonomy_id}/policyTags/{policy_tag}"
            )
            new_field = field._properties.copy()
            new_field["policyTags"] = {"names": [policy_tag_ref]}
            new_schema.append(bigquery.SchemaField.from_api_repr(new_field))
        else:
            new_schema.append(field)

    table.schema = new_schema
    bq_client.update_table(table, ["schema"])

    logger.info(
        f"Applied column-level security to {table_ref}: "
        f"columns={pii_columns}"
    )
