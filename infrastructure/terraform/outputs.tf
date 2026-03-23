# ─────────────────────────────────────────────────────────────────────
# Terraform Outputs
# ─────────────────────────────────────────────────────────────────────

output "bronze_bucket" {
  value = google_storage_bucket.bronze.name
}

output "staging_bucket" {
  value = google_storage_bucket.staging.name
}

output "dataflow_temp_bucket" {
  value = google_storage_bucket.dataflow_temp.name
}

output "dlq_bucket" {
  value = google_storage_bucket.dlq.name
}

output "biglake_connection_id" {
  value = google_bigquery_connection.biglake.connection_id
}

output "biglake_service_account" {
  value = google_bigquery_connection.biglake.cloud_resource[0].service_account_id
}

output "pipeline_service_account_email" {
  value = google_service_account.pipeline_sa.email
}

output "kms_key_name" {
  value = google_kms_crypto_key.dlp_key.id
}
