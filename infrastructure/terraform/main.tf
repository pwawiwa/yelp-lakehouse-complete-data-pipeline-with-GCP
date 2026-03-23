# ─────────────────────────────────────────────────────────────────────
# Terraform: GCP Infrastructure for Data Lakehouse ELT Pipeline
# ─────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# ─── Enable Required APIs ──────────────────────────────────────────

resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "bigquerystorage.googleapis.com",
    "biglake.googleapis.com",
    "storage.googleapis.com",
    "dataflow.googleapis.com",
    "dlp.googleapis.com",
    "monitoring.googleapis.com",
    "cloudkms.googleapis.com",
    "iam.googleapis.com",
  ])

  service            = each.key
  disable_on_destroy = false
}

# ─── Service Account ──────────────────────────────────────────────

resource "google_service_account" "pipeline_sa" {
  account_id   = "data-lakehouse-pipeline"
  display_name = "Data Lakehouse ELT Pipeline Service Account"
  description  = "Service account for the data lakehouse ELT pipeline"
}

resource "google_project_iam_member" "pipeline_sa_roles" {
  for_each = toset([
    "roles/bigquery.admin",
    "roles/storage.admin",
    "roles/dataflow.admin",
    "roles/dlp.admin",
    "roles/monitoring.editor",
    "roles/cloudkms.cryptoKeyEncrypterDecrypter",
    "roles/biglake.admin",
    "roles/iam.serviceAccountUser",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ─── GCS Buckets ──────────────────────────────────────────────────

resource "google_storage_bucket" "bronze" {
  name          = "${var.project_id}-bronze"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

resource "google_storage_bucket" "staging" {
  name          = "${var.project_id}-staging"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "dataflow_temp" {
  name          = "${var.project_id}-dataflow-temp"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "dlq" {
  name          = "${var.project_id}-dlq"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# ─── BigQuery Datasets ────────────────────────────────────────────

resource "google_bigquery_dataset" "bronze" {
  dataset_id                 = "bronze"
  friendly_name              = "Bronze Layer"
  description                = "Raw data from Yelp dataset — Iceberg external tables"
  location                   = var.region
  delete_contents_on_destroy = true

  labels = {
    layer = "bronze"
    env   = var.environment
  }
}

resource "google_bigquery_dataset" "silver" {
  dataset_id                 = "silver"
  friendly_name              = "Silver Layer"
  description                = "Cleansed, PII-masked, partitioned data"
  location                   = var.region
  delete_contents_on_destroy = true

  labels = {
    layer = "silver"
    env   = var.environment
  }
}

resource "google_bigquery_dataset" "gold" {
  dataset_id                 = "gold"
  friendly_name              = "Gold Layer"
  description                = "Aggregated business analytics and ML feature tables"
  location                   = var.region
  delete_contents_on_destroy = true

  labels = {
    layer = "gold"
    env   = var.environment
  }
}

resource "google_bigquery_dataset" "ml_models" {
  dataset_id                 = "ml_models"
  friendly_name              = "ML Models"
  description                = "BigQuery ML models and prediction results"
  location                   = var.region
  delete_contents_on_destroy = true

  labels = {
    layer = "ml"
    env   = var.environment
  }
}

resource "google_bigquery_dataset" "dlq" {
  dataset_id                 = "dlq"
  friendly_name              = "Dead Letter Queue"
  description                = "Failed records from validation pipeline"
  location                   = var.region
  delete_contents_on_destroy = true

  labels = {
    layer = "dlq"
    env   = var.environment
  }
}

# ─── BigLake Connection ───────────────────────────────────────────

resource "google_bigquery_connection" "biglake" {
  provider      = google-beta
  connection_id = "biglake-connection"
  location      = var.region
  friendly_name = "BigLake Connection for Iceberg Tables"
  description   = "Connects BigQuery to GCS Iceberg data via BigLake"

  cloud_resource {}
}

# Grant BigLake connection SA access to GCS
# Use a time_sleep to ensure the service account is fully propagated
resource "time_sleep" "wait_for_biglake_sa" {
  depends_on      = [google_bigquery_connection.biglake]
  create_duration = "30s"
}

resource "google_storage_bucket_iam_member" "biglake_bronze" {
  bucket     = google_storage_bucket.bronze.name
  role       = "roles/storage.objectAdmin"
  member     = "serviceAccount:${google_bigquery_connection.biglake.cloud_resource[0].service_account_id}"
  depends_on = [time_sleep.wait_for_biglake_sa]
}

# ─── Cloud KMS (for DLP Deterministic Encryption) ─────────────────

resource "google_kms_key_ring" "dlp_keyring" {
  name       = "dlp-keyring"
  location   = var.region
  depends_on = [google_project_service.apis]
}

resource "google_kms_crypto_key" "dlp_key" {
  name     = "dlp-pii-encryption-key"
  key_ring = google_kms_key_ring.dlp_keyring.id

  rotation_period = "7776000s"  # 90 days

  lifecycle {
    prevent_destroy = false
  }
}
