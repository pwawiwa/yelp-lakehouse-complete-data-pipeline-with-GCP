# ─────────────────────────────────────────────────────────────────────
# Cloud Monitoring: Alert Policies & Notification Channels
#
# NOTE: Since we run Airflow on Astronomer (Astro) locally, not
# Cloud Composer, the composer.googleapis.com metrics are unavailable.
# These alerts use custom log-based metrics instead.
# When migrating to Cloud Composer, switch to the native Composer metrics.
# ─────────────────────────────────────────────────────────────────────

# ─── Notification Channel: Email ──────────────────────────────────

resource "google_monitoring_notification_channel" "email" {
  display_name = "Pipeline Alert Email"
  type         = "email"

  labels = {
    email_address = var.alert_email
  }

  user_labels = {
    team = "data-engineering"
  }
}

# ─── Log-Based Metric: Pipeline Error Count ───────────────────────
# Captures any ERROR-level logs from our pipeline service account

resource "google_logging_metric" "pipeline_errors" {
  name   = "pipeline/error_count"
  filter = <<-EOT
    severity >= ERROR
    AND resource.type = "gcs_bucket"
    OR resource.type = "bigquery_resource"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

# ─── Alert Policy 1: Pipeline Error Alert ─────────────────────────

resource "google_monitoring_alert_policy" "pipeline_errors" {
  display_name = "Data Pipeline Error Alert"
  combiner     = "OR"

  conditions {
    display_name = "Pipeline errors detected"

    condition_threshold {
      filter = <<-EOT
        resource.type = "global"
        AND metric.type = "logging.googleapis.com/user/${google_logging_metric.pipeline_errors.name}"
      EOT

      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.email.id
  ]

  alert_strategy {
    auto_close = "604800s"
  }

  documentation {
    content   = <<-EOT
      ## Data Pipeline Error Alert

      Errors were detected in the data lakehouse pipeline.

      ### Troubleshooting Steps
      1. Check Airflow UI for failed DAG runs
      2. Review task logs for error messages
      3. Check BigQuery job history for failed queries
      4. Verify GCS bucket permissions and connectivity
      5. Check if the Yelp dataset files are accessible

      ### Contact
      Data Engineering Team: wira.hutomo2@gmail.com
    EOT
    mime_type = "text/markdown"
  }

  user_labels = {
    severity = "critical"
    team     = "data-engineering"
  }
}

# ─── Alert Policy 2: BigQuery Slot Usage (Performance) ────────────

resource "google_monitoring_alert_policy" "bq_slot_usage" {
  display_name = "BigQuery High Slot Usage"
  combiner     = "OR"

  conditions {
    display_name = "BigQuery slot usage exceeds threshold"

    condition_threshold {
      filter = <<-EOT
        resource.type = "bigquery_project"
        AND metric.type = "bigquery.googleapis.com/slots/total_available"
      EOT

      comparison      = "COMPARISON_GT"
      threshold_value = 80
      duration        = "300s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.email.id
  ]

  alert_strategy {
    auto_close = "604800s"
  }

  documentation {
    content   = <<-EOT
      ## BigQuery High Slot Usage

      BigQuery slot usage is high, which may cause query slowdowns.

      ### Possible Causes
      1. Large Gold layer aggregation queries running
      2. ML model training consuming excessive resources
      3. Multiple concurrent queries from different users

      ### Actions
      1. Check BigQuery job history for long-running queries
      2. Consider scheduling heavy jobs during off-peak hours
      3. Review query optimization opportunities
      4. Consider increasing slot reservation

      ### Contact
      Data Engineering Team: wira.hutomo2@gmail.com
    EOT
    mime_type = "text/markdown"
  }

  user_labels = {
    severity = "warning"
    team     = "data-engineering"
  }
}

# ─── Alert Policy 3: GCS Operation Errors ─────────────────────────

resource "google_monitoring_alert_policy" "gcs_errors" {
  display_name = "GCS Bucket Operation Errors"
  combiner     = "OR"

  conditions {
    display_name = "GCS request errors detected"

    condition_threshold {
      filter = <<-EOT
        resource.type = "gcs_bucket"
        AND metric.type = "storage.googleapis.com/api/request_count"
        AND metric.labels.response_code != "OK"
        AND metric.labels.response_code != "NoContent"
      EOT

      comparison      = "COMPARISON_GT"
      threshold_value = 10
      duration        = "300s"

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.labels.bucket_name"]
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [
    google_monitoring_notification_channel.email.id
  ]

  alert_strategy {
    auto_close = "604800s"
  }

  documentation {
    content   = <<-EOT
      ## GCS Bucket Operation Errors

      Repeated errors accessing GCS buckets used by the pipeline.

      ### Troubleshooting
      1. Check service account permissions on the bucket
      2. Verify bucket exists and is accessible
      3. Check for quota limits on GCS API requests
      4. Review network connectivity

      ### Contact
      Data Engineering Team: wira.hutomo2@gmail.com
    EOT
    mime_type = "text/markdown"
  }

  user_labels = {
    severity = "warning"
    team     = "data-engineering"
  }
}
