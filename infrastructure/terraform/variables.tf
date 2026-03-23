# ─────────────────────────────────────────────────────────────────────
# Terraform Variables
# ─────────────────────────────────────────────────────────────────────

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "asia-southeast1"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "alert_email" {
  description = "Email address for alert notifications"
  type        = string
  default     = "wira.hutomo2@gmail.com"
}

variable "dag_duration_threshold_seconds" {
  description = "Threshold in seconds for DAG duration performance degradation alert"
  type        = number
  default     = 3600  # 1 hour
}
