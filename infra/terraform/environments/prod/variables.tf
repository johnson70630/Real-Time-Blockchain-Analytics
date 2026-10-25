variable "aws_region" {
  description = "AWS region for production platform resources."
  type        = string
  default     = "us-west-2"
}

variable "project_name" {
  description = "Stable project identifier used in names and tags."
  type        = string
  default     = "blockchain-analytics"
}

variable "environment" {
  description = "Explicit environment represented by this root module."
  type        = string
  default     = "prod"
}

variable "snowflake_storage_external_id" {
  description = "Existing external ID required by the Snowflake storage integration trust policy."
  type        = string
  sensitive   = true
  nullable    = false

  validation {
    condition     = length(trimspace(var.snowflake_storage_external_id)) > 0
    error_message = "The Snowflake storage integration external ID must not be empty."
  }
}
