variable "aws_region" {
  description = "AWS region that will contain the dedicated Terraform state bucket."
  type        = string
  default     = "us-west-2"
}

variable "project_name" {
  description = "Stable project identifier used in names and tags."
  type        = string
  default     = "blockchain-analytics"
}

variable "environment" {
  description = "Deployment environment represented by this state backend."
  type        = string
  default     = "prod"
}

variable "state_bucket_suffix" {
  description = "Globally unique, non-sensitive suffix for the state bucket name."
  type        = string

  validation {
    condition = (
      length(var.state_bucket_suffix) >= 6 &&
      length(var.state_bucket_suffix) <= 24 &&
      can(regex("^[a-z0-9][a-z0-9-]*[a-z0-9]$", var.state_bucket_suffix))
    )
    error_message = "state_bucket_suffix must be 6-24 lowercase letters, digits, or hyphens."
  }
}
