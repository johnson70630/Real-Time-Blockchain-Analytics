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
