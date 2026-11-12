variable "aws_region" {
  description = "AWS Region for the runtime foundation."
  type        = string
  default     = "us-west-2"

  validation {
    condition     = var.aws_region == "us-west-2"
    error_message = "The approved runtime Region is us-west-2."
  }
}

variable "availability_zone" {
  description = "Single availability zone used by the portfolio runtime."
  type        = string
  default     = "us-west-2a"

  validation {
    condition     = startswith(var.availability_zone, "us-west-2")
    error_message = "The availability zone must be in us-west-2."
  }
}

variable "project_name" {
  description = "Project tag value."
  type        = string
  default     = "real-time-blockchain-analytics"
}

variable "environment" {
  description = "Environment tag value."
  type        = string
  default     = "production-runtime"
}

variable "owner" {
  description = "Non-sensitive owner tag used for cost allocation."
  type        = string
  default     = "blockchain-data-engineering"
}

variable "vpc_cidr" {
  description = "CIDR for the isolated runtime VPC."
  type        = string
  default     = "10.46.0.0/24"
}

variable "public_subnet_cidr" {
  description = "CIDR for the single public runtime subnet."
  type        = string
  default     = "10.46.0.0/26"
}

variable "instance_type" {
  description = "Provisional instance size pending Kafka and Spark benchmarks."
  type        = string
  default     = "t3.xlarge"

  validation {
    condition     = contains(["t3.xlarge", "t3.2xlarge", "m6i.xlarge", "m6i.2xlarge"], var.instance_type)
    error_message = "Use an approved x86_64 runtime instance type."
  }
}

variable "root_volume_size_gib" {
  description = "Encrypted gp3 root volume size."
  type        = number
  default     = 30

  validation {
    condition     = var.root_volume_size_gib >= 20 && var.root_volume_size_gib <= 100
    error_message = "Root volume size must be between 20 and 100 GiB."
  }
}

variable "kafka_volume_size_gib" {
  description = "Encrypted persistent gp3 volume size for Kafka logs."
  type        = number
  default     = 100

  validation {
    condition     = var.kafka_volume_size_gib >= 100 && var.kafka_volume_size_gib <= 500
    error_message = "Kafka volume size must be between 100 and 500 GiB."
  }
}

variable "data_bucket_name" {
  description = "Existing analytical-data bucket; this root does not own it."
  type        = string
  default     = "johnson-blockchain-analytics-prod"

  validation {
    condition     = var.data_bucket_name == "johnson-blockchain-analytics-prod"
    error_message = "The runtime may reference only the approved analytical-data bucket."
  }
}

variable "terraform_state_bucket_name" {
  description = "Existing state bucket used only to construct an explicit deny."
  type        = string
  default     = "blockchain-analytics-terraform-state-209479274979-usw2"

  validation {
    condition     = var.terraform_state_bucket_name == "blockchain-analytics-terraform-state-209479274979-usw2"
    error_message = "The runtime deny must cover the approved Terraform state bucket."
  }
}

variable "pilot_s3_prefix" {
  description = "Isolated prefix for the first runtime pilot and its checkpoints."
  type        = string
  default     = "blockchain-analytics/pilot/runtime"

  validation {
    condition = (
      startswith(var.pilot_s3_prefix, "blockchain-analytics/pilot/") &&
      !endswith(var.pilot_s3_prefix, "/")
    )
    error_message = "The pilot prefix must remain below blockchain-analytics/pilot and omit a trailing slash."
  }
}

variable "approved_secret_arns" {
  description = "Exact approved Secrets Manager ARNs; empty disables secret access for infrastructure-only planning."
  type        = list(string)
  sensitive   = true
  default     = []

  validation {
    condition = alltrue([
      for arn in var.approved_secret_arns :
      can(regex("^arn:aws:secretsmanager:us-west-2:209479274979:secret:[A-Za-z0-9/_+=.@-]+-[A-Za-z0-9]{6}$", arn))
    ])
    error_message = "Use an empty list for no secret access, or exact secret ARNs in the approved account and Region; wildcards are forbidden."
  }
}

variable "docker_compose_download_url" {
  description = "Pinned HTTPS URL for the x86_64 Docker Compose plugin binary."
  type        = string

  validation {
    condition     = startswith(var.docker_compose_download_url, "https://github.com/docker/compose/releases/download/")
    error_message = "Docker Compose must come from a pinned official GitHub release URL."
  }
}

variable "docker_compose_sha256" {
  description = "Operator-reviewed SHA-256 for the pinned Docker Compose binary."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{64}$", var.docker_compose_sha256))
    error_message = "Docker Compose SHA-256 must be 64 lowercase hexadecimal characters."
  }
}

variable "log_retention_days" {
  description = "CloudWatch log retention for the bounded runtime."
  type        = number
  default     = 14

  validation {
    condition     = contains([7, 14, 30], var.log_retention_days)
    error_message = "Log retention must be 7, 14, or 30 days."
  }
}

variable "enable_scheduled_shutdown" {
  description = "Opt-in creation of the stop-only EventBridge Scheduler control."
  type        = bool
  default     = false
}

variable "shutdown_schedule_expression" {
  description = "UTC stop schedule used only when scheduled shutdown is explicitly enabled."
  type        = string
  default     = "cron(0 6 * * ? *)"
}
