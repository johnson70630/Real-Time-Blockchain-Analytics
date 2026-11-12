locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Repository  = "Real-Time-Blockchain-Analytics"
    Workload    = "blockchain-runtime"
    Owner       = var.owner
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}
