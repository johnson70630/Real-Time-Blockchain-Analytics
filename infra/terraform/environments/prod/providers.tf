locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Repository  = "Real-Time-Blockchain-Analytics"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

# Existing untagged resources use this alias so adoption does not add tags.
provider "aws" {
  alias  = "adopted"
  region = var.aws_region
}

provider "snowflake" {}
