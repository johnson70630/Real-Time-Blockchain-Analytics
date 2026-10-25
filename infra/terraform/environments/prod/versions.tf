terraform {
  required_version = ">= 1.10.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.69.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.22.0"
    }
  }
}
