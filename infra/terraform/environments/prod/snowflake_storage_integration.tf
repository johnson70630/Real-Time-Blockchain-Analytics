resource "snowflake_storage_integration_aws" "blockchain_s3" {
  name    = "BLOCKCHAIN_ANALYTICS_S3_INT"
  enabled = true

  storage_provider          = "S3"
  storage_allowed_locations = ["s3://johnson-blockchain-analytics-prod/blockchain-analytics/production/"]
  storage_blocked_locations = []
  storage_aws_role_arn      = aws_iam_role.snowflake_s3.arn

  lifecycle {
    prevent_destroy = true
  }
}
