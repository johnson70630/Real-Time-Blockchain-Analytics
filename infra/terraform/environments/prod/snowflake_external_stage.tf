resource "snowflake_stage_external_s3" "silver" {
  name     = "SILVER_S3_STAGE"
  database = snowflake_database.blockchain.name
  schema   = snowflake_schema.raw.name
  url      = "s3://johnson-blockchain-analytics-prod/blockchain-analytics/production/"

  storage_integration = snowflake_storage_integration_aws.blockchain_s3.name

  directory {
    enable = false
  }

  lifecycle {
    prevent_destroy = true
  }
}
