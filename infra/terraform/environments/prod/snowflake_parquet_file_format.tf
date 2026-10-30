resource "snowflake_file_format_parquet" "raw" {
  name     = "PARQUET_FORMAT"
  database = snowflake_database.blockchain.name
  schema   = snowflake_schema.raw.name

  compression = "AUTO"
  null_if     = []

  lifecycle {
    prevent_destroy = true
  }
}
