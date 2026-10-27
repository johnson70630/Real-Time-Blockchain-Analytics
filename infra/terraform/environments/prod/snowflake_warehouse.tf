resource "snowflake_warehouse" "transform" {
  name = "BLOCKCHAIN_TRANSFORM_WH"

  warehouse_type = "STANDARD"
  warehouse_size = "XSMALL"
  generation     = "2"
  auto_suspend   = 60
  auto_resume    = "true"
}
