resource "snowflake_grant_privileges_to_account_role" "marts_schema_dbt_privileges" {
  account_role_name = snowflake_account_role.dbt.name

  privileges = [
    "CREATE TABLE",
    "CREATE VIEW",
    "USAGE",
  ]

  with_grant_option           = false
  always_apply                = false
  strict_privilege_management = false

  on_schema {
    schema_name = snowflake_schema.marts.fully_qualified_name
  }

  lifecycle {
    prevent_destroy = true
  }
}
