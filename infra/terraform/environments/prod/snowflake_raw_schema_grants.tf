resource "snowflake_grant_privileges_to_account_role" "raw_schema_usage_de" {
  account_role_name = snowflake_account_role.de.name
  privileges        = ["USAGE"]

  with_grant_option           = false
  always_apply                = false
  strict_privilege_management = false

  on_schema {
    schema_name = snowflake_schema.raw.fully_qualified_name
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_grant_privileges_to_account_role" "raw_schema_usage_pipeline" {
  account_role_name = snowflake_account_role.pipeline.name
  privileges        = ["USAGE"]

  with_grant_option           = false
  always_apply                = false
  strict_privilege_management = false

  on_schema {
    schema_name = snowflake_schema.raw.fully_qualified_name
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_grant_privileges_to_account_role" "raw_schema_usage_dbt" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE"]

  with_grant_option           = false
  always_apply                = false
  strict_privilege_management = false

  on_schema {
    schema_name = snowflake_schema.raw.fully_qualified_name
  }

  lifecycle {
    prevent_destroy = true
  }
}
