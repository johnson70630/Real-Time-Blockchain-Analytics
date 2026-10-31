resource "snowflake_grant_privileges_to_account_role" "warehouse_usage_de" {
  account_role_name = snowflake_account_role.de.name
  privileges        = ["USAGE"]

  with_grant_option           = false
  always_apply                = false
  strict_privilege_management = false

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage_pipeline" {
  account_role_name = snowflake_account_role.pipeline.name
  privileges        = ["USAGE"]

  with_grant_option           = false
  always_apply                = false
  strict_privilege_management = false

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage_dbt" {
  account_role_name = snowflake_account_role.dbt.name
  privileges        = ["USAGE"]

  with_grant_option           = false
  always_apply                = false
  strict_privilege_management = false

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage_analyst" {
  account_role_name = snowflake_account_role.analyst.name
  privileges        = ["USAGE"]

  with_grant_option           = false
  always_apply                = false
  strict_privilege_management = false

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }

  lifecycle {
    prevent_destroy = true
  }
}
