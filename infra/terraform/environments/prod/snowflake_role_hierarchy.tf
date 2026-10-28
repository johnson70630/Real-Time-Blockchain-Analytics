resource "snowflake_grant_account_role" "admin_to_sysadmin" {
  role_name        = snowflake_account_role.admin.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "de_to_admin" {
  role_name        = snowflake_account_role.de.name
  parent_role_name = snowflake_account_role.admin.name
}

resource "snowflake_grant_account_role" "pipeline_to_admin" {
  role_name        = snowflake_account_role.pipeline.name
  parent_role_name = snowflake_account_role.admin.name
}

resource "snowflake_grant_account_role" "dbt_to_admin" {
  role_name        = snowflake_account_role.dbt.name
  parent_role_name = snowflake_account_role.admin.name
}

resource "snowflake_grant_account_role" "analyst_to_admin" {
  role_name        = snowflake_account_role.analyst.name
  parent_role_name = snowflake_account_role.admin.name
}
