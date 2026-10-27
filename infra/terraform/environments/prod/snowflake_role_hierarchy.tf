resource "snowflake_grant_account_role" "admin_to_sysadmin" {
  role_name        = snowflake_account_role.admin.fully_qualified_name
  parent_role_name = "\"SYSADMIN\""
}

resource "snowflake_grant_account_role" "de_to_admin" {
  role_name        = snowflake_account_role.de.fully_qualified_name
  parent_role_name = snowflake_account_role.admin.fully_qualified_name
}

resource "snowflake_grant_account_role" "pipeline_to_admin" {
  role_name        = snowflake_account_role.pipeline.fully_qualified_name
  parent_role_name = snowflake_account_role.admin.fully_qualified_name
}

resource "snowflake_grant_account_role" "dbt_to_admin" {
  role_name        = snowflake_account_role.dbt.fully_qualified_name
  parent_role_name = snowflake_account_role.admin.fully_qualified_name
}

resource "snowflake_grant_account_role" "analyst_to_admin" {
  role_name        = snowflake_account_role.analyst.fully_qualified_name
  parent_role_name = snowflake_account_role.admin.fully_qualified_name
}
