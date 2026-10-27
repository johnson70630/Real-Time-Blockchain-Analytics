resource "snowflake_account_role" "admin" {
  name    = "BLOCKCHAIN_ADMIN_ROLE"
  comment = "Owns blockchain analytics platform infrastructure"

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_account_role" "de" {
  name    = "BLOCKCHAIN_DE_ROLE"
  comment = "Human development role for blockchain data engineering"

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_account_role" "pipeline" {
  name    = "BLOCKCHAIN_PIPELINE_ROLE"
  comment = "Service role for automated blockchain data pipelines"

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_account_role" "dbt" {
  name    = "BLOCKCHAIN_DBT_ROLE"
  comment = "Transformation role for dbt models in the blockchain analytics platform"

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_account_role" "analyst" {
  name    = "BLOCKCHAIN_ANALYST_ROLE"
  comment = "Read-only analytics role for curated blockchain datasets"

  lifecycle {
    prevent_destroy = true
  }
}
