resource "snowflake_database" "blockchain" {
  name         = "BLOCKCHAIN_ANALYTICS"
  is_transient = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.blockchain.name
  name     = "RAW"

  is_transient        = "false"
  with_managed_access = "false"

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.blockchain.name
  name     = "STAGING"

  is_transient        = "false"
  with_managed_access = "false"

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_schema" "core" {
  database = snowflake_database.blockchain.name
  name     = "CORE"

  is_transient        = "false"
  with_managed_access = "false"

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.blockchain.name
  name     = "MARTS"

  is_transient        = "false"
  with_managed_access = "false"

  lifecycle {
    prevent_destroy = true
  }
}

resource "snowflake_schema" "control" {
  database = snowflake_database.blockchain.name
  name     = "CONTROL"

  is_transient        = "false"
  with_managed_access = "false"
  comment             = "Operational control state separate from analytical models"

  lifecycle {
    prevent_destroy = true
  }
}
