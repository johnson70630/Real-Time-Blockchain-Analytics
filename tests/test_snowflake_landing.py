import re

import pytest

from config.storage import DATASET_PATHS
from warehouse.snowflake import (
    PARQUET_FILE_PATTERN,
    UNISWAP_SWAPS,
    WAREHOUSE_DATASETS,
    SnowflakeLandingConfig,
    generate_all_sql,
    generate_dataset_load_sql,
    generate_setup_sql,
    validate_uniswap_dataset_path,
)


@pytest.fixture
def landing_config() -> SnowflakeLandingConfig:
    return SnowflakeLandingConfig()


def test_config_references_provisioned_snowflake_objects(
    landing_config: SnowflakeLandingConfig,
) -> None:
    assert landing_config.database == "BLOCKCHAIN_ANALYTICS"
    assert landing_config.schema == "RAW"
    assert landing_config.stage == "SILVER_S3_STAGE"
    assert landing_config.file_format == "PARQUET_FORMAT"


def test_setup_creates_only_persistent_uniswap_tables(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_setup_sql(landing_config)

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "BLOCKCHAIN_ANALYTICS.RAW.UNISWAP_SWAPS_LANDING" in sql
    )
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "BLOCKCHAIN_ANALYTICS.RAW.UNISWAP_SWAPS" in sql
    )
    assert "TEMPORARY" not in sql
    assert "CREATE STAGE" not in sql
    assert "CREATE STORAGE INTEGRATION" not in sql
    assert "CREATE DATABASE" not in sql
    assert "CREATE SCHEMA" not in sql


def test_only_uniswap_silver_is_registered() -> None:
    assert WAREHOUSE_DATASETS == (UNISWAP_SWAPS,)
    assert UNISWAP_SWAPS.dataset == "silver_uniswap_swaps"
    assert DATASET_PATHS[UNISWAP_SWAPS.dataset] == (
        "silver/swaps/swaps_silver.parquet"
    )
    assert UNISWAP_SWAPS.stage_path == "swaps/"
    validate_uniswap_dataset_path()


def test_copy_selects_parquet_before_decoding(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_dataset_load_sql(landing_config)

    assert (
        "FROM @BLOCKCHAIN_ANALYTICS.RAW.SILVER_S3_STAGE/swaps/" in sql
    )
    assert f"PATTERN = '{PARQUET_FILE_PATTERN}'" in sql
    assert (
        "FILE_FORMAT = (FORMAT_NAME = "
        "'BLOCKCHAIN_ANALYTICS.RAW.PARQUET_FORMAT')" in sql
    )
    assert "WHERE METADATA$FILENAME" not in sql
    assert "FORCE = TRUE" not in sql.upper()


def test_parquet_pattern_excludes_spark_control_files() -> None:
    pattern = re.compile(PARQUET_FILE_PATTERN)

    assert pattern.fullmatch("swaps/swaps_silver.parquet/part-00000.parquet")
    assert not pattern.fullmatch("swaps/swaps_silver.parquet/_SUCCESS")
    assert not pattern.fullmatch("swaps/swaps_silver.parquet/_committed_123")


def test_copy_preserves_complete_record_and_file_lineage(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_dataset_load_sql(landing_config)

    assert "SELECT\n    $1," in sql
    assert "METADATA$FILENAME" in sql
    assert "METADATA$FILE_ROW_NUMBER" in sql
    assert "METADATA$START_SCAN_TIME" in sql
    setup_sql = generate_setup_sql(landing_config)
    assert "record VARIANT NOT NULL" in setup_sql
    assert "source_file VARCHAR NOT NULL" in setup_sql
    assert "loaded_at TIMESTAMP_TZ NOT NULL" in setup_sql


def test_merge_is_deterministic_and_keyed_by_event_id(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_dataset_load_sql(landing_config)

    assert "record:event_id::VARCHAR AS event_id" in sql
    assert "PARTITION BY record:event_id::VARCHAR" in sql
    assert "ON target.event_id = source.event_id" in sql
    assert "WHEN MATCHED" in sql
    assert "WHEN NOT MATCHED" in sql
    assert (
        "ORDER BY loaded_at DESC, source_file DESC, "
        "source_file_row_number DESC" in sql
    )
    assert "WHERE merged_at IS NULL" in sql


def test_raw_table_preserves_required_metadata(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_all_sql(landing_config)

    for column in (
        "event_id",
        "chain",
        "protocol",
        "block_timestamp",
        "producer_version",
        "schema_version",
        "silver_job_version",
        "record VARIANT",
        "source_file",
        "source_file_row_number",
        "loaded_at",
    ):
        assert column in sql


def test_generated_sql_contains_no_credentials(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_all_sql(landing_config).upper()

    for forbidden in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "PASSWORD",
        "STORAGE_AWS_ROLE_ARN",
    ):
        assert forbidden not in sql


def test_invalid_snowflake_configuration_is_rejected() -> None:
    with pytest.raises(ValueError, match="SNOWFLAKE_DATABASE"):
        SnowflakeLandingConfig(database="bad-name")

    with pytest.raises(ValueError, match="SNOWFLAKE_FILE_FORMAT"):
        SnowflakeLandingConfig(file_format="bad format")
