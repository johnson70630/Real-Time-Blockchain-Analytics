import re
from pathlib import Path

import pytest

from config.storage import DATASET_PATHS, StorageConfig
from warehouse.snowflake import (
    AAVE_BORROWS,
    AAVE_LIQUIDATIONS,
    AAVE_REPAYS,
    CHAINLINK_PRICES,
    PARQUET_FILE_PATTERN,
    TOKENS,
    UNISWAP_SWAPS,
    UNISWAP_V3_POOLS,
    WAREHOUSE_DATASETS,
    SnowflakeLandingConfig,
    generate_all_sql,
    generate_dataset_load_sql,
    generate_setup_sql,
    generate_stage_root_sql,
    validate_dataset_paths,
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


def test_setup_creates_persistent_landing_and_raw_tables_only(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_setup_sql(landing_config)

    for table in (
        "UNISWAP_SWAPS",
        "UNISWAP_V3_POOLS",
        "TOKENS",
        "AAVE_BORROWS",
        "AAVE_REPAYS",
        "AAVE_LIQUIDATIONS",
        "CHAINLINK_PRICES",
    ):
        assert (
            f"CREATE TABLE IF NOT EXISTS BLOCKCHAIN_ANALYTICS.RAW.{table}_LANDING"
            in sql
        )
        assert (
            f"CREATE TABLE IF NOT EXISTS BLOCKCHAIN_ANALYTICS.RAW.{table}"
            in sql
        )
    assert "TEMPORARY" not in sql
    assert "CREATE STAGE" not in sql
    assert "CREATE STORAGE INTEGRATION" not in sql
    assert "CREATE DATABASE" not in sql
    assert "CREATE SCHEMA" not in sql


def test_existing_stage_is_reused_at_common_lake_root(tmp_path: Path) -> None:
    storage = StorageConfig(
        mode="s3",
        project_root=tmp_path,
        bucket="analytics-lake-123",
        prefix="production/blockchain",
        aws_region="us-west-2",
    )

    sql = generate_stage_root_sql(SnowflakeLandingConfig(), storage)

    assert sql == (
        "ALTER STAGE BLOCKCHAIN_ANALYTICS.RAW.SILVER_S3_STAGE SET URL = "
        "'s3://analytics-lake-123/production/blockchain/';"
    )
    assert "STORAGE_INTEGRATION" not in sql


def test_stage_setup_requires_s3_storage(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires S3"):
        generate_stage_root_sql(
            SnowflakeLandingConfig(),
            StorageConfig(mode="local", project_root=tmp_path),
        )


def test_canonical_lake_datasets_are_registered() -> None:
    assert WAREHOUSE_DATASETS == (
        UNISWAP_SWAPS,
        UNISWAP_V3_POOLS,
        TOKENS,
        AAVE_BORROWS,
        AAVE_REPAYS,
        AAVE_LIQUIDATIONS,
        CHAINLINK_PRICES,
    )
    assert UNISWAP_SWAPS.stage_path == (
        "silver/swaps/swaps_silver.parquet/"
    )
    assert UNISWAP_V3_POOLS.stage_path == "reference/uniswap_v3/pools/"
    assert TOKENS.stage_path == "reference/tokens/"
    for dataset in WAREHOUSE_DATASETS:
        assert dataset.dataset in DATASET_PATHS
    validate_dataset_paths()


@pytest.mark.parametrize(
    ("dataset", "stage_path"),
    [
        (UNISWAP_SWAPS, "silver/swaps/swaps_silver.parquet/"),
        (UNISWAP_V3_POOLS, "reference/uniswap_v3/pools/"),
        (TOKENS, "reference/tokens/"),
        (AAVE_BORROWS, "silver/aave_v3/borrow_events.parquet/"),
        (AAVE_REPAYS, "silver/aave_v3/repay_events.parquet/"),
        (
            AAVE_LIQUIDATIONS,
            "silver/aave_v3/liquidation_events.parquet/",
        ),
        (CHAINLINK_PRICES, "silver/market_prices/"),
    ],
)
def test_copy_selects_only_parquet_from_canonical_stage_path(
    landing_config: SnowflakeLandingConfig,
    dataset,
    stage_path: str,
) -> None:
    sql = generate_dataset_load_sql(landing_config, dataset)

    assert (
        f"FROM @BLOCKCHAIN_ANALYTICS.RAW.SILVER_S3_STAGE/{stage_path}"
        in sql
    )
    assert f"PATTERN = '{PARQUET_FILE_PATTERN}'" in sql
    assert (
        "FILE_FORMAT = (FORMAT_NAME = "
        "'BLOCKCHAIN_ANALYTICS.RAW.PARQUET_FORMAT')" in sql
    )
    assert "FORCE = TRUE" not in sql.upper()
    assert "quarantine" not in sql.lower()


def test_parquet_pattern_excludes_spark_control_files() -> None:
    pattern = re.compile(PARQUET_FILE_PATTERN)

    assert pattern.fullmatch("reference/tokens/part-00000.parquet")
    assert not pattern.fullmatch("reference/tokens/_SUCCESS")
    assert not pattern.fullmatch("reference/tokens/_committed_123")


def test_copy_preserves_complete_record_and_file_lineage(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_dataset_load_sql(landing_config, TOKENS)

    assert "SELECT\n    $1," in sql
    assert "METADATA$FILENAME" in sql
    assert "METADATA$FILE_ROW_NUMBER" in sql
    assert "METADATA$START_SCAN_TIME" in sql
    setup_sql = generate_setup_sql(landing_config)
    assert "record VARIANT NOT NULL" in setup_sql
    assert "source_file VARCHAR NOT NULL" in setup_sql
    assert "loaded_at TIMESTAMP_TZ NOT NULL" in setup_sql


def test_reference_merges_use_complete_natural_keys(
    landing_config: SnowflakeLandingConfig,
) -> None:
    pool_sql = generate_dataset_load_sql(landing_config, UNISWAP_V3_POOLS)
    token_sql = generate_dataset_load_sql(landing_config, TOKENS)

    assert (
        "ON target.chain = source.chain AND "
        "target.protocol = source.protocol AND "
        "target.pool_address = source.pool_address" in pool_sql
    )
    assert (
        "PARTITION BY record:chain::VARCHAR, record:protocol::VARCHAR, "
        "record:pool_address::VARCHAR" in pool_sql
    )
    assert (
        "ON target.chain = source.chain AND "
        "target.token_address = source.token_address" in token_sql
    )
    assert (
        "PARTITION BY record:chain::VARCHAR, "
        "record:token_address::VARCHAR" in token_sql
    )
    for sql in (pool_sql, token_sql):
        assert "WHEN MATCHED" in sql
        assert "WHEN NOT MATCHED" in sql
        assert "WHERE merged_at IS NULL" in sql
        assert (
            "ORDER BY loaded_at DESC, source_file DESC, "
            "source_file_row_number DESC" in sql
        )


def test_raw_tables_preserve_reference_fields(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_setup_sql(landing_config)

    for column in (
        "pool_address",
        "token0_address",
        "token1_address",
        "fee_tier",
        "tick_spacing",
        "factory_address",
        "factory_verified",
        "created_block_timestamp",
        "token_address",
        "symbol",
        "name",
        "decimals",
        "metadata_block_number",
        "record VARIANT",
        "source_file",
        "source_file_row_number",
        "loaded_at",
    ):
        assert column in sql


def test_aave_landing_preserves_exact_raw_amounts(
    landing_config: SnowflakeLandingConfig,
) -> None:
    sql = generate_setup_sql(landing_config)

    for column in (
        "amount_raw VARCHAR",
        "borrow_rate_raw VARCHAR",
        "debt_to_cover_raw VARCHAR",
        "liquidated_collateral_amount_raw VARCHAR",
    ):
        assert column in sql
    for dataset in (AAVE_BORROWS, AAVE_REPAYS, AAVE_LIQUIDATIONS):
        load_sql = generate_dataset_load_sql(landing_config, dataset)
        assert "ON target.event_id = source.event_id" in load_sql
        assert "PARTITION BY record:event_id::VARCHAR" in load_sql


def test_chainlink_landing_preserves_raw_and_precise_price_fields(
    landing_config: SnowflakeLandingConfig,
) -> None:
    setup_sql = generate_setup_sql(landing_config)
    load_sql = generate_dataset_load_sql(landing_config, CHAINLINK_PRICES)

    assert "round_id VARCHAR" in setup_sql
    assert "answer_raw VARCHAR" in setup_sql
    assert "price NUMBER(38, 18)" in setup_sql
    assert "TRY_TO_DECIMAL(record:price::VARCHAR, 38, 18)" in load_sql
    assert "ON target.observation_id = source.observation_id" in load_sql
    assert "PARTITION BY record:observation_id::VARCHAR" in load_sql


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
