"""Generate Snowflake SQL for canonical S3-to-RAW landing flows."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Mapping

from config.storage import DATASET_PATHS, StorageConfig

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
PARQUET_FILE_PATTERN = r".*[.]parquet$"


@dataclass(frozen=True)
class WarehouseColumn:
    """Define one typed RAW column extracted from a Parquet VARIANT."""

    name: str
    sql_type: str
    expression: str
    required: bool = False


@dataclass(frozen=True)
class WarehouseDataset:
    """Map one canonical lake dataset to its Snowflake RAW table grain."""

    dataset: str
    table: str
    stage_path: str
    natural_key: tuple[str, ...]
    columns: tuple[WarehouseColumn, ...]


def _text(name: str, *, required: bool = False) -> WarehouseColumn:
    return WarehouseColumn(
        name,
        "VARCHAR",
        f"record:{name}::VARCHAR",
        required,
    )


def _number(name: str, *, required: bool = False) -> WarehouseColumn:
    return WarehouseColumn(
        name,
        "NUMBER(38, 0)",
        f"TRY_TO_NUMBER(record:{name}::VARCHAR)::NUMBER(38, 0)",
        required,
    )


def _timestamp(name: str) -> WarehouseColumn:
    return WarehouseColumn(
        name,
        "TIMESTAMP_TZ",
        f"TRY_TO_TIMESTAMP_TZ(record:{name}::VARCHAR)",
    )


UNISWAP_SWAPS = WarehouseDataset(
    dataset="silver_uniswap_swaps",
    table="UNISWAP_SWAPS",
    stage_path="silver/swaps/swaps_silver.parquet/",
    natural_key=("event_id",),
    columns=(
        _text("event_id", required=True),
        _text("chain"),
        _text("protocol"),
        _timestamp("block_timestamp"),
        _text("producer_version"),
        _text("schema_version"),
        _text("silver_job_version"),
    ),
)

UNISWAP_V3_POOLS = WarehouseDataset(
    dataset="reference_uniswap_v3_pools",
    table="UNISWAP_V3_POOLS",
    stage_path="reference/uniswap_v3/pools/",
    natural_key=("chain", "protocol", "pool_address"),
    columns=(
        _text("chain", required=True),
        _text("protocol", required=True),
        _text("pool_address", required=True),
        _text("token0_address"),
        _text("token1_address"),
        _number("fee_tier"),
        _number("tick_spacing"),
        _text("factory_address"),
        _text("metadata_source"),
        WarehouseColumn(
            "factory_verified",
            "BOOLEAN",
            "TRY_TO_BOOLEAN(record:factory_verified::VARCHAR)",
        ),
        _number("created_block"),
        _text("created_transaction_hash"),
        _number("created_log_index"),
        _timestamp("created_block_timestamp"),
        _timestamp("ingested_at"),
        _text("producer_version"),
        _text("schema_version"),
        _timestamp("processed_at"),
    ),
)

TOKENS = WarehouseDataset(
    dataset="reference_tokens",
    table="TOKENS",
    stage_path="reference/tokens/",
    natural_key=("chain", "token_address"),
    columns=(
        _text("chain", required=True),
        _text("token_address", required=True),
        _text("symbol"),
        _text("name"),
        _number("decimals"),
        _text("metadata_source"),
        _number("metadata_block_number"),
        _timestamp("ingested_at"),
        _timestamp("processed_at"),
    ),
)

WAREHOUSE_DATASETS: tuple[WarehouseDataset, ...] = (
    UNISWAP_SWAPS,
    UNISWAP_V3_POOLS,
    TOKENS,
)


def _identifier(value: str, setting: str) -> str:
    value = value.strip().upper()
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{setting} must be a valid Snowflake identifier")
    return value


@dataclass(frozen=True)
class SnowflakeLandingConfig:
    """Non-secret object names for the pre-provisioned Snowflake environment."""

    database: str = "BLOCKCHAIN_ANALYTICS"
    schema: str = "RAW"
    stage: str = "SILVER_S3_STAGE"
    file_format: str = "PARQUET_FORMAT"

    def __post_init__(self) -> None:
        for setting, value in (
            ("SNOWFLAKE_DATABASE", self.database),
            ("SNOWFLAKE_RAW_SCHEMA", self.schema),
            ("SNOWFLAKE_STAGE", self.stage),
            ("SNOWFLAKE_FILE_FORMAT", self.file_format),
        ):
            _identifier(value, setting)

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> SnowflakeLandingConfig:
        """Load non-secret Snowflake object names from the environment."""
        values = os.environ if environ is None else environ
        return cls(
            database=values.get(
                "SNOWFLAKE_DATABASE",
                "BLOCKCHAIN_ANALYTICS",
            ),
            schema=values.get("SNOWFLAKE_RAW_SCHEMA", "RAW"),
            stage=values.get("SNOWFLAKE_STAGE", "SILVER_S3_STAGE"),
            file_format=values.get(
                "SNOWFLAKE_FILE_FORMAT",
                "PARQUET_FORMAT",
            ),
        )


def _qualified(config: SnowflakeLandingConfig, name: str) -> str:
    return f"{config.database}.{config.schema}.{name}"


def generate_stage_root_sql(
    config: SnowflakeLandingConfig,
    storage: StorageConfig,
) -> str:
    """Repoint the existing external stage to the common data-lake prefix."""
    if not storage.is_s3:
        raise ValueError("Snowflake external-stage setup requires S3 storage")
    prefix = storage.prefix.strip("/")
    url = f"s3://{storage.bucket}/{prefix}/"
    return f"ALTER STAGE {_qualified(config, config.stage)} SET URL = '{url}';"


def _landing_ddl(config: SnowflakeLandingConfig, dataset: WarehouseDataset) -> str:
    landing_table = _qualified(config, f"{dataset.table}_LANDING")
    return f"""CREATE TABLE IF NOT EXISTS {landing_table} (
  record VARIANT NOT NULL,
  source_file VARCHAR NOT NULL,
  source_file_row_number NUMBER NOT NULL,
  loaded_at TIMESTAMP_TZ NOT NULL,
  merged_at TIMESTAMP_TZ
);"""


def _raw_ddl(config: SnowflakeLandingConfig, dataset: WarehouseDataset) -> str:
    raw_table = _qualified(config, dataset.table)
    typed_columns = ",\n".join(
        f"  {column.name} {column.sql_type}"
        f"{' NOT NULL' if column.required else ''}"
        for column in dataset.columns
    )
    return f"""CREATE TABLE IF NOT EXISTS {raw_table} (
{typed_columns},
  record VARIANT NOT NULL,
  source_file VARCHAR NOT NULL,
  source_file_row_number NUMBER NOT NULL,
  loaded_at TIMESTAMP_TZ NOT NULL
);"""


def generate_setup_sql(config: SnowflakeLandingConfig) -> str:
    """Create persistent landing and canonical RAW tables for all datasets."""
    statements: list[str] = []
    for dataset in WAREHOUSE_DATASETS:
        statements.extend(
            (_landing_ddl(config, dataset), _raw_ddl(config, dataset))
        )
    return "\n\n".join(statements)


def generate_dataset_load_sql(
    config: SnowflakeLandingConfig,
    dataset: WarehouseDataset = UNISWAP_SWAPS,
) -> str:
    """Generate incremental COPY and deterministic natural-key MERGE SQL."""
    if dataset not in WAREHOUSE_DATASETS:
        raise ValueError(f"Unsupported warehouse dataset: {dataset.dataset}")

    raw_table = _qualified(config, dataset.table)
    landing_table = _qualified(config, f"{dataset.table}_LANDING")
    stage = _qualified(config, config.stage)
    file_format = _qualified(config, config.file_format)
    projections = ",\n".join(
        f"    {column.expression} AS {column.name}"
        for column in dataset.columns
    )
    key_partition = ", ".join(
        next(
            column.expression
            for column in dataset.columns
            if column.name == key
        )
        for key in dataset.natural_key
    )
    key_filter = "\n    AND ".join(
        f"record:{key} IS NOT NULL" for key in dataset.natural_key
    )
    join_condition = " AND ".join(
        f"target.{key} = source.{key}" for key in dataset.natural_key
    )
    typed_names = [column.name for column in dataset.columns]
    update_names = [
        *(name for name in typed_names if name not in dataset.natural_key),
        "record",
        "source_file",
        "source_file_row_number",
        "loaded_at",
    ]
    updates = ",\n".join(
        f"  {name} = source.{name}" for name in update_names
    )
    insert_names = (
        *typed_names,
        "record",
        "source_file",
        "source_file_row_number",
        "loaded_at",
    )
    insert_columns = ",\n".join(f"  {name}" for name in insert_names)
    insert_values = ",\n".join(f"  source.{name}" for name in insert_names)
    return f"""COPY INTO {landing_table} (
  record,
  source_file,
  source_file_row_number,
  loaded_at
)
FROM (
  SELECT
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    METADATA$START_SCAN_TIME
  FROM @{stage}/{dataset.stage_path}
)
PATTERN = '{PARQUET_FILE_PATTERN}'
FILE_FORMAT = (FORMAT_NAME = '{file_format}')
ON_ERROR = 'ABORT_STATEMENT';

BEGIN TRANSACTION;

MERGE INTO {raw_table} AS target
USING (
  SELECT
{projections},
    record,
    source_file,
    source_file_row_number,
    loaded_at
  FROM {landing_table}
  WHERE merged_at IS NULL
    AND {key_filter}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {key_partition}
    ORDER BY loaded_at DESC, source_file DESC, source_file_row_number DESC
  ) = 1
) AS source
ON {join_condition}
WHEN MATCHED AND source.loaded_at >= target.loaded_at THEN UPDATE SET
{updates}
WHEN NOT MATCHED THEN INSERT (
{insert_columns}
) VALUES (
{insert_values}
);

UPDATE {landing_table}
SET merged_at = CURRENT_TIMESTAMP()
WHERE merged_at IS NULL;

COMMIT;"""


def generate_all_sql(config: SnowflakeLandingConfig) -> str:
    """Generate table setup plus all incremental landing transactions."""
    load_sql = tuple(
        generate_dataset_load_sql(config, dataset)
        for dataset in WAREHOUSE_DATASETS
    )
    return "\n\n".join((generate_setup_sql(config), *load_sql))


def validate_dataset_paths() -> None:
    """Ensure warehouse stage mappings match canonical lake dataset paths."""
    for dataset in WAREHOUSE_DATASETS:
        expected = f"{DATASET_PATHS[dataset.dataset].rstrip('/')}/"
        if dataset.stage_path != expected:
            raise ValueError(
                f"{dataset.dataset} stage path changed: expected "
                f"{expected}, found {dataset.stage_path}"
            )


def validate_uniswap_dataset_path() -> None:
    """Backward-compatible validation entry point for existing callers."""
    validate_dataset_paths()
