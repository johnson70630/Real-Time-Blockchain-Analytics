"""Generate Snowflake SQL for the Uniswap Silver-to-RAW landing flow."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Mapping

from config.storage import DATASET_PATHS

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# COPY applies this pattern while selecting stage objects, before Snowflake
# attempts Parquet decoding. Spark control files such as _SUCCESS cannot match.
PARQUET_FILE_PATTERN = r".*[.]parquet$"


@dataclass(frozen=True)
class WarehouseDataset:
    """Map one canonical lake dataset to its Snowflake RAW table grain."""

    dataset: str
    table: str
    stage_path: str
    record_key: str
    source_timestamp: str


UNISWAP_SWAPS = WarehouseDataset(
    dataset="silver_uniswap_swaps",
    table="UNISWAP_SWAPS",
    stage_path="swaps/",
    record_key="event_id",
    source_timestamp="block_timestamp",
)

# This milestone intentionally registers only the first end-to-end dataset.
WAREHOUSE_DATASETS: tuple[WarehouseDataset, ...] = (UNISWAP_SWAPS,)


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


def generate_setup_sql(config: SnowflakeLandingConfig) -> str:
    """Create only the persistent Uniswap landing and canonical RAW tables."""
    raw_table = _qualified(config, UNISWAP_SWAPS.table)
    landing_table = _qualified(config, f"{UNISWAP_SWAPS.table}_LANDING")
    return f"""CREATE TABLE IF NOT EXISTS {landing_table} (
  record VARIANT NOT NULL,
  source_file VARCHAR NOT NULL,
  source_file_row_number NUMBER NOT NULL,
  loaded_at TIMESTAMP_TZ NOT NULL,
  merged_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS {raw_table} (
  event_id VARCHAR NOT NULL,
  chain VARCHAR,
  protocol VARCHAR,
  block_timestamp TIMESTAMP_TZ,
  producer_version VARCHAR,
  schema_version VARCHAR,
  silver_job_version VARCHAR,
  record VARIANT NOT NULL,
  source_file VARCHAR NOT NULL,
  source_file_row_number NUMBER NOT NULL,
  loaded_at TIMESTAMP_TZ NOT NULL
);"""


def generate_dataset_load_sql(
    config: SnowflakeLandingConfig,
    dataset: WarehouseDataset = UNISWAP_SWAPS,
) -> str:
    """Generate incremental COPY and idempotent MERGE SQL for Uniswap swaps."""
    if dataset != UNISWAP_SWAPS:
        raise ValueError("Only Uniswap swaps are supported in this milestone")

    raw_table = _qualified(config, dataset.table)
    landing_table = _qualified(config, f"{dataset.table}_LANDING")
    stage = _qualified(config, config.stage)
    file_format = _qualified(config, config.file_format)
    key = dataset.record_key
    timestamp = dataset.source_timestamp
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
    record:{key}::VARCHAR AS event_id,
    record:chain::VARCHAR AS chain,
    record:protocol::VARCHAR AS protocol,
    TRY_TO_TIMESTAMP_TZ(record:{timestamp}::VARCHAR) AS block_timestamp,
    record:producer_version::VARCHAR AS producer_version,
    record:schema_version::VARCHAR AS schema_version,
    record:silver_job_version::VARCHAR AS silver_job_version,
    record,
    source_file,
    source_file_row_number,
    loaded_at
  FROM {landing_table}
  WHERE merged_at IS NULL
    AND record:{key} IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY record:{key}::VARCHAR
    ORDER BY loaded_at DESC, source_file DESC, source_file_row_number DESC
  ) = 1
) AS source
ON target.event_id = source.event_id
WHEN MATCHED AND source.loaded_at >= target.loaded_at THEN UPDATE SET
  chain = source.chain,
  protocol = source.protocol,
  block_timestamp = source.block_timestamp,
  producer_version = source.producer_version,
  schema_version = source.schema_version,
  silver_job_version = source.silver_job_version,
  record = source.record,
  source_file = source.source_file,
  source_file_row_number = source.source_file_row_number,
  loaded_at = source.loaded_at
WHEN NOT MATCHED THEN INSERT (
  event_id,
  chain,
  protocol,
  block_timestamp,
  producer_version,
  schema_version,
  silver_job_version,
  record,
  source_file,
  source_file_row_number,
  loaded_at
) VALUES (
  source.event_id,
  source.chain,
  source.protocol,
  source.block_timestamp,
  source.producer_version,
  source.schema_version,
  source.silver_job_version,
  source.record,
  source.source_file,
  source.source_file_row_number,
  source.loaded_at
);

UPDATE {landing_table}
SET merged_at = CURRENT_TIMESTAMP()
WHERE merged_at IS NULL;

COMMIT;"""


def generate_all_sql(config: SnowflakeLandingConfig) -> str:
    """Generate table setup plus the Uniswap incremental load transaction."""
    return "\n\n".join(
        (
            generate_setup_sql(config),
            generate_dataset_load_sql(config),
        )
    )


def validate_uniswap_dataset_path() -> None:
    """Ensure the warehouse mapping still targets the canonical Silver output."""
    expected = "silver/swaps/swaps_silver.parquet"
    actual = DATASET_PATHS[UNISWAP_SWAPS.dataset]
    if actual != expected:
        raise ValueError(
            f"Uniswap Silver path changed: expected {expected}, found {actual}"
        )
