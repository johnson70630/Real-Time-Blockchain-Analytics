"""Build analytics-ready Aave V3 Silver event datasets from Bronze."""

import logging
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import duckdb

from config.settings import (
    AAVE_BORROW_EVENTS_FILE,
    AAVE_LIQUIDATION_EVENTS_FILE,
    AAVE_REPAY_EVENTS_FILE,
    BRONZE_OUTPUT_PATH,
)
from config.versions import SILVER_JOB_VERSION

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AaveSilverModel:
    """Declarative mapping from one Aave event type to a Silver dataset."""

    event_type: str
    output_path: Path
    payload_fields: tuple[str, ...]


COMMON_COLUMNS = (
    "protocol",
    "chain",
    "event_type",
    "event_date",
    "block_timestamp",
    "transaction_hash",
    "block_number",
    "log_index",
    "kafka_timestamp",
    "ingested_at",
    "producer_version",
    "schema_version",
    "bronze_processed_at",
    "bronze_file",
)

RAW_PAYLOAD_FIELDS = (
    "contract_address",
    "raw_data",
    "raw_topics",
)

PAYLOAD_FIELD_TYPES = {
    "raw_topics": "VARCHAR[]",
    "interest_rate_mode": "INTEGER",
    "referral_code": "INTEGER",
    "use_atokens": "BOOLEAN",
    "receive_atoken": "BOOLEAN",
}

AAVE_SILVER_MODELS = (
    AaveSilverModel(
        event_type="borrow",
        output_path=AAVE_BORROW_EVENTS_FILE,
        payload_fields=(
            "reserve",
            "user",
            "on_behalf_of",
            "amount_raw",
            "interest_rate_mode",
            "borrow_rate_raw",
            "referral_code",
        ),
    ),
    AaveSilverModel(
        event_type="repay",
        output_path=AAVE_REPAY_EVENTS_FILE,
        payload_fields=(
            "reserve",
            "user",
            "repayer",
            "amount_raw",
            "use_atokens",
        ),
    ),
    AaveSilverModel(
        event_type="liquidation",
        output_path=AAVE_LIQUIDATION_EVENTS_FILE,
        payload_fields=(
            "collateral_asset",
            "debt_asset",
            "user",
            "debt_to_cover_raw",
            "liquidated_collateral_amount_raw",
            "liquidator",
            "receive_atoken",
        ),
    ),
)


def discover_bronze_files(
    bronze_root: Path = BRONZE_OUTPUT_PATH,
) -> list[Path]:
    """Return sorted Bronze Parquet files without touching generated data."""
    if not bronze_root.exists():
        return []

    return sorted(
        path
        for path in bronze_root.rglob("*.parquet")
        if path.is_file() and not path.name.startswith((".", "_"))
    )


def silver_query(model: AaveSilverModel) -> str:
    """Return the normalization query for one Aave event dataset."""
    common_columns = ",\n            ".join(COMMON_COLUMNS)
    payload_columns = ",\n            ".join(
        _payload_projection(field)
        for field in (*RAW_PAYLOAD_FIELDS, *model.payload_fields)
    )

    return f"""
        SELECT
            chain || '-' || transaction_hash || '-'
                || CAST(log_index AS VARCHAR) AS event_id,
            {common_columns},
            {payload_columns},
            CURRENT_TIMESTAMP AS silver_processed_at,
            '{SILVER_JOB_VERSION}' AS silver_job_version
        FROM bronze_events
        WHERE protocol = 'aave_v3'
          AND event_type = '{model.event_type}'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY chain, transaction_hash, log_index
            ORDER BY kafka_timestamp DESC, ingested_at DESC
        ) = 1
        ORDER BY block_number, log_index
    """


def _payload_projection(field: str) -> str:
    """Read a payload field safely across old and new Bronze struct schemas."""
    json_value = f"json_extract(to_json(payload), '$.{field}')"
    field_type = PAYLOAD_FIELD_TYPES.get(field)
    if field_type is not None:
        return f"CAST({json_value} AS {field_type}) AS {field}"
    return (
        f"json_extract_string(to_json(payload), '$.{field}') AS {field}"
    )


def write_silver_model(
    connection: duckdb.DuckDBPyConnection,
    model: AaveSilverModel,
) -> int:
    """Atomically write one normalized Aave Silver dataset."""
    model.output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = model.output_path.with_name(
        f".{model.output_path.stem}.{uuid4().hex}.tmp.parquet"
    )

    try:
        relation = connection.sql(silver_query(model))
        row_count = relation.count("*").fetchone()[0]
        relation.write_parquet(str(temporary_output), overwrite=True)
        temporary_output.replace(model.output_path)
    except Exception:
        temporary_output.unlink(missing_ok=True)
        raise

    logger.info(
        "Wrote %s Aave %s events to: %s",
        row_count,
        model.event_type,
        model.output_path,
    )
    return row_count


def build_aave_silver(
    bronze_root: Path = BRONZE_OUTPUT_PATH,
    models: tuple[AaveSilverModel, ...] = AAVE_SILVER_MODELS,
) -> dict[str, int]:
    """Build all configured Aave Silver datasets from Bronze Parquet."""
    bronze_files = discover_bronze_files(bronze_root)
    logger.info("Bronze Parquet files discovered: %s", len(bronze_files))

    if not bronze_files:
        logger.info("No Bronze files found; Aave Silver outputs are unchanged")
        return {}

    connection = duckdb.connect()
    try:
        connection.read_parquet(
            [str(path) for path in bronze_files],
            hive_partitioning=True,
            union_by_name=True,
        ).create_view("bronze_events")
        return {
            model.event_type: write_silver_model(connection, model)
            for model in models
        }
    finally:
        connection.close()


def main() -> None:
    build_aave_silver()


if __name__ == "__main__":
    main()
