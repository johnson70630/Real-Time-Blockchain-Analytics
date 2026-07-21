import logging
from pathlib import Path

import duckdb

from config.logging import configure_logging
from config.settings import (
    GOLD_PIPELINE_SUMMARY_FILE,
    GOLD_RECENT_SWAPS_FILE,
    GOLD_SWAPS_PER_MINUTE_FILE,
    GOLD_TOP_POOLS_FILE,
    SILVER_PARQUET_GLOB,
)
from config.versions import GOLD_JOB_VERSION
from spark.build_swaps_silver import (
    SILVER_OUTPUT_COLUMNS,
    create_compatible_view,
)
from spark.parquet import write_relation_atomic

logger = logging.getLogger(__name__)


AGGREGATE_METADATA_SQL = f"""
            -- Aggregate outputs retain representative upstream provenance.
            MAX(producer_version) AS producer_version,
            MAX(schema_version) AS schema_version,
            MAX(bronze_processed_at) AS bronze_processed_at,
            MAX(bronze_file) AS bronze_file,
            MAX(silver_processed_at) AS silver_processed_at,
            MAX(silver_job_version) AS silver_job_version,
            -- Gold processing time and version identify this aggregation run.
            CURRENT_TIMESTAMP AS gold_processed_at,
            '{GOLD_JOB_VERSION}' AS aggregation_version
"""


def write_parquet(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    output_path: Path,
) -> None:
    write_relation_atomic(connection, connection.sql(query), output_path)

    logger.info("Wrote %s", output_path)


def _create_silver_source(
    connection: duckdb.DuckDBPyConnection,
    silver_path: Path,
) -> None:
    connection.read_parquet(str(silver_path)).create_view("silver_raw")
    create_compatible_view(
        connection,
        "silver_raw",
        "silver",
        SILVER_OUTPUT_COLUMNS,
    )


def build_swaps_per_minute(
    silver_path: Path = SILVER_PARQUET_GLOB,
    output_path: Path = GOLD_SWAPS_PER_MINUTE_FILE,
) -> None:
    connection = duckdb.connect()
    try:
        _create_silver_source(connection, silver_path)
        write_parquet(
            connection,
            f"""
        SELECT
            DATE_TRUNC('minute', kafka_timestamp) AS minute_ts,
            protocol,
            chain,
            COUNT(*) AS swap_count,
            COUNT(DISTINCT pool_address) AS unique_pools,
            MIN(block_number) AS min_block_number,
            MAX(block_number) AS max_block_number,
            {AGGREGATE_METADATA_SQL}
        FROM silver
        GROUP BY 1, 2, 3
        ORDER BY minute_ts
        """,
            output_path,
        )
    finally:
        connection.close()


def build_top_pools(
    silver_path: Path = SILVER_PARQUET_GLOB,
    output_path: Path = GOLD_TOP_POOLS_FILE,
) -> None:
    connection = duckdb.connect()
    try:
        _create_silver_source(connection, silver_path)
        write_parquet(
            connection,
            f"""
        SELECT
            protocol,
            chain,
            pool_address,
            COUNT(*) AS swap_count,
            MIN(block_number) AS first_block_seen,
            MAX(block_number) AS latest_block_seen,
            MIN(kafka_timestamp) AS first_seen_at,
            MAX(kafka_timestamp) AS latest_seen_at,
            {AGGREGATE_METADATA_SQL}
        FROM silver
        GROUP BY 1, 2, 3
        ORDER BY swap_count DESC
        LIMIT 50
        """,
            output_path,
        )
    finally:
        connection.close()


def build_pipeline_summary(
    silver_path: Path = SILVER_PARQUET_GLOB,
    output_path: Path = GOLD_PIPELINE_SUMMARY_FILE,
) -> None:
    connection = duckdb.connect()
    try:
        _create_silver_source(connection, silver_path)
        write_parquet(
            connection,
            f"""
        SELECT
            protocol,
            chain,
            COUNT(*) AS total_swaps,
            COUNT(DISTINCT transaction_hash) AS unique_transactions,
            COUNT(DISTINCT pool_address) AS unique_pools,
            MIN(block_number) AS min_block_number,
            MAX(block_number) AS latest_block_number,
            MIN(kafka_timestamp) AS first_event_time,
            MAX(kafka_timestamp) AS latest_event_time,
            MIN(ingested_at) AS first_ingested_at,
            MAX(ingested_at) AS latest_ingested_at,
            {AGGREGATE_METADATA_SQL}
        FROM silver
        GROUP BY 1, 2
        """,
            output_path,
        )
    finally:
        connection.close()


def build_recent_swaps(
    silver_path: Path = SILVER_PARQUET_GLOB,
    output_path: Path = GOLD_RECENT_SWAPS_FILE,
) -> None:
    connection = duckdb.connect()
    try:
        _create_silver_source(connection, silver_path)
        write_parquet(
            connection,
            f"""
        SELECT
            event_id,
            protocol,
            chain,
            event_type,
            block_number,
            transaction_hash,
            pool_address,
            log_index,
            kafka_timestamp,
            producer_version,
            schema_version,
            ingested_at,
            bronze_processed_at,
            bronze_file,
            silver_processed_at,
            silver_job_version,
            CURRENT_TIMESTAMP AS gold_processed_at,
            '{GOLD_JOB_VERSION}' AS aggregation_version
        FROM silver
        ORDER BY kafka_timestamp DESC, block_number DESC
        LIMIT 100
        """,
            output_path,
        )
    finally:
        connection.close()


def main() -> None:
    configure_logging()
    build_swaps_per_minute()
    build_top_pools()
    build_pipeline_summary()
    build_recent_swaps()

    logger.info("Gold analytics layer complete")


if __name__ == "__main__":
    main()
