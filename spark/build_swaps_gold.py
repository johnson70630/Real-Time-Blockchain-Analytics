import logging
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

SILVER_PATH = "data/silver/swaps/*.parquet"

GOLD_SWAPS_PER_MINUTE = "data/gold/swaps_per_minute/swaps_per_minute.parquet"
GOLD_TOP_POOLS = "data/gold/top_pools/top_pools.parquet"
GOLD_PIPELINE_SUMMARY = "data/gold/pipeline_summary/pipeline_summary.parquet"
GOLD_RECENT_SWAPS = "data/gold/recent_swaps/recent_swaps.parquet"


def write_parquet(query: str, output_path: str) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    duckdb.sql(
        f"""
        COPY (
            {query}
        )
        TO '{output_path}'
        (FORMAT PARQUET, OVERWRITE true)
        """
    )

    logger.info("Wrote %s", output_path)


def build_swaps_per_minute() -> None:
    write_parquet(
        f"""
        SELECT
            DATE_TRUNC('minute', kafka_timestamp) AS minute_ts,
            protocol,
            chain,
            COUNT(*) AS swap_count,
            COUNT(DISTINCT pool_address) AS unique_pools,
            MIN(block_number) AS min_block_number,
            MAX(block_number) AS max_block_number
        FROM read_parquet('{SILVER_PATH}')
        GROUP BY 1, 2, 3
        ORDER BY minute_ts
        """,
        GOLD_SWAPS_PER_MINUTE,
    )


def build_top_pools() -> None:
    write_parquet(
        f"""
        SELECT
            protocol,
            chain,
            pool_address,
            COUNT(*) AS swap_count,
            MIN(block_number) AS first_block_seen,
            MAX(block_number) AS latest_block_seen,
            MIN(kafka_timestamp) AS first_seen_at,
            MAX(kafka_timestamp) AS latest_seen_at
        FROM read_parquet('{SILVER_PATH}')
        GROUP BY 1, 2, 3
        ORDER BY swap_count DESC
        LIMIT 50
        """,
        GOLD_TOP_POOLS,
    )


def build_pipeline_summary() -> None:
    write_parquet(
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
            MAX(ingested_at) AS latest_ingested_at
        FROM read_parquet('{SILVER_PATH}')
        GROUP BY 1, 2
        """,
        GOLD_PIPELINE_SUMMARY,
    )


def build_recent_swaps() -> None:
    write_parquet(
        f"""
        SELECT
            event_id,
            chain,
            event_type,
            block_number,
            transaction_hash,
            pool_address,
            log_index,
            kafka_timestamp,
            ingested_at
        FROM read_parquet('{SILVER_PATH}')
        ORDER BY kafka_timestamp DESC, block_number DESC
        LIMIT 100
        """,
        GOLD_RECENT_SWAPS,
    )


def main() -> None:
    build_swaps_per_minute()
    build_top_pools()
    build_pipeline_summary()
    build_recent_swaps()

    logger.info("Gold analytics layer complete")


if __name__ == "__main__":
    main()