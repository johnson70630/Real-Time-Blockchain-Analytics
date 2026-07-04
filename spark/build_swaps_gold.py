from pathlib import Path

import duckdb

SILVER_PATH = "data/silver/swaps/*.parquet"

GOLD_SWAPS_PER_MINUTE = "data/gold/swaps_per_minute/swaps_per_minute.parquet"
GOLD_TOP_POOLS = "data/gold/top_pools/top_pools.parquet"
GOLD_PIPELINE_SUMMARY = "data/gold/pipeline_summary/pipeline_summary.parquet"
GOLD_RECENT_SWAPS = "data/gold/recent_swaps/recent_swaps.parquet"

for path in [
    GOLD_SWAPS_PER_MINUTE,
    GOLD_TOP_POOLS,
    GOLD_PIPELINE_SUMMARY,
    GOLD_RECENT_SWAPS,
]:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def write_parquet(query: str, output_path: str) -> None:
    duckdb.sql(
        f"""
        COPY (
            {query}
        )
        TO '{output_path}'
        (FORMAT PARQUET, OVERWRITE true)
        """
    )
    print(f"Wrote {output_path}")


write_parquet(
    f"""
    SELECT
        DATE_TRUNC('minute', kafka_timestamp) AS minute_ts,
        chain,
        COUNT(*) AS swap_count,
        COUNT(DISTINCT pool_address) AS unique_pools,
        MIN(block_number) AS min_block_number,
        MAX(block_number) AS max_block_number
    FROM read_parquet('{SILVER_PATH}')
    GROUP BY 1, 2
    ORDER BY minute_ts
    """,
    GOLD_SWAPS_PER_MINUTE,
)

write_parquet(
    f"""
    SELECT
        chain,
        pool_address,
        COUNT(*) AS swap_count,
        MIN(block_number) AS first_block_seen,
        MAX(block_number) AS latest_block_seen,
        MIN(kafka_timestamp) AS first_seen_at,
        MAX(kafka_timestamp) AS latest_seen_at
    FROM read_parquet('{SILVER_PATH}')
    GROUP BY 1, 2
    ORDER BY swap_count DESC
    LIMIT 50
    """,
    GOLD_TOP_POOLS,
)

write_parquet(
    f"""
    SELECT
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
    GROUP BY 1
    """,
    GOLD_PIPELINE_SUMMARY,
)

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

print("Gold analytics layer complete.")