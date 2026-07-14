import logging

import duckdb

from config.settings import BRONZE_PARQUET_GLOB, SILVER_DIR, SILVER_OUTPUT_FILE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def build_silver_swaps() -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    query = f"""
        COPY (
            SELECT
                chain || '-' || transaction_hash || '-'
                    || CAST(log_index AS VARCHAR) AS event_id,
                protocol,
                chain,
                event_date,
                event_type,
                block_number,
                transaction_hash,
                pool_address,
                log_index,
                raw_data,
                raw_topics,
                kafka_timestamp,
                ingested_at
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY chain, transaction_hash, log_index
                        ORDER BY kafka_timestamp DESC, ingested_at DESC
                    ) AS row_num
                FROM read_parquet(
                    '{BRONZE_PARQUET_GLOB}',
                    hive_partitioning = true
                )
                WHERE protocol IS NOT NULL
                  AND chain IS NOT NULL
                  AND event_date IS NOT NULL
                  AND transaction_hash IS NOT NULL
                  AND pool_address IS NOT NULL
                  AND block_number IS NOT NULL
                  AND log_index IS NOT NULL
            )
            WHERE row_num = 1
        )
        TO '{SILVER_OUTPUT_FILE}'
        (FORMAT PARQUET, OVERWRITE true)
    """

    duckdb.sql(query)
    logger.info("Silver swaps written to: %s", SILVER_OUTPUT_FILE)


def main() -> None:
    build_silver_swaps()


if __name__ == "__main__":
    main()
