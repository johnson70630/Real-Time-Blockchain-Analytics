import logging
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

BRONZE_PATH = "data/bronze/swaps/*.parquet"
SILVER_DIR = "data/silver/swaps"
SILVER_FILE = f"{SILVER_DIR}/swaps_silver.parquet"


def build_silver_swaps() -> None:
    Path(SILVER_DIR).mkdir(parents=True, exist_ok=True)

    query = f"""
        COPY (
            SELECT
                transaction_hash || '-' || CAST(log_index AS VARCHAR) AS event_id,
                chain,
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
                        PARTITION BY transaction_hash, log_index
                        ORDER BY kafka_timestamp DESC, ingested_at DESC
                    ) AS row_num
                FROM read_parquet('{BRONZE_PATH}')
                WHERE transaction_hash IS NOT NULL
                  AND pool_address IS NOT NULL
                  AND block_number IS NOT NULL
                  AND log_index IS NOT NULL
            )
            WHERE row_num = 1
        )
        TO '{SILVER_FILE}'
        (FORMAT PARQUET, OVERWRITE true)
    """

    duckdb.sql(query)
    logger.info("Silver swaps written to: %s", SILVER_FILE)


def main() -> None:
    build_silver_swaps()


if __name__ == "__main__":
    main()