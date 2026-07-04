import logging

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

SILVER_PATH = "data/silver/swaps/*.parquet"
GOLD_SUMMARY_PATH = "data/gold/pipeline_summary/*.parquet"


def run_check(name: str, query: str, expected_value: int | None = None) -> None:
    result = duckdb.sql(query).fetchone()[0]

    if expected_value is not None:
        assert result == expected_value, (
            f"{name} failed: got {result}, expected {expected_value}"
        )
    else:
        assert result > 0, f"{name} failed: got {result}"

    logger.info("Passed: %s", name)


def main() -> None:
    run_check(
        "Silver event_id uniqueness",
        f"""
        SELECT COUNT(*) - COUNT(DISTINCT event_id)
        FROM read_parquet('{SILVER_PATH}')
        """,
        expected_value=0,
    )

    run_check(
        "Silver required fields not null",
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{SILVER_PATH}')
        WHERE event_id IS NULL
           OR transaction_hash IS NULL
           OR pool_address IS NULL
           OR block_number IS NULL
           OR log_index IS NULL
        """,
        expected_value=0,
    )

    run_check(
        "Gold summary has swaps",
        f"""
        SELECT total_swaps
        FROM read_parquet('{GOLD_SUMMARY_PATH}')
        """,
    )

    logger.info("All data quality checks passed")


if __name__ == "__main__":
    main()