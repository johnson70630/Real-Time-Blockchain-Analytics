import logging

import duckdb

from config.logging import configure_logging
from config.settings import (
    AAVE_DAILY_BORROW_ACTIVITY_FILE,
    AAVE_DAILY_LENDING_SUMMARY_FILE,
    AAVE_DAILY_LIQUIDATION_ACTIVITY_FILE,
    AAVE_DAILY_REPAYMENT_ACTIVITY_FILE,
    GOLD_PIPELINE_SUMMARY_GLOB,
    SILVER_PARQUET_GLOB,
)

logger = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    """Raised when an operational data-quality invariant fails."""


def run_check(name: str, query: str, expected_value: int | None = None) -> None:
    result = duckdb.sql(query).fetchone()[0]

    if expected_value is not None:
        if result != expected_value:
            raise DataQualityError(
                f"{name} failed: got {result}, expected {expected_value}"
            )
    elif result <= 0:
        raise DataQualityError(f"{name} failed: got {result}")

    logger.info("Passed: %s", name)


def run_aave_gold_checks() -> None:
    """Validate Aave Gold grains, metrics, metadata, and reconciliation."""
    outputs = (
        AAVE_DAILY_BORROW_ACTIVITY_FILE,
        AAVE_DAILY_REPAYMENT_ACTIVITY_FILE,
        AAVE_DAILY_LIQUIDATION_ACTIVITY_FILE,
        AAVE_DAILY_LENDING_SUMMARY_FILE,
    )
    existing = [path.exists() for path in outputs]
    if not any(existing):
        logger.info("Aave Gold outputs not found; skipping Aave quality checks")
        return
    if not all(existing):
        raise DataQualityError(
            "Aave Gold quality checks require all four outputs"
        )

    checks = (
        (
            "Aave daily borrow grain and required fields",
            AAVE_DAILY_BORROW_ACTIVITY_FILE,
            "event_date, protocol, chain, reserve_address",
            "reserve_address IS NULL OR borrow_event_count < 0",
        ),
        (
            "Aave daily repayment grain and required fields",
            AAVE_DAILY_REPAYMENT_ACTIVITY_FILE,
            "event_date, protocol, chain, reserve_address",
            "reserve_address IS NULL OR repay_event_count < 0",
        ),
        (
            "Aave daily liquidation grain and required fields",
            AAVE_DAILY_LIQUIDATION_ACTIVITY_FILE,
            "event_date, protocol, chain, collateral_asset, debt_asset",
            "collateral_asset IS NULL OR debt_asset IS NULL "
            "OR liquidation_count < 0",
        ),
        (
            "Aave daily lending summary grain and required fields",
            AAVE_DAILY_LENDING_SUMMARY_FILE,
            "event_date, protocol, chain",
            "borrow_event_count < 0 OR repay_event_count < 0 "
            "OR liquidation_count < 0",
        ),
    )
    for name, path, grain, invalid_metric in checks:
        run_check(
            name,
            f"""
            SELECT
                (COUNT(*) - COUNT(DISTINCT ({grain})))
                + COUNT(*) FILTER (
                    WHERE event_date IS NULL
                        OR protocol IS NULL
                        OR protocol <> 'aave_v3'
                        OR chain IS NULL
                        OR gold_processed_at IS NULL
                        OR aggregation_version IS NULL
                        OR {invalid_metric}
                )
            FROM read_parquet('{path}')
            """,
            expected_value=0,
        )

    run_check(
        "Aave asset activity reconciles to daily summary",
        f"""
        WITH activity AS (
            SELECT
                event_date,
                protocol,
                chain,
                SUM(borrow_event_count) AS borrow_event_count,
                0::BIGINT AS repay_event_count,
                0::BIGINT AS liquidation_count
            FROM read_parquet('{AAVE_DAILY_BORROW_ACTIVITY_FILE}')
            GROUP BY 1, 2, 3
            UNION ALL
            SELECT
                event_date, protocol, chain, 0::BIGINT,
                SUM(repay_event_count), 0::BIGINT
            FROM read_parquet('{AAVE_DAILY_REPAYMENT_ACTIVITY_FILE}')
            GROUP BY 1, 2, 3
            UNION ALL
            SELECT
                event_date, protocol, chain, 0::BIGINT, 0::BIGINT,
                SUM(liquidation_count)
            FROM read_parquet('{AAVE_DAILY_LIQUIDATION_ACTIVITY_FILE}')
            GROUP BY 1, 2, 3
        ), totals AS (
            SELECT
                event_date,
                protocol,
                chain,
                SUM(borrow_event_count) AS borrow_event_count,
                SUM(repay_event_count) AS repay_event_count,
                SUM(liquidation_count) AS liquidation_count
            FROM activity
            GROUP BY 1, 2, 3
        )
        SELECT COUNT(*)
        FROM totals
        FULL OUTER JOIN read_parquet(
            '{AAVE_DAILY_LENDING_SUMMARY_FILE}'
        ) summary USING (event_date, protocol, chain)
        WHERE totals.borrow_event_count IS DISTINCT FROM
                summary.borrow_event_count
            OR totals.repay_event_count IS DISTINCT FROM
                summary.repay_event_count
            OR totals.liquidation_count IS DISTINCT FROM
                summary.liquidation_count
        """,
        expected_value=0,
    )


def main() -> None:
    configure_logging()
    run_check(
        "Silver event_id uniqueness",
        f"""
        SELECT COUNT(*) - COUNT(DISTINCT event_id)
        FROM read_parquet('{SILVER_PARQUET_GLOB}')
        """,
        expected_value=0,
    )

    run_check(
        "Silver required fields not null",
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{SILVER_PARQUET_GLOB}')
        WHERE event_id IS NULL
            OR protocol IS NULL
            OR chain IS NULL
            OR event_date IS NULL
            OR transaction_hash IS NULL
            OR pool_address IS NULL
            OR block_number IS NULL
            OR log_index IS NULL
            OR producer_version IS NULL
            OR schema_version IS NULL
            OR ingested_at IS NULL
            OR bronze_processed_at IS NULL
            OR bronze_file IS NULL
            OR silver_processed_at IS NULL
            OR silver_job_version IS NULL
        """,
        expected_value=0,
    )

    run_check(
        "Gold operational metadata not null",
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{GOLD_PIPELINE_SUMMARY_GLOB}')
        WHERE producer_version IS NULL
            OR schema_version IS NULL
            OR bronze_processed_at IS NULL
            OR silver_processed_at IS NULL
            OR silver_job_version IS NULL
            OR gold_processed_at IS NULL
            OR aggregation_version IS NULL
        """,
        expected_value=0,
    )

    run_check(
        "Gold summary has swaps",
        f"""
        SELECT total_swaps
        FROM read_parquet('{GOLD_PIPELINE_SUMMARY_GLOB}')
        """,
    )

    run_aave_gold_checks()

    logger.info("All data quality checks passed")


if __name__ == "__main__":
    main()
