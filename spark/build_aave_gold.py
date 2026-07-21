"""Build daily Aave V3 lending aggregates from normalized Silver events."""

import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from config.logging import configure_logging
from config.settings import (
    AAVE_BORROW_EVENTS_FILE,
    AAVE_DAILY_BORROW_ACTIVITY_FILE,
    AAVE_DAILY_LENDING_SUMMARY_FILE,
    AAVE_DAILY_LIQUIDATION_ACTIVITY_FILE,
    AAVE_DAILY_REPAYMENT_ACTIVITY_FILE,
    AAVE_LIQUIDATION_EVENTS_FILE,
    AAVE_REPAY_EVENTS_FILE,
)
from config.versions import GOLD_JOB_VERSION
from spark.parquet import write_relation_atomic

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AaveGoldPaths:
    """Configured Aave Silver inputs and Gold outputs."""

    borrow_input: Path = AAVE_BORROW_EVENTS_FILE
    repay_input: Path = AAVE_REPAY_EVENTS_FILE
    liquidation_input: Path = AAVE_LIQUIDATION_EVENTS_FILE
    daily_borrow_output: Path = AAVE_DAILY_BORROW_ACTIVITY_FILE
    daily_repayment_output: Path = AAVE_DAILY_REPAYMENT_ACTIVITY_FILE
    daily_liquidation_output: Path = AAVE_DAILY_LIQUIDATION_ACTIVITY_FILE
    daily_summary_output: Path = AAVE_DAILY_LENDING_SUMMARY_FILE

    @property
    def inputs(self) -> tuple[Path, ...]:
        return (
            self.borrow_input,
            self.repay_input,
            self.liquidation_input,
        )

    @property
    def outputs(self) -> tuple[Path, ...]:
        return (
            self.daily_borrow_output,
            self.daily_repayment_output,
            self.daily_liquidation_output,
            self.daily_summary_output,
        )


UPSTREAM_METADATA_SQL = """
            MAX(producer_version) AS producer_version,
            MAX(schema_version) AS schema_version,
            MAX(bronze_processed_at) AS bronze_processed_at,
            MAX(silver_processed_at) AS silver_processed_at,
            MAX(silver_job_version) AS silver_job_version,
"""

GOLD_METADATA_SQL = f"""
            CURRENT_TIMESTAMP AS gold_processed_at,
            '{GOLD_JOB_VERSION}' AS aggregation_version
"""


def daily_borrow_activity_query() -> str:
    """Aggregate deduplicated borrows by UTC date and reserve address."""
    return f"""
        SELECT
            gold_event_date AS event_date,
            protocol,
            chain,
            reserve AS reserve_address,
            COUNT(*) AS borrow_event_count,
            COUNT(DISTINCT user) AS unique_borrowers,
            SUM(CAST(amount_raw AS BIGNUM)) AS total_borrow_amount_raw,
            COUNT(*) FILTER (WHERE interest_rate_mode = 2)
                AS variable_rate_borrow_count,
            COUNT(*) FILTER (WHERE interest_rate_mode = 1)
                AS stable_rate_borrow_count,
            {UPSTREAM_METADATA_SQL}
            {GOLD_METADATA_SQL}
        FROM borrow_events
        GROUP BY 1, 2, 3, 4
        ORDER BY event_date, protocol, chain, reserve_address
    """


def daily_repayment_activity_query() -> str:
    """Aggregate deduplicated repayments by UTC date and reserve address."""
    return f"""
        SELECT
            gold_event_date AS event_date,
            protocol,
            chain,
            reserve AS reserve_address,
            COUNT(*) AS repay_event_count,
            COUNT(DISTINCT user) AS unique_borrowers_repaid,
            COUNT(DISTINCT repayer) AS unique_repayers,
            SUM(CAST(amount_raw AS BIGNUM)) AS total_repaid_amount_raw,
            COUNT(*) FILTER (WHERE use_atokens IS TRUE) AS atoken_repay_count,
            {UPSTREAM_METADATA_SQL}
            {GOLD_METADATA_SQL}
        FROM repay_events
        GROUP BY 1, 2, 3, 4
        ORDER BY event_date, protocol, chain, reserve_address
    """


def daily_liquidation_activity_query() -> str:
    """Aggregate liquidations without combining different asset pairs."""
    return f"""
        SELECT
            gold_event_date AS event_date,
            protocol,
            chain,
            collateral_asset,
            debt_asset,
            COUNT(*) AS liquidation_count,
            COUNT(DISTINCT user) AS unique_liquidated_users,
            COUNT(DISTINCT liquidator) AS unique_liquidators,
            SUM(CAST(debt_to_cover_raw AS BIGNUM))
                AS total_debt_covered_raw,
            SUM(CAST(liquidated_collateral_amount_raw AS BIGNUM))
                AS total_collateral_liquidated_raw,
            COUNT(*) FILTER (WHERE receive_atoken IS TRUE)
                AS receive_atoken_count,
            {UPSTREAM_METADATA_SQL}
            {GOLD_METADATA_SQL}
        FROM liquidation_events
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY event_date, protocol, chain, collateral_asset, debt_asset
    """


def daily_lending_summary_query() -> str:
    """Build cross-asset daily counts without summing raw token amounts."""
    return f"""
        SELECT
            event_date,
            protocol,
            chain,
            SUM(borrow_event_count) AS borrow_event_count,
            SUM(repay_event_count) AS repay_event_count,
            SUM(liquidation_count) AS liquidation_count,
            SUM(unique_borrowers) AS unique_borrowers,
            SUM(unique_repayers) AS unique_repayers,
            SUM(unique_liquidated_users) AS unique_liquidated_users,
            SUM(unique_liquidators) AS unique_liquidators,
            MAX(producer_version) AS producer_version,
            MAX(schema_version) AS schema_version,
            MAX(bronze_processed_at) AS bronze_processed_at,
            MAX(silver_processed_at) AS silver_processed_at,
            MAX(silver_job_version) AS silver_job_version,
            {GOLD_METADATA_SQL}
        FROM (
            SELECT
                gold_event_date AS event_date,
                protocol,
                chain,
                COUNT(*) AS borrow_event_count,
                0::BIGINT AS repay_event_count,
                0::BIGINT AS liquidation_count,
                COUNT(DISTINCT user) AS unique_borrowers,
                0::BIGINT AS unique_repayers,
                0::BIGINT AS unique_liquidated_users,
                0::BIGINT AS unique_liquidators,
                MAX(producer_version) AS producer_version,
                MAX(schema_version) AS schema_version,
                MAX(bronze_processed_at) AS bronze_processed_at,
                MAX(silver_processed_at) AS silver_processed_at,
                MAX(silver_job_version) AS silver_job_version
            FROM borrow_events
            GROUP BY 1, 2, 3

            UNION ALL

            SELECT
                gold_event_date,
                protocol,
                chain,
                0::BIGINT,
                COUNT(*),
                0::BIGINT,
                0::BIGINT,
                COUNT(DISTINCT repayer),
                0::BIGINT,
                0::BIGINT,
                MAX(producer_version),
                MAX(schema_version),
                MAX(bronze_processed_at),
                MAX(silver_processed_at),
                MAX(silver_job_version)
            FROM repay_events
            GROUP BY 1, 2, 3

            UNION ALL

            SELECT
                gold_event_date,
                protocol,
                chain,
                0::BIGINT,
                0::BIGINT,
                COUNT(*),
                0::BIGINT,
                0::BIGINT,
                COUNT(DISTINCT user),
                COUNT(DISTINCT liquidator),
                MAX(producer_version),
                MAX(schema_version),
                MAX(bronze_processed_at),
                MAX(silver_processed_at),
                MAX(silver_job_version)
            FROM liquidation_events
            GROUP BY 1, 2, 3
        ) daily_metrics
        GROUP BY 1, 2, 3
        ORDER BY event_date, protocol, chain
    """


def _validate_inputs(paths: AaveGoldPaths) -> bool:
    existing = [path.exists() for path in paths.inputs]
    if not any(existing):
        if any(path.exists() for path in paths.outputs):
            raise FileNotFoundError(
                "Aave Gold outputs exist but all required Silver inputs are "
                "missing; refusing to leave stale aggregates"
            )
        logger.info("No Aave Silver inputs found; Aave Gold outputs are unchanged")
        return False
    if not all(existing):
        missing = ", ".join(
            str(path) for path, exists in zip(paths.inputs, existing) if not exists
        )
        raise FileNotFoundError(
            f"Cannot build partial Aave Gold datasets; missing inputs: {missing}"
        )
    return True


def _create_deduplicated_view(
    connection: duckdb.DuckDBPyConnection,
    view_name: str,
    input_path: Path,
) -> None:
    raw_view = f"{view_name}_raw"
    connection.read_parquet(str(input_path)).create_view(raw_view)
    connection.sql(
        f"""
        CREATE TEMP VIEW {view_name} AS
        SELECT
            *,
            CAST(block_timestamp AS DATE) AS gold_event_date
        FROM {raw_view}
        WHERE protocol = 'aave_v3'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY protocol, chain, transaction_hash, log_index
            ORDER BY kafka_timestamp DESC,
                ingested_at DESC,
                silver_processed_at DESC,
                bronze_file DESC
        ) = 1
        """
    )


def _write_atomic(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    output_path: Path,
) -> int:
    row_count = write_relation_atomic(
        connection,
        connection.sql(query),
        output_path,
    )

    logger.info("Wrote %s rows to: %s", row_count, output_path)
    return row_count


def build_aave_gold(paths: AaveGoldPaths = AaveGoldPaths()) -> dict[str, int]:
    """Build all Aave Gold datasets, rejecting partial Silver inputs."""
    if not _validate_inputs(paths):
        return {}

    connection = duckdb.connect()
    connection.sql("SET TimeZone = 'UTC'")
    try:
        _create_deduplicated_view(
            connection,
            "borrow_events",
            paths.borrow_input,
        )
        _create_deduplicated_view(
            connection,
            "repay_events",
            paths.repay_input,
        )
        _create_deduplicated_view(
            connection,
            "liquidation_events",
            paths.liquidation_input,
        )

        return {
            "daily_borrow_activity": _write_atomic(
                connection,
                daily_borrow_activity_query(),
                paths.daily_borrow_output,
            ),
            "daily_repayment_activity": _write_atomic(
                connection,
                daily_repayment_activity_query(),
                paths.daily_repayment_output,
            ),
            "daily_liquidation_activity": _write_atomic(
                connection,
                daily_liquidation_activity_query(),
                paths.daily_liquidation_output,
            ),
            "daily_lending_summary": _write_atomic(
                connection,
                daily_lending_summary_query(),
                paths.daily_summary_output,
            ),
        }
    finally:
        connection.close()


def main() -> None:
    configure_logging()
    build_aave_gold()


if __name__ == "__main__":
    main()
