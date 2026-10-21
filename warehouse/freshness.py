"""Read-only Snowflake queries used by freshness checks."""

from __future__ import annotations

from contextlib import closing
from datetime import datetime
from typing import Any

import snowflake.connector

from warehouse.load_raw import (
    DEFAULT_CONNECTION_NAME,
    DEFAULT_WAREHOUSE,
    validate_key_pair_connection_config,
)

ANALYST_ROLE = "BLOCKCHAIN_ANALYST_ROLE"
DBT_ROLE = "BLOCKCHAIN_DBT_ROLE"
PIPELINE_ROLE = "BLOCKCHAIN_PIPELINE_ROLE"


def connect_read_only(
    role: str,
    *,
    connection_name: str = DEFAULT_CONNECTION_NAME,
    warehouse: str = DEFAULT_WAREHOUSE,
) -> Any:
    """Open a key-pair session with secondary roles explicitly disabled."""
    if role not in {ANALYST_ROLE, DBT_ROLE, PIPELINE_ROLE}:
        raise ValueError("Freshness checks require an approved read role")
    validate_key_pair_connection_config(connection_name)
    connection = snowflake.connector.connect(
        connection_name=connection_name,
        role=role,
        warehouse=warehouse,
        application="BLOCKCHAIN_FRESHNESS_CHECKS",
    )
    try:
        with closing(connection.cursor()) as cursor:
            cursor.execute("USE SECONDARY ROLES NONE")
            cursor.execute("SELECT CURRENT_ROLE()")
            effective_role = str(cursor.fetchone()[0]).upper()
        if effective_role != role:
            raise RuntimeError(f"Expected Snowflake role {role}, found {effective_role}")
    except Exception:
        connection.close()
        raise
    return connection


class FreshnessWarehouseReader:
    """Query control, RAW, and MART metadata without changing any object."""

    def __init__(self, pipeline_connection: Any, analyst_connection: Any) -> None:
        self.pipeline_connection = pipeline_connection
        self.analyst_connection = analyst_connection

    @staticmethod
    def _one(connection: Any, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        with closing(connection.cursor()) as cursor:
            cursor.execute(sql, parameters)
            row = cursor.fetchone()
        return row[0] if row else None

    def latest_event_timestamps(self) -> dict[str, datetime | None]:
        """Return latest retained business event times from canonical RAW."""
        sql = """
            SELECT
              (SELECT MAX(block_timestamp) FROM BLOCKCHAIN_ANALYTICS.RAW.UNISWAP_SWAPS),
              GREATEST_IGNORE_NULLS(
                (SELECT MAX(block_timestamp) FROM BLOCKCHAIN_ANALYTICS.RAW.AAVE_BORROWS),
                (SELECT MAX(block_timestamp) FROM BLOCKCHAIN_ANALYTICS.RAW.AAVE_REPAYS),
                (SELECT MAX(block_timestamp) FROM BLOCKCHAIN_ANALYTICS.RAW.AAVE_LIQUIDATIONS)
              )
        """
        with closing(self.pipeline_connection.cursor()) as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
        return {"uniswap_forward": row[0], "aave_forward": row[1]}

    def chainlink_feed_times(self) -> dict[str, datetime]:
        """Return one latest effective observation time per configured pair."""
        sql = """
            SELECT base_asset || '/' || quote_asset AS feed_name,
                   MAX(feed_updated_at)
            FROM BLOCKCHAIN_ANALYTICS.RAW.CHAINLINK_PRICES
            GROUP BY base_asset, quote_asset
        """
        with closing(self.pipeline_connection.cursor()) as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
        return {str(name).upper(): timestamp for name, timestamp in rows}

    def latest_warehouse_refresh(self) -> datetime | None:
        """Return the last successful terminal daily warehouse completion."""
        sql = """
            SELECT MAX(finished_at)
            FROM BLOCKCHAIN_ANALYTICS.CONTROL.PIPELINE_RUNS
            WHERE pipeline_name = 'daily_warehouse_pipeline'
              AND status IN ('SUCCEEDED', 'NO_OP')
        """
        return self._one(self.pipeline_connection, sql)

    def mart_represented_times(self) -> dict[str, datetime | None]:
        """Return latest business time represented by each approved MART."""
        sql = """
            SELECT
              (SELECT MAX(block_timestamp) FROM BLOCKCHAIN_ANALYTICS.MARTS.MART_UNISWAP_SWAP_TUTORIAL),
              (SELECT MAX(block_timestamp) FROM BLOCKCHAIN_ANALYTICS.MARTS.MART_AAVE_LENDING_TUTORIAL),
              (SELECT MAX(feed_updated_at) FROM BLOCKCHAIN_ANALYTICS.MARTS.MART_CHAINLINK_ORACLE_TUTORIAL),
              (SELECT MAX(event_timestamp) FROM BLOCKCHAIN_ANALYTICS.MARTS.MART_POINT_IN_TIME_PRICING_TUTORIAL)
        """
        with closing(self.analyst_connection.cursor()) as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
        return {
            "mart_uniswap_swap_tutorial": row[0],
            "mart_aave_lending_tutorial": row[1],
            "mart_chainlink_oracle_tutorial": row[2],
            "mart_point_in_time_pricing_tutorial": row[3],
        }
