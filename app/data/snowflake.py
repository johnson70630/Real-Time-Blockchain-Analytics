"""Centralized Snowflake MARTS queries for the tutorial application."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import closing
from typing import Any, Protocol

import snowflake.connector
from snowflake.connector import DictCursor

from app.config import AppConfig


class Cursor(Protocol):
    description: Any

    def execute(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> Cursor: ...

    def fetchall(self) -> list[Any]: ...

    def fetchone(self) -> Any: ...

    def close(self) -> None: ...


class Connection(Protocol):
    def cursor(self, cursor_class: Any = None) -> Cursor: ...

    def close(self) -> None: ...


ConnectionFactory = Callable[..., Connection]

_SWAP_LIST_COLUMNS = """
    EVENT_ID,
    POOL_PAIR,
    BLOCK_TIMESTAMP,
    TRANSACTION_HASH,
    LOG_INDEX,
    POOL_ADDRESS,
    PRICING_COVERAGE_STATUS
"""

_SWAP_TUTORIAL_COLUMNS = """
    EVENT_ID,
    BLOCK_TIMESTAMP,
    BLOCK_NUMBER,
    TRANSACTION_HASH,
    LOG_INDEX,
    POOL_ADDRESS,
    POOL_PAIR,
    TOKEN0_ADDRESS,
    TOKEN0_SYMBOL,
    TOKEN0_NAME,
    TOKEN0_DECIMALS,
    AMOUNT0_RAW,
    AMOUNT0,
    TOKEN0_PRICE_USD,
    AMOUNT0_USD,
    TOKEN0_PRICE_FEED_UPDATED_AT,
    TOKEN0_PRICE_AGE_SECONDS,
    TOKEN0_PRICE_STATUS,
    TOKEN0_PRICE_STATUS_REASON,
    TOKEN1_ADDRESS,
    TOKEN1_SYMBOL,
    TOKEN1_NAME,
    TOKEN1_DECIMALS,
    AMOUNT1_RAW,
    AMOUNT1,
    TOKEN1_PRICE_USD,
    AMOUNT1_USD,
    TOKEN1_PRICE_FEED_UPDATED_AT,
    TOKEN1_PRICE_AGE_SECONDS,
    TOKEN1_PRICE_STATUS,
    TOKEN1_PRICE_STATUS_REASON,
    PRICING_COVERAGE_STATUS
"""

_AAVE_ACTIVITY_TYPES = frozenset({"BORROW", "REPAY"})

_AAVE_LIST_COLUMNS = """
    EVENT_ID,
    ACTIVITY_TYPE,
    BLOCK_TIMESTAMP,
    TRANSACTION_HASH,
    LOG_INDEX,
    TOKEN_SYMBOL,
    TOKEN_NAME,
    TOKEN_ADDRESS,
    PRICE_STATUS
"""

_AAVE_TUTORIAL_COLUMNS = """
    EVENT_ID,
    ACTIVITY_TYPE,
    BLOCK_TIMESTAMP,
    BLOCK_NUMBER,
    TRANSACTION_HASH,
    LOG_INDEX,
    TOKEN_ADDRESS,
    TOKEN_SYMBOL,
    TOKEN_NAME,
    TOKEN_DECIMALS,
    AMOUNT_RAW,
    AMOUNT,
    PRICE_USD,
    AMOUNT_USD,
    PRICE_FEED_UPDATED_AT,
    PRICE_AGE_SECONDS,
    PRICE_STATUS,
    PRICE_STATUS_REASON,
    USER,
    ON_BEHALF_OF,
    INTEREST_RATE_MODE,
    BORROW_RATE_RAW,
    REFERRAL_CODE,
    REPAYER,
    USE_ATOKENS
"""


def _lowercase_keys(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key).lower(): value for key, value in row.items()}


class SnowflakeTutorialRepository:
    """Read trusted tutorial rows through the analyst role only."""

    def __init__(
        self,
        config: AppConfig,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        self.config = config
        self._connection_factory = (
            connection_factory or snowflake.connector.connect
        )

    def _connect(self) -> Connection:
        connection = self._connection_factory(
            connection_name=self.config.connection_name,
            role=self.config.role,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=self.config.schema,
        )
        try:
            # Named connections can activate every role granted to a user.
            # Disable them so the app is constrained to its analyst role.
            with closing(connection.cursor()) as cursor:
                cursor.execute("USE SECONDARY ROLES NONE")
        except Exception:
            connection.close()
            raise
        return connection

    def list_pool_pairs(self) -> list[str]:
        """Return pool-pair labels represented in the tutorial mart."""
        query = f"""
            SELECT DISTINCT POOL_PAIR
            FROM {self.config.tutorial_mart}
            WHERE POOL_PAIR IS NOT NULL
            ORDER BY POOL_PAIR
        """
        rows = self._fetch_all(query)
        return [str(row["pool_pair"]) for row in rows]

    def list_swaps(
        self,
        *,
        pool_pair: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """List recent canonical swaps, optionally filtered by pool pair."""
        if not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1000")

        where_clause = ""
        params: dict[str, Any] = {"limit": limit}
        if pool_pair is not None:
            where_clause = "WHERE POOL_PAIR = %(pool_pair)s"
            params["pool_pair"] = pool_pair

        query = f"""
            SELECT {_SWAP_LIST_COLUMNS}
            FROM {self.config.tutorial_mart}
            {where_clause}
            ORDER BY BLOCK_TIMESTAMP DESC, EVENT_ID
            LIMIT %(limit)s
        """
        return self._fetch_all(query, params)

    def get_swap(self, event_id: str) -> dict[str, Any] | None:
        """Retrieve one tutorial-ready swap by its immutable event ID."""
        if not event_id.strip():
            raise ValueError("event_id must not be empty")

        query = f"""
            SELECT {_SWAP_TUTORIAL_COLUMNS}
            FROM {self.config.tutorial_mart}
            WHERE EVENT_ID = %(event_id)s
        """
        rows = self._fetch_all(query, {"event_id": event_id})
        if len(rows) > 1:
            raise RuntimeError(f"Duplicate tutorial event_id: {event_id}")
        return rows[0] if rows else None

    def list_aave_tokens(self, activity_type: str) -> list[str]:
        """Return token symbols represented for one lending activity type."""
        activity_type = self._aave_activity_type(activity_type)
        query = f"""
            SELECT DISTINCT TOKEN_SYMBOL
            FROM {self.config.aave_lending_mart}
            WHERE ACTIVITY_TYPE = %(activity_type)s
                AND TOKEN_SYMBOL IS NOT NULL
            ORDER BY TOKEN_SYMBOL
        """
        rows = self._fetch_all(query, {"activity_type": activity_type})
        return [str(row["token_symbol"]) for row in rows]

    def list_aave_activities(
        self,
        activity_type: str,
        *,
        token_symbol: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """List Borrow or Repay events, optionally filtered by token."""
        activity_type = self._aave_activity_type(activity_type)
        if not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1000")

        token_filter = ""
        params: dict[str, Any] = {
            "activity_type": activity_type,
            "limit": limit,
        }
        if token_symbol is not None:
            token_filter = "AND TOKEN_SYMBOL = %(token_symbol)s"
            params["token_symbol"] = token_symbol

        query = f"""
            SELECT {_AAVE_LIST_COLUMNS}
            FROM {self.config.aave_lending_mart}
            WHERE ACTIVITY_TYPE = %(activity_type)s
                {token_filter}
            ORDER BY BLOCK_TIMESTAMP DESC, EVENT_ID
            LIMIT %(limit)s
        """
        return self._fetch_all(query, params)

    def get_aave_activity(self, event_id: str) -> dict[str, Any] | None:
        """Retrieve one tutorial-ready Aave event by immutable event ID."""
        if not event_id.strip():
            raise ValueError("event_id must not be empty")

        query = f"""
            SELECT {_AAVE_TUTORIAL_COLUMNS}
            FROM {self.config.aave_lending_mart}
            WHERE EVENT_ID = %(event_id)s
        """
        rows = self._fetch_all(query, {"event_id": event_id})
        if len(rows) > 1:
            raise RuntimeError(f"Duplicate tutorial event_id: {event_id}")
        return rows[0] if rows else None

    @staticmethod
    def _aave_activity_type(activity_type: str) -> str:
        normalized = activity_type.strip().upper()
        if normalized not in _AAVE_ACTIVITY_TYPES:
            raise ValueError("activity_type must be BORROW or REPAY")
        return normalized

    def _fetch_all(
        self,
        query: str,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            with closing(connection.cursor(DictCursor)) as cursor:
                cursor.execute(query, params)
                return [_lowercase_keys(row) for row in cursor.fetchall()]
