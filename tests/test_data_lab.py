"""Focused tests for the DeFi Data Lab foundation."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from app.components.swap_lesson import (
    display_usd,
    normalization_equation,
    price_status_explanation,
)
from app.config import AppConfig
from app.data.snowflake import SnowflakeTutorialRepository


class FakeCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.query = ""
        self.params: dict[str, Any] = {}
        self.executions: list[tuple[str, dict[str, Any]]] = []
        self.closed = False

    def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> FakeCursor:
        self.query = query
        self.params = params or {}
        self.executions.append((self.query, self.params))
        return self

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.fake_cursor = cursor
        self.closed = False

    def cursor(self, cursor_class: Any = None) -> FakeCursor:
        return self.fake_cursor

    def close(self) -> None:
        self.closed = True


def test_app_config_uses_read_only_marts_defaults() -> None:
    config = AppConfig.from_env({})

    assert config.connection_name == "blockchain-dev"
    assert config.role == "BLOCKCHAIN_ANALYST_ROLE"
    assert config.schema == "MARTS"
    assert config.tutorial_mart.endswith("MARTS.MART_UNISWAP_SWAP_TUTORIAL")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"SNOWFLAKE_APP_ROLE": "ACCOUNTADMIN"}, "BLOCKCHAIN_ANALYST_ROLE"),
        ({"SNOWFLAKE_APP_SCHEMA": "RAW"}, "must be MARTS"),
    ],
)
def test_app_config_rejects_write_roles_and_non_marts_schemas(
    overrides: dict[str, str], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        AppConfig.from_env(overrides)


def test_swap_list_query_is_parameterized_and_marts_only() -> None:
    cursor = FakeCursor(
        [
            {
                "EVENT_ID": "event-1",
                "POOL_PAIR": "WETH / USDC",
                "BLOCK_TIMESTAMP": datetime(2026, 1, 1, tzinfo=UTC),
                "LOG_INDEX": 42,
            }
        ]
    )
    connection = FakeConnection(cursor)
    calls: list[dict[str, Any]] = []

    def factory(**kwargs: Any) -> FakeConnection:
        calls.append(kwargs)
        return connection

    repository = SnowflakeTutorialRepository(AppConfig(), factory)
    rows = repository.list_swaps(pool_pair="WETH / USDC", limit=25)

    assert rows[0]["event_id"] == "event-1"
    assert "BLOCKCHAIN_ANALYTICS.MARTS" in cursor.query
    assert "RAW" not in cursor.query
    assert "POOL_PAIR = %(pool_pair)s" in cursor.query
    assert cursor.params == {"limit": 25, "pool_pair": "WETH / USDC"}
    assert calls[0]["role"] == "BLOCKCHAIN_ANALYST_ROLE"
    assert cursor.executions[0][0] == "USE SECONDARY ROLES NONE"
    assert cursor.closed and connection.closed


def test_selected_swap_retrieval_uses_event_id_parameter() -> None:
    cursor = FakeCursor([{"EVENT_ID": "event-1", "AMOUNT0_USD": None}])
    repository = SnowflakeTutorialRepository(
        AppConfig(), lambda **kwargs: FakeConnection(cursor)
    )

    result = repository.get_swap("event-1")

    assert result == {"event_id": "event-1", "amount0_usd": None}
    assert cursor.params == {"event_id": "event-1"}
    assert "WHERE EVENT_ID = %(event_id)s" in cursor.query


def test_unknown_usd_value_is_not_displayed_as_zero() -> None:
    assert display_usd(None) == "Unavailable"
    assert display_usd(Decimal("0")) == "$0.00000000"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("stale", "exceeded"),
        ("unmapped", "no approved"),
        ("no_prior_price", "No Chainlink observation"),
    ],
)
def test_unavailable_price_statuses_have_specific_explanations(
    status: str, expected: str
) -> None:
    assert expected in price_status_explanation(status)


def test_normalization_equation_uses_supplied_mart_value() -> None:
    equation = normalization_equation("1234567", 6, Decimal("1.234567"))

    assert equation == "1234567 ÷ 10^6 = 1.234567"


def test_application_modules_import_without_connecting_to_snowflake() -> None:
    from app.app import main
    from app.pages.uniswap import render_uniswap

    assert callable(main)
    assert callable(render_uniswap)
