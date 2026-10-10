"""Focused tests for the Aave lending lesson."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from app.components.aave_lesson import (
    actor_roles,
    valid_amount_usd_display,
    valid_price_display,
)
from app.config import AppConfig
from app.data.snowflake import SnowflakeTutorialRepository
from app.pages.aave import aave_activity_label


class FakeCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.query = ""
        self.params: dict[str, Any] = {}

    def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> FakeCursor:
        self.query = query
        self.params = params or {}
        return self

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.fake_cursor = cursor

    def cursor(self, cursor_class: Any = None) -> FakeCursor:
        return self.fake_cursor

    def close(self) -> None:
        pass


def repository_with_rows(
    rows: list[dict[str, Any]],
) -> tuple[SnowflakeTutorialRepository, FakeCursor]:
    cursor = FakeCursor(rows)
    repository = SnowflakeTutorialRepository(
        AppConfig(), lambda **kwargs: FakeConnection(cursor)
    )
    return repository, cursor


@pytest.mark.parametrize("activity_type", ["BORROW", "REPAY"])
def test_aave_event_list_filters_activity_type(activity_type: str) -> None:
    repository, cursor = repository_with_rows([{"EVENT_ID": "event-1"}])

    rows = repository.list_aave_activities(activity_type)

    assert rows == [{"event_id": "event-1"}]
    assert "MART_AAVE_LENDING_TUTORIAL" in cursor.query
    assert "CORE" not in cursor.query
    assert cursor.params == {"activity_type": activity_type, "limit": 500}


def test_aave_event_list_filters_token_with_parameter() -> None:
    repository, cursor = repository_with_rows([])

    repository.list_aave_activities("BORROW", token_symbol="USDC")

    assert "TOKEN_SYMBOL = %(token_symbol)s" in cursor.query
    assert cursor.params["token_symbol"] == "USDC"


def test_aave_token_list_is_scoped_to_activity_type() -> None:
    repository, cursor = repository_with_rows(
        [{"TOKEN_SYMBOL": "USDC"}, {"TOKEN_SYMBOL": "WETH"}]
    )

    tokens = repository.list_aave_tokens("REPAY")

    assert tokens == ["USDC", "WETH"]
    assert cursor.params == {"activity_type": "REPAY"}


def test_aave_event_retrieval_uses_event_id_parameter() -> None:
    repository, cursor = repository_with_rows(
        [{"EVENT_ID": "event-1", "PRICE_USD": None}]
    )

    event = repository.get_aave_activity("event-1")

    assert event == {"event_id": "event-1", "price_usd": None}
    assert cursor.params == {"event_id": "event-1"}
    assert "WHERE EVENT_ID = %(event_id)s" in cursor.query


def test_aave_activity_type_validation_rejects_other_values() -> None:
    repository, _ = repository_with_rows([])

    with pytest.raises(ValueError, match="BORROW or REPAY"):
        repository.list_aave_activities("LIQUIDATION")


def test_aave_selector_label_distinguishes_log_index() -> None:
    base = {
        "block_timestamp": datetime(2026, 10, 5, tzinfo=UTC),
        "transaction_hash": "0x1234567890abcdef",
        "token_symbol": "USDC",
    }

    first = aave_activity_label({**base, "log_index": 4})
    second = aave_activity_label({**base, "log_index": 9})

    assert first != second
    assert "tx …7890abcdef" in first
    assert first.endswith("log 4")


def test_borrow_actor_roles_remain_distinct() -> None:
    roles = actor_roles(
        {
            "activity_type": "BORROW",
            "user": "0xuser",
            "on_behalf_of": "0xborrower",
        }
    )

    assert roles == (("User", "0xuser"), ("On behalf of", "0xborrower"))


def test_repay_actor_roles_remain_distinct() -> None:
    roles = actor_roles(
        {
            "activity_type": "REPAY",
            "user": "0xborrower",
            "repayer": "0xrepayer",
        }
    )

    assert roles == (("User", "0xborrower"), ("Repayer", "0xrepayer"))


def test_priced_aave_value_is_displayed() -> None:
    assert valid_price_display("priced", Decimal("0.9999")) == "$0.99990000"
    assert valid_amount_usd_display("priced", Decimal("10")) == "$10.00"


@pytest.mark.parametrize("status", ["stale", "unmapped", "no_prior_price"])
def test_invalid_aave_price_status_never_displays_value(status: str) -> None:
    assert valid_price_display(status, Decimal("100")) == "Unavailable"
    assert valid_amount_usd_display(status, Decimal("100")) == "Unavailable"


def test_null_aave_values_are_not_substituted_with_zero() -> None:
    assert valid_price_display("priced", None) == "Unavailable"
    assert valid_amount_usd_display("priced", None) == "Unavailable"


def test_aave_modules_import_without_snowflake_connection() -> None:
    from app.pages.aave import render_aave

    assert callable(render_aave)
