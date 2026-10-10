"""Focused tests for Chainlink and point-in-time pricing lessons."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from app.components.aave_lesson import (
    valid_amount_usd_display,
    valid_price_display,
)
from app.components.oracle_lesson import oracle_price_equation
from app.components.pit_lesson import as_of_timeline, pit_status_explanation
from app.config import AppConfig
from app.data.snowflake import SnowflakeTutorialRepository
from app.pages.chainlink import observation_label
from app.pages.pit import pit_row_label


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


def test_chainlink_feed_listing_queries_only_oracle_mart() -> None:
    repository, cursor = repository_with_rows(
        [
            {
                "FEED_ADDRESS": "0xfeed",
                "BASE_ASSET": "ETH",
                "QUOTE_ASSET": "USD",
            }
        ]
    )

    feeds = repository.list_chainlink_feeds()

    assert feeds[0]["base_asset"] == "ETH"
    assert "MART_CHAINLINK_ORACLE_TUTORIAL" in cursor.query
    assert "CORE" not in cursor.query


def test_chainlink_observation_list_filters_feed_with_parameter() -> None:
    repository, cursor = repository_with_rows([])

    repository.list_chainlink_observations("0xfeed", limit=25)

    assert "LOWER(%(feed_address)s)" in cursor.query
    assert cursor.params == {"feed_address": "0xfeed", "limit": 25}


def test_chainlink_observation_retrieves_previous_round_fields() -> None:
    repository, cursor = repository_with_rows(
        [
            {
                "OBSERVATION_ID": "observation-2",
                "PREVIOUS_OBSERVATION_ID": "observation-1",
                "PREVIOUS_PRICE": Decimal("100"),
            }
        ]
    )

    result = repository.get_chainlink_observation("observation-2")

    assert result["previous_observation_id"] == "observation-1"
    assert result["previous_price"] == Decimal("100")
    assert cursor.params == {"observation_id": "observation-2"}


def test_oracle_equation_uses_canonical_mart_price() -> None:
    equation = oracle_price_equation(
        {
            "answer_raw": "123456789",
            "feed_decimals": 8,
            "price": Decimal("1.234567890000000000"),
        }
    )

    assert equation == "123456789 ÷ 10^8 = 1.23456789"


def test_observation_label_distinguishes_rounds() -> None:
    timestamp = datetime(2026, 10, 5, tzinfo=UTC)

    first = observation_label({"feed_updated_at": timestamp, "round_id": "1"})
    second = observation_label({"feed_updated_at": timestamp, "round_id": "2"})

    assert first != second


def test_pit_protocol_and_token_lists_are_parameterized() -> None:
    repository, cursor = repository_with_rows([{"PROTOCOL": "aave_v3"}])
    assert repository.list_pit_protocols() == ["aave_v3"]

    cursor.rows = [{"TOKEN_SYMBOL": "USDC"}]
    assert repository.list_pit_tokens("aave_v3") == ["USDC"]
    assert cursor.params == {"protocol": "aave_v3"}


def test_pit_status_list_applies_optional_token_filter() -> None:
    repository, cursor = repository_with_rows([{"PRICE_STATUS": "priced"}])

    statuses = repository.list_pit_price_statuses("aave_v3", "USDC")

    assert statuses == ["priced"]
    assert "TOKEN_SYMBOL = %(token_symbol)s" in cursor.query
    assert cursor.params == {"protocol": "aave_v3", "token_symbol": "USDC"}


def test_pit_row_list_applies_all_filters() -> None:
    repository, cursor = repository_with_rows([{"EVENT_ID": "event-1"}])

    rows = repository.list_pit_rows(
        "uniswap_v3",
        token_symbol="WETH",
        price_status="stale",
        limit=50,
    )

    assert rows == [{"event_id": "event-1"}]
    assert "MART_POINT_IN_TIME_PRICING_TUTORIAL" in cursor.query
    assert "TOKEN_SYMBOL = %(token_symbol)s" in cursor.query
    assert "PRICE_STATUS = %(price_status)s" in cursor.query
    assert cursor.params == {
        "protocol": "uniswap_v3",
        "token_symbol": "WETH",
        "price_status": "stale",
        "limit": 50,
    }


def test_pit_selected_row_uses_full_event_token_identity() -> None:
    repository, cursor = repository_with_rows(
        [{"EVENT_ID": "event-1", "TOKEN_SIDE": "token0"}]
    )

    result = repository.get_pit_row("uniswap_swap", "event-1", "token0")

    assert result["token_side"] == "token0"
    assert cursor.params == {
        "event_domain": "uniswap_swap",
        "event_id": "event-1",
        "token_side": "token0",
    }


@pytest.mark.parametrize(
    "status", ["stale", "unmapped", "no_prior_price"]
)
def test_nonpriced_pit_status_never_displays_valuation(status: str) -> None:
    assert valid_price_display(status, Decimal("42")) == "Unavailable"
    assert valid_amount_usd_display(status, Decimal("84")) == "Unavailable"


def test_pit_status_explanations_preserve_canonical_semantics() -> None:
    assert "300-second threshold" in pit_status_explanation("stale")
    assert "approved feed mapping" in pit_status_explanation("no_prior_price")
    assert "at or before the event" in pit_status_explanation("no_prior_price")


def test_priced_pit_status_displays_mart_values() -> None:
    assert valid_price_display("priced", Decimal("42")) == "$42.00000000"
    assert valid_amount_usd_display("priced", None) == "Unavailable"


def test_as_of_timeline_keeps_next_round_separate() -> None:
    selected = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    event = datetime(2026, 10, 5, 12, 1, tzinfo=UTC)
    next_update = datetime(2026, 10, 5, 12, 2, tzinfo=UTC)

    timeline = as_of_timeline(
        {
            "feed_updated_at": selected,
            "event_timestamp": event,
            "next_feed_updated_at": next_update,
        }
    )

    assert timeline.index("selected") < timeline.index("EVENT")
    assert timeline.index("EVENT") < timeline.index("next")
    assert selected <= event < next_update


def test_pit_selector_label_includes_token_side_and_event_suffix() -> None:
    row = {
        "event_timestamp": datetime(2026, 10, 5, tzinfo=UTC),
        "event_type": "swap",
        "token_symbol": "WETH",
        "token_side": "token1",
        "event_id": "arbitrum-transaction-42",
    }

    label = pit_row_label(row)

    assert "token1" in label
    assert label.endswith("…ansaction-42")


def test_pricing_page_modules_import_without_snowflake_connection() -> None:
    from app.pages.chainlink import render_chainlink
    from app.pages.pit import render_pit

    assert callable(render_chainlink)
    assert callable(render_pit)
