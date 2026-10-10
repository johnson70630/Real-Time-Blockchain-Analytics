"""Focused tests for the integrated DeFi Data Lab presentation layer."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.components.shared import (
    LESSON_PAGES,
    display_value,
    format_decimal,
    format_price,
    format_timestamp,
    format_usd,
    price_status_explanation,
    price_status_label,
    shorten_identifier,
)
from app.components.pit_lesson import timeline_points
from app.pages.chainlink import build_price_history_chart, prepare_price_history


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("1000.000000000000000000"), "1,000"),
        (Decimal("0.002057449371046765"), "0.002057449371046765"),
        (Decimal("-1234.500000"), "-1,234.5"),
        (None, "Unavailable"),
    ],
)
def test_exact_decimal_formatting(value: Decimal | None, expected: str) -> None:
    assert format_decimal(value) == expected


def test_currency_formatting_preserves_useful_precision_and_nulls() -> None:
    assert format_usd(Decimal("2947.44000000")) == "$2,947.44"
    assert format_usd(Decimal("0.002057449")) == "$0.00205745"
    assert format_price(Decimal("0.99988392")) == "$0.99988392"
    assert format_usd(None) == "Unavailable"
    assert display_value(None) == "Unavailable"


def test_identifier_shortening_does_not_mutate_underlying_value() -> None:
    address = "0xe5ec1234567890dc"

    assert shorten_identifier(address) == "0xe5ec…90dc"
    assert address == "0xe5ec1234567890dc"


def test_timestamp_format_is_explicit_without_timezone_conversion() -> None:
    aware = datetime(2026, 10, 5, 12, 30, tzinfo=UTC)
    naive = datetime(2026, 10, 5, 12, 30)

    assert format_timestamp(aware) == "2026-10-05 12:30:00+00:00"
    assert format_timestamp(naive).endswith("(timezone not provided)")


@pytest.mark.parametrize(
    ("status", "label", "phrase"),
    [
        ("priced", "Priced", "no more than 300 seconds"),
        ("stale", "Stale", "older than"),
        ("unmapped", "Unmapped", "no approved Chainlink feed mapping"),
        ("no_prior_price", "No prior price", "at or before the event"),
    ],
)
def test_price_status_presentation_is_consistent(
    status: str, label: str, phrase: str
) -> None:
    assert price_status_label(status) == label
    assert phrase in price_status_explanation(status)


def test_cross_page_navigation_targets_every_lesson() -> None:
    assert LESSON_PAGES == (
        "Uniswap Swap",
        "Aave Lending",
        "Chainlink Oracle",
        "Point-in-Time Pricing",
    )


def test_chainlink_history_is_chronological_and_preserves_decimals() -> None:
    later = datetime(2026, 10, 5, 12, 1, tzinfo=UTC)
    earlier = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    rows = [
        {"feed_updated_at": later, "price": Decimal("1.00000001")},
        {"feed_updated_at": earlier, "price": Decimal("0.99988392")},
        {"feed_updated_at": None, "price": None},
    ]

    history = prepare_price_history(rows)

    assert [point["Observation time"] for point in history] == [earlier, later]
    assert history[0]["Oracle price"] == Decimal("0.99988392")

    chart = build_price_history_chart(history).to_dict()
    assert chart["encoding"]["y"]["type"] == "quantitative"
    assert chart["data"]["values"][0]["Oracle price"] == "0.99988392"


def test_pit_timeline_uses_mart_timestamps_without_recomputing_match() -> None:
    selected = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    event = datetime(2026, 10, 5, 12, 1, tzinfo=UTC)
    next_update = datetime(2026, 10, 5, 12, 2, tzinfo=UTC)

    points = timeline_points(
        {
            "feed_updated_at": selected,
            "event_timestamp": event,
            "next_feed_updated_at": next_update,
        }
    )

    assert [point[0] for point in points] == [
        "Selected observation",
        "Event",
        "Next observation",
    ]
    assert [point[1] for point in points] == [selected, event, next_update]
    assert "never used for valuation" in points[-1][2]


def test_pit_timeline_handles_unavailable_observations() -> None:
    event = datetime(2026, 10, 5, 12, 1, tzinfo=UTC)

    points = timeline_points(
        {
            "feed_updated_at": None,
            "event_timestamp": event,
            "next_feed_updated_at": None,
        }
    )

    assert points == (("Event", event, "Valuation timestamp"),)


def test_all_page_modules_import_without_external_connections() -> None:
    from app.app import main
    from app.pages.aave import render_aave
    from app.pages.chainlink import render_chainlink
    from app.pages.home import render_home
    from app.pages.pit import render_pit
    from app.pages.uniswap import render_uniswap

    assert all(
        callable(page)
        for page in (
            main,
            render_home,
            render_uniswap,
            render_aave,
            render_chainlink,
            render_pit,
        )
    )
