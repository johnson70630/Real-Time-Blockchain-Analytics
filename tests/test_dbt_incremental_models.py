"""Contracts for the mixed dbt incremental materialization strategy."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

DBT_MODELS = Path("dbt/models")
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)

INCREMENTAL_MODELS = {
    "core/fact_uniswap_swap.sql": "unique_key='event_id'",
    "core/fact_uniswap_swap_valued.sql": "unique_key='event_id'",
    "intermediate/pricing/int_defi_event_token_prices.sql": (
        "unique_key=['event_domain', 'event_id', 'token_side']"
    ),
    "marts/pricing/mart_point_in_time_pricing_tutorial.sql": (
        "unique_key=['event_domain', 'event_id', 'token_side']"
    ),
    "marts/uniswap/mart_uniswap_swap_tutorial.sql": "unique_key='event_id'",
}

SMALL_FULL_REBUILD_MODELS = (
    "core/dim_chain.sql",
    "core/dim_date.sql",
    "core/dim_pool.sql",
    "core/dim_protocol.sql",
    "core/dim_token.sql",
    "core/fact_aave_borrow.sql",
    "core/fact_aave_liquidation.sql",
    "core/fact_aave_repay.sql",
    "core/fact_market_price.sql",
    "marts/aave/mart_aave_lending_tutorial.sql",
    "marts/pricing/mart_chainlink_oracle_tutorial.sql",
)


def _model(relative_path: str) -> str:
    return (DBT_MODELS / relative_path).read_text(encoding="utf-8")


def _selected_price(
    event_time: datetime,
    price_times: tuple[datetime, ...],
) -> datetime | None:
    eligible = tuple(price_time for price_time in price_times if price_time <= event_time)
    return max(eligible, default=None)


def _is_affected(
    event_time: datetime,
    previous_price_time: datetime | None,
    next_price_time: datetime | None,
) -> bool:
    return (
        previous_price_time is None or event_time >= previous_price_time
    ) and (next_price_time is None or event_time < next_price_time)


def test_only_measured_large_models_are_incremental() -> None:
    for path, unique_key in INCREMENTAL_MODELS.items():
        sql = _model(path)
        assert "materialized='incremental'" in sql
        assert "incremental_strategy='merge'" in sql
        assert unique_key in sql
        assert "{% if is_incremental() %}" in sql

    for path in SMALL_FULL_REBUILD_MODELS:
        assert "materialized='incremental'" not in _model(path)


def test_uniswap_fact_selects_new_and_replayed_loaded_events() -> None:
    sql = _model("core/fact_uniswap_swap.sql")
    assert "swaps.loaded_at as source_loaded_at" in sql
    assert "swaps.loaded_at >" in sql
    assert "max(source_loaded_at)" in sql
    assert "unique_key='event_id'" in sql


def test_late_price_changes_existing_pit_assignment() -> None:
    event_time = EPOCH.replace(hour=10, minute=5)
    old_prices = (EPOCH.replace(hour=9, minute=58), EPOCH.replace(hour=10, minute=10))
    recovered_prices = (
        EPOCH.replace(hour=9, minute=58),
        EPOCH.replace(hour=10, minute=3),
        EPOCH.replace(hour=10, minute=10),
    )

    assert _selected_price(event_time, old_prices) == old_prices[0]
    assert _selected_price(event_time, recovered_prices) == recovered_prices[1]


def test_price_split_recomputes_only_neighboring_intervals() -> None:
    previous_price = EPOCH.replace(hour=9, minute=58)
    next_price = EPOCH.replace(hour=10, minute=10)

    assert _is_affected(EPOCH.replace(hour=10), previous_price, next_price)
    assert _is_affected(
        EPOCH.replace(hour=10, minute=5), previous_price, next_price
    )
    assert not _is_affected(
        EPOCH.replace(hour=9, minute=57), previous_price, next_price
    )
    assert not _is_affected(
        EPOCH.replace(hour=10, minute=10), previous_price, next_price
    )


def test_stale_and_no_future_price_contract_is_unchanged() -> None:
    event_time = EPOCH.replace(hour=10, minute=5)
    old_price = EPOCH.replace(hour=9, minute=58)
    future_price = EPOCH.replace(hour=10, minute=10)

    assert _selected_price(event_time, (old_price, future_price)) == old_price
    assert event_time - old_price > timedelta(seconds=300)


def test_incremental_pit_tracks_both_event_and_price_changes() -> None:
    sql = _model("intermediate/pricing/int_defi_event_token_prices.sql")
    assert "event_tokens.source_loaded_at >" in sql
    assert "prices.source_loaded_at >" in sql
    assert "previous_feed_updated_at" in sql
    assert "next_feed_updated_at" in sql
    assert "existing.matched_observation_id = changed.observation_id" in sql
    assert "selected_event_tokens as (\n    select * from event_tokens" in sql
