"""Unit coverage for provider-neutral freshness and SLA evaluation."""

from datetime import UTC, datetime, timedelta

import pytest

from orchestration.freshness import (
    CHAINLINK_FEED_THRESHOLDS,
    EVENT_ACTIVITY_THRESHOLD,
    FeedFreshnessSnapshot,
    FreshnessDimension,
    FreshnessState,
    FreshnessThreshold,
    SourceFreshnessSnapshot,
    age_seconds,
    build_freshness_report,
    classify,
    position_lag,
)

NOW = datetime(2026, 10, 21, 12, tzinfo=UTC)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (9, FreshnessState.FRESH),
        (10, FreshnessState.WARNING),
        (19, FreshnessState.WARNING),
        (20, FreshnessState.STALE),
    ],
)
def test_threshold_boundaries_are_exact(
    value: int,
    expected: FreshnessState,
) -> None:
    assert classify(value, FreshnessThreshold(10, 20, "blocks")) is expected


def test_position_lag_never_counts_confirmation_window_or_goes_negative() -> None:
    assert position_lag(safe_head=9_000, committed_position=8_500) == 500
    assert position_lag(safe_head=9_000, committed_position=9_100) == 0


def test_age_rejects_future_or_timezone_naive_values() -> None:
    assert age_seconds(NOW - timedelta(seconds=7), NOW) == 7
    with pytest.raises(ValueError, match="future"):
        age_seconds(NOW + timedelta(seconds=1), NOW)
    with pytest.raises(ValueError, match="timezone-aware"):
        age_seconds(datetime(2026, 10, 21), NOW)


def _feed(
    name: str,
    *,
    age: int = 60,
) -> FeedFreshnessSnapshot:
    return FeedFreshnessSnapshot(
        feed_name=name,
        feed_address=f"0x{name.replace('/', '').lower():0<40}"[:42],
        committed_round_id=100,
        source_round_id=100,
        latest_feed_updated_at=NOW - timedelta(seconds=age),
    )


def _report(
    *,
    aave_lag: int = 0,
    uniswap_lag: int = 0,
    event_age: int = 60,
    feed_ages: dict[str, int] | None = None,
    warehouse_age: int = 60,
):
    safe_head = 1_000_000
    ages = feed_ages or {}
    feeds = tuple(
        _feed(name, age=ages.get(name, 60))
        for name in CHAINLINK_FEED_THRESHOLDS
    )
    represented = {
        "mart_uniswap_swap_tutorial": NOW - timedelta(seconds=event_age),
        "mart_aave_lending_tutorial": NOW - timedelta(seconds=event_age),
        "mart_chainlink_oracle_tutorial": NOW - timedelta(seconds=event_age),
        "mart_point_in_time_pricing_tutorial": NOW - timedelta(seconds=event_age),
    }
    return build_freshness_report(
        checked_at=NOW,
        aave=SourceFreshnessSnapshot(
            "aave_forward",
            safe_head + 1_200,
            safe_head,
            safe_head - aave_lag,
            NOW - timedelta(seconds=event_age),
        ),
        uniswap=SourceFreshnessSnapshot(
            "uniswap_forward",
            safe_head + 1_200,
            safe_head,
            safe_head - uniswap_lag,
            NOW - timedelta(seconds=event_age),
        ),
        chainlink_source_head=safe_head + 1_200,
        chainlink_safe_head=safe_head,
        chainlink_committed_position=safe_head,
        chainlink_feeds=feeds,
        warehouse_refreshed_at=NOW - timedelta(seconds=warehouse_age),
        mart_represented_times=represented,
    )


def test_quiet_business_activity_does_not_override_fresh_source_coverage() -> None:
    report = _report(event_age=EVENT_ACTIVITY_THRESHOLD.stale)

    activity = tuple(
        result
        for result in report.results
        if result.dimension
        in {FreshnessDimension.EVENT_TIME, FreshnessDimension.REPRESENTED_DATA}
    )
    assert activity
    assert all(result.state is FreshnessState.STALE for result in activity)
    assert all(result.blocking is False for result in activity)
    assert report.overall_state is FreshnessState.FRESH


def test_source_position_staleness_is_authoritative() -> None:
    report = _report(aave_lag=250_000)

    aave = next(
        result
        for result in report.results
        if result.check_name == "aave_forward_source_position"
    )
    assert aave.observed_value == 250_000
    assert aave.state is FreshnessState.STALE
    assert report.overall_state is FreshnessState.STALE


def test_chainlink_feeds_are_evaluated_independently() -> None:
    report = _report(
        feed_ages={"USDT/USD": CHAINLINK_FEED_THRESHOLDS["USDT/USD"].stale}
    )
    feed_results = {
        result.check_name: result
        for result in report.results
        if result.dimension is FreshnessDimension.FEED
    }

    assert feed_results["chainlink_usdt_usd"].state is FreshnessState.STALE
    assert feed_results["chainlink_eth_usd"].state is FreshnessState.FRESH


def test_unknown_chainlink_feed_has_no_implicit_policy() -> None:
    with pytest.raises(ValueError, match="No freshness policy"):
        build_freshness_report(
            checked_at=NOW,
            aave=SourceFreshnessSnapshot(
                "aave_forward", 2_200, 1_000, 1_000, NOW
            ),
            uniswap=SourceFreshnessSnapshot(
                "uniswap_forward", 2_200, 1_000, 1_000, NOW
            ),
            chainlink_source_head=2_200,
            chainlink_safe_head=1_000,
            chainlink_committed_position=1_000,
            chainlink_feeds=(_feed("UNKNOWN/USD"),),
            warehouse_refreshed_at=NOW,
            mart_represented_times={"mart": NOW},
        )


def test_warehouse_and_mart_transformations_share_successful_refresh_age() -> None:
    report = _report(warehouse_age=93_600)
    warehouse_results = tuple(
        result
        for result in report.results
        if result.dimension
        in {FreshnessDimension.LOAD, FreshnessDimension.TRANSFORMATION}
    )

    assert len(warehouse_results) == 5
    assert {result.observed_value for result in warehouse_results} == {93_600}
    assert all(result.state is FreshnessState.WARNING for result in warehouse_results)
