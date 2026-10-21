"""Provider-neutral freshness evaluation contracts.

Freshness is evaluated on demand.  This module deliberately contains no
persistence or provider access so the same rules can be used by Airflow, a
CLI, and unit tests without mutating pipeline state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class FreshnessState(StrEnum):
    """Ordered health states for one independently measured dimension."""

    FRESH = "FRESH"
    WARNING = "WARNING"
    STALE = "STALE"


class FreshnessDimension(StrEnum):
    """Independent dimensions which must not be collapsed into one age."""

    SOURCE_POSITION = "source_position"
    EVENT_TIME = "event_time"
    FEED = "feed"
    LOAD = "load"
    TRANSFORMATION = "transformation"
    REPRESENTED_DATA = "represented_data"


@dataclass(frozen=True, slots=True)
class FreshnessThreshold:
    """Warning and stale boundaries expressed in one explicit unit."""

    warning: int
    stale: int
    unit: str

    def __post_init__(self) -> None:
        if self.warning < 0 or self.stale <= self.warning:
            raise ValueError("Freshness thresholds require 0 <= warning < stale")
        if not self.unit.strip():
            raise ValueError("Freshness threshold unit must not be empty")


@dataclass(frozen=True, slots=True)
class FreshnessResult:
    """One auditable freshness measurement and its evaluation context."""

    check_name: str
    pipeline_name: str
    dimension: FreshnessDimension
    state: FreshnessState
    observed_value: int
    threshold: FreshnessThreshold
    checked_at: datetime
    reason: str
    blocking: bool = True
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.check_name.strip() or not self.pipeline_name.strip():
            raise ValueError("Freshness check and pipeline names must not be empty")
        if self.observed_value < 0:
            raise ValueError("Freshness observed value must not be negative")
        if self.checked_at.tzinfo is None:
            raise ValueError("Freshness checked_at must be timezone-aware")
        if not self.reason.strip():
            raise ValueError("Freshness reason must not be empty")


def classify(value: int, threshold: FreshnessThreshold) -> FreshnessState:
    """Classify an exact value using inclusive warning/stale boundaries."""
    if value < 0:
        raise ValueError("Freshness value must not be negative")
    if value >= threshold.stale:
        return FreshnessState.STALE
    if value >= threshold.warning:
        return FreshnessState.WARNING
    return FreshnessState.FRESH


def evaluate_value(
    *,
    check_name: str,
    pipeline_name: str,
    dimension: FreshnessDimension,
    value: int,
    threshold: FreshnessThreshold,
    checked_at: datetime,
    reason: str,
    blocking: bool = True,
    details: dict[str, Any] | None = None,
) -> FreshnessResult:
    """Build one result from an already computed age or position lag."""
    return FreshnessResult(
        check_name=check_name,
        pipeline_name=pipeline_name,
        dimension=dimension,
        state=classify(value, threshold),
        observed_value=value,
        threshold=threshold,
        checked_at=checked_at.astimezone(UTC),
        reason=reason,
        blocking=blocking,
        details=details or {},
    )


def position_lag(safe_head: int, committed_position: int) -> int:
    """Return eligible unprocessed source positions, never confirmation lag."""
    if safe_head < 0 or committed_position < 0:
        raise ValueError("Source positions must not be negative")
    return max(0, safe_head - committed_position)


def age_seconds(observed_at: datetime, checked_at: datetime) -> int:
    """Return whole elapsed seconds, rejecting timestamps from the future."""
    if observed_at.tzinfo is None or checked_at.tzinfo is None:
        raise ValueError("Freshness timestamps must be timezone-aware")
    elapsed = checked_at.astimezone(UTC) - observed_at.astimezone(UTC)
    seconds = int(elapsed.total_seconds())
    if seconds < 0:
        raise ValueError("Freshness observation timestamp is in the future")
    return seconds


# Position thresholds are applied after the existing 1,200-block safety lag.
# The jobs are on-demand today: warning represents an operational backlog and
# stale represents a material recovery backlog, not protocol finality.
AAVE_POSITION_THRESHOLD = FreshnessThreshold(50_000, 250_000, "blocks")
UNISWAP_POSITION_THRESHOLD = FreshnessThreshold(50_000, 250_000, "blocks")
CHAINLINK_POSITION_THRESHOLD = FreshnessThreshold(10_000, 50_000, "blocks")

# Business inactivity is reported separately and never overrides source
# coverage.  These broad thresholds avoid treating quiet protocols as gaps.
EVENT_ACTIVITY_THRESHOLD = FreshnessThreshold(86_400, 259_200, "seconds")

# The warehouse is scheduled once daily at 06:00 UTC.  A 26-hour warning gives
# normal scheduling/runtime tolerance; 50 hours means two expected refreshes
# have effectively been missed.
WAREHOUSE_REFRESH_THRESHOLD = FreshnessThreshold(93_600, 180_000, "seconds")

# Recent canonical intervals were approximately 135s median / 480s p95 for
# ETH, 270s / 271s for USDC and USDT, and 271s / 1,623s for WBTC.  Warning is
# above the measured p95; stale allows several observed update intervals.
CHAINLINK_FEED_THRESHOLDS = {
    "ETH/USD": FreshnessThreshold(900, 3_600, "seconds"),
    "WBTC/USD": FreshnessThreshold(1_800, 7_200, "seconds"),
    "USDC/USD": FreshnessThreshold(900, 3_600, "seconds"),
    "USDT/USD": FreshnessThreshold(900, 3_600, "seconds"),
}


@dataclass(frozen=True, slots=True)
class SourceFreshnessSnapshot:
    """Read-only source and retained-event positions for one protocol."""

    pipeline_name: str
    source_head: int
    safe_head: int
    committed_position: int
    latest_event_at: datetime


@dataclass(frozen=True, slots=True)
class FeedFreshnessSnapshot:
    """Read-only state for one configured Chainlink feed."""

    feed_name: str
    feed_address: str
    committed_round_id: int
    source_round_id: int
    latest_feed_updated_at: datetime


@dataclass(frozen=True, slots=True)
class FreshnessReport:
    """Complete point-in-time result set for the production platform."""

    checked_at: datetime
    results: tuple[FreshnessResult, ...]

    @property
    def overall_state(self) -> FreshnessState:
        ranking = {
            FreshnessState.FRESH: 0,
            FreshnessState.WARNING: 1,
            FreshnessState.STALE: 2,
        }
        blocking = tuple(item for item in self.results if item.blocking)
        if not blocking:
            raise ValueError("Freshness report requires at least one blocking check")
        return max((item.state for item in blocking), key=ranking.get)


def build_freshness_report(
    *,
    checked_at: datetime,
    aave: SourceFreshnessSnapshot,
    uniswap: SourceFreshnessSnapshot,
    chainlink_source_head: int,
    chainlink_safe_head: int,
    chainlink_committed_position: int,
    chainlink_feeds: tuple[FeedFreshnessSnapshot, ...],
    warehouse_refreshed_at: datetime,
    mart_represented_times: dict[str, datetime],
) -> FreshnessReport:
    """Evaluate independent source, activity, feed, and warehouse dimensions."""
    now = checked_at.astimezone(UTC)
    results: list[FreshnessResult] = []
    source_thresholds = {
        "aave_forward": AAVE_POSITION_THRESHOLD,
        "uniswap_forward": UNISWAP_POSITION_THRESHOLD,
    }
    for source in (aave, uniswap):
        details = {
            "source_head": source.source_head,
            "safe_head": source.safe_head,
            "committed_position": source.committed_position,
        }
        results.append(
            evaluate_value(
                check_name=f"{source.pipeline_name}_source_position",
                pipeline_name=source.pipeline_name,
                dimension=FreshnessDimension.SOURCE_POSITION,
                value=position_lag(source.safe_head, source.committed_position),
                threshold=source_thresholds[source.pipeline_name],
                checked_at=now,
                reason="Eligible confirmed blocks not yet committed",
                details=details,
            )
        )
        results.append(
            evaluate_value(
                check_name=f"{source.pipeline_name}_event_activity",
                pipeline_name=source.pipeline_name,
                dimension=FreshnessDimension.EVENT_TIME,
                value=age_seconds(source.latest_event_at, now),
                threshold=EVENT_ACTIVITY_THRESHOLD,
                checked_at=now,
                reason=(
                    "Age of the latest retained business event; informational and "
                    "independent of source coverage"
                ),
                blocking=False,
                details={"latest_event_at": source.latest_event_at.isoformat()},
            )
        )

    results.append(
        evaluate_value(
            check_name="chainlink_forward_source_position",
            pipeline_name="chainlink_forward",
            dimension=FreshnessDimension.SOURCE_POSITION,
            value=position_lag(
                chainlink_safe_head,
                chainlink_committed_position,
            ),
            threshold=CHAINLINK_POSITION_THRESHOLD,
            checked_at=now,
            reason="Eligible confirmed blocks not yet committed globally",
            details={
                "source_head": chainlink_source_head,
                "safe_head": chainlink_safe_head,
                "committed_position": chainlink_committed_position,
            },
        )
    )
    for feed in chainlink_feeds:
        threshold = CHAINLINK_FEED_THRESHOLDS.get(feed.feed_name)
        if threshold is None:
            raise ValueError(f"No freshness policy for Chainlink feed {feed.feed_name}")
        results.append(
            evaluate_value(
                check_name=f"chainlink_{feed.feed_name.lower().replace('/', '_')}",
                pipeline_name="chainlink_forward",
                dimension=FreshnessDimension.FEED,
                value=age_seconds(feed.latest_feed_updated_at, now),
                threshold=threshold,
                checked_at=now,
                reason="Age of the feed's latest canonical effective observation",
                details={
                    "feed_address": feed.feed_address,
                    "committed_round_id": feed.committed_round_id,
                    "source_round_id": feed.source_round_id,
                    "latest_feed_updated_at": feed.latest_feed_updated_at.isoformat(),
                },
            )
        )

    refresh_age = age_seconds(warehouse_refreshed_at, now)
    results.append(
        evaluate_value(
            check_name="daily_warehouse_load",
            pipeline_name="daily_warehouse_pipeline",
            dimension=FreshnessDimension.LOAD,
            value=refresh_age,
            threshold=WAREHOUSE_REFRESH_THRESHOLD,
            checked_at=now,
            reason="Age of the latest successful daily warehouse run",
            details={"latest_successful_refresh": warehouse_refreshed_at.isoformat()},
        )
    )
    for mart_name, represented_at in sorted(mart_represented_times.items()):
        results.append(
            evaluate_value(
                check_name=f"{mart_name}_transformation",
                pipeline_name="daily_warehouse_pipeline",
                dimension=FreshnessDimension.TRANSFORMATION,
                value=refresh_age,
                threshold=WAREHOUSE_REFRESH_THRESHOLD,
                checked_at=now,
                reason="MART transformation age from the successful warehouse run",
                details={
                    "latest_successful_refresh": warehouse_refreshed_at.isoformat(),
                    "latest_represented_at": represented_at.isoformat(),
                    "represented_data_age_seconds": age_seconds(represented_at, now),
                },
            )
        )
        results.append(
            evaluate_value(
                check_name=f"{mart_name}_represented_data",
                pipeline_name="daily_warehouse_pipeline",
                dimension=FreshnessDimension.REPRESENTED_DATA,
                value=age_seconds(represented_at, now),
                threshold=EVENT_ACTIVITY_THRESHOLD,
                checked_at=now,
                reason=(
                    "Age of the latest business time represented by the MART; "
                    "informational and independent of transformation freshness"
                ),
                blocking=False,
                details={"latest_represented_at": represented_at.isoformat()},
            )
        )
    return FreshnessReport(now, tuple(results))
