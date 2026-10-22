"""Provider-neutral alert lifecycle decisions for pipeline health signals."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from orchestration.freshness import (
    FreshnessDimension,
    FreshnessReport,
    FreshnessResult,
    FreshnessState,
)


class AlertState(StrEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"


class AlertSeverity(StrEnum):
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertEventType(StrEnum):
    OPENED = "ALERT_OPENED"
    ESCALATED = "ALERT_ESCALATED"
    RESOLVED = "ALERT_RESOLVED"


class AlertReasonCode(StrEnum):
    SOURCE_POSITION_LAG = "SOURCE_POSITION_LAG"
    CHAINLINK_FEED_AGE = "CHAINLINK_FEED_AGE"
    WAREHOUSE_REFRESH_AGE = "WAREHOUSE_REFRESH_AGE"
    MART_REFRESH_AGE = "MART_REFRESH_AGE"
    PIPELINE_FAILURE = "PIPELINE_FAILURE"


class AlertConcurrencyError(RuntimeError):
    """Raised when durable alert state changes during a CAS update."""


class DuplicateAlertKeyError(RuntimeError):
    """Raised when physical duplicate rows exist for one alert key."""


@dataclass(frozen=True, slots=True)
class HealthCondition:
    """One current health result, separate from durable incident state."""

    alert_key: str
    check_name: str
    pipeline_name: str
    reason_code: AlertReasonCode
    state: FreshnessState
    current_value: int
    warning_threshold: int
    stale_threshold: int


@dataclass(frozen=True, slots=True)
class AlertRecord:
    """Current durable lifecycle state for one pre-seeded alert identity."""

    alert_key: str
    check_name: str
    pipeline_name: str
    reason_code: AlertReasonCode
    state: AlertState
    severity: AlertSeverity | None
    opened_at: datetime | None
    last_observed_at: datetime | None
    resolved_at: datetime | None
    incident_generation: int
    occurrence_count: int
    current_value: int | None
    warning_threshold: int | None
    stale_threshold: int | None
    state_version: int


@dataclass(frozen=True, slots=True)
class NotificationEvent:
    """Secret-free future contract for an external notification adapter."""

    event_type: AlertEventType
    alert_key: str
    pipeline_name: str
    check_name: str
    severity: AlertSeverity
    reason_code: AlertReasonCode
    current_value: int
    warning_threshold: int
    stale_threshold: int
    opened_at: datetime
    resolved_at: datetime | None
    observed_at: datetime
    incident_generation: int


@dataclass(frozen=True, slots=True)
class AlertDecision:
    record: AlertRecord
    event: NotificationEvent | None
    changed: bool


class AlertStore(Protocol):
    def get(self, alert_key: str) -> AlertRecord: ...

    def compare_and_set(
        self,
        expected_version: int,
        candidate: AlertRecord,
    ) -> AlertRecord: ...


_FRESHNESS_ALERTS = {
    "aave_forward_source_position": (
        "freshness:aave_forward",
        AlertReasonCode.SOURCE_POSITION_LAG,
    ),
    "uniswap_forward_source_position": (
        "freshness:uniswap_forward",
        AlertReasonCode.SOURCE_POSITION_LAG,
    ),
    "chainlink_forward_source_position": (
        "freshness:chainlink_global",
        AlertReasonCode.SOURCE_POSITION_LAG,
    ),
    "chainlink_eth_usd": (
        "freshness:chainlink_eth_usd",
        AlertReasonCode.CHAINLINK_FEED_AGE,
    ),
    "chainlink_usdc_usd": (
        "freshness:chainlink_usdc_usd",
        AlertReasonCode.CHAINLINK_FEED_AGE,
    ),
    "chainlink_usdt_usd": (
        "freshness:chainlink_usdt_usd",
        AlertReasonCode.CHAINLINK_FEED_AGE,
    ),
    "chainlink_wbtc_usd": (
        "freshness:chainlink_wbtc_usd",
        AlertReasonCode.CHAINLINK_FEED_AGE,
    ),
    "daily_warehouse_load": (
        "freshness:daily_warehouse",
        AlertReasonCode.WAREHOUSE_REFRESH_AGE,
    ),
}

MONITORED_PIPELINES = (
    "aave_forward",
    "chainlink_forward",
    "uniswap_forward",
    "daily_warehouse_pipeline",
)


def _condition(result: FreshnessResult) -> HealthCondition:
    alert_key, reason_code = _FRESHNESS_ALERTS[result.check_name]
    return HealthCondition(
        alert_key=alert_key,
        check_name=result.check_name,
        pipeline_name=result.pipeline_name,
        reason_code=reason_code,
        state=result.state,
        current_value=result.observed_value,
        warning_threshold=result.threshold.warning,
        stale_threshold=result.threshold.stale,
    )


def freshness_conditions(report: FreshnessReport) -> tuple[HealthCondition, ...]:
    """Map existing blocking freshness results without recalculating policy."""
    conditions = [
        _condition(result)
        for result in report.results
        if result.blocking and result.check_name in _FRESHNESS_ALERTS
    ]
    marts = tuple(
        result
        for result in report.results
        if result.blocking
        and result.dimension is FreshnessDimension.TRANSFORMATION
    )
    if marts:
        ranking = {
            FreshnessState.FRESH: 0,
            FreshnessState.WARNING: 1,
            FreshnessState.STALE: 2,
        }
        worst = max(marts, key=lambda result: ranking[result.state])
        conditions.append(
            HealthCondition(
                alert_key="freshness:marts",
                check_name="mart_transformations",
                pipeline_name="daily_warehouse_pipeline",
                reason_code=AlertReasonCode.MART_REFRESH_AGE,
                state=worst.state,
                current_value=max(result.observed_value for result in marts),
                warning_threshold=worst.threshold.warning,
                stale_threshold=worst.threshold.stale,
            )
        )
    return tuple(sorted(conditions, key=lambda item: item.alert_key))


def pipeline_failure_conditions(
    statuses: dict[str, str],
) -> tuple[HealthCondition, ...]:
    """Convert latest terminal run status into independent failure health."""
    conditions = []
    for pipeline_name in MONITORED_PIPELINES:
        status = statuses.get(pipeline_name)
        if status not in {"FAILED", "SUCCEEDED", "NO_OP"}:
            continue
        conditions.append(
            HealthCondition(
                alert_key=f"pipeline_failure:{pipeline_name}",
                check_name=f"{pipeline_name}_execution",
                pipeline_name=pipeline_name,
                reason_code=AlertReasonCode.PIPELINE_FAILURE,
                state=(
                    FreshnessState.STALE
                    if status == "FAILED"
                    else FreshnessState.FRESH
                ),
                current_value=1 if status == "FAILED" else 0,
                warning_threshold=0,
                stale_threshold=1,
            )
        )
    return tuple(conditions)


def _severity(state: FreshnessState) -> AlertSeverity:
    if state is FreshnessState.WARNING:
        return AlertSeverity.WARNING
    if state is FreshnessState.STALE:
        return AlertSeverity.CRITICAL
    raise ValueError("FRESH has no alert severity")


def _event(
    event_type: AlertEventType,
    record: AlertRecord,
    observed_at: datetime,
) -> NotificationEvent:
    assert record.severity is not None
    assert record.opened_at is not None
    assert record.current_value is not None
    assert record.warning_threshold is not None
    assert record.stale_threshold is not None
    return NotificationEvent(
        event_type=event_type,
        alert_key=record.alert_key,
        pipeline_name=record.pipeline_name,
        check_name=record.check_name,
        severity=record.severity,
        reason_code=record.reason_code,
        current_value=record.current_value,
        warning_threshold=record.warning_threshold,
        stale_threshold=record.stale_threshold,
        opened_at=record.opened_at,
        resolved_at=record.resolved_at,
        observed_at=observed_at,
        incident_generation=record.incident_generation,
    )


def decide_alert_transition(
    current: AlertRecord,
    condition: HealthCondition,
    observed_at: datetime,
) -> AlertDecision:
    """Apply deduplication, escalation, resolution, and reopening policy."""
    if observed_at.tzinfo is None:
        raise ValueError("Alert observation time must be timezone-aware")
    now = observed_at.astimezone(UTC)
    if current.alert_key != condition.alert_key:
        raise ValueError("Alert record and condition identities differ")
    if (
        current.check_name != condition.check_name
        or current.pipeline_name != condition.pipeline_name
        or current.reason_code is not condition.reason_code
    ):
        raise ValueError("Pre-seeded alert metadata does not match the condition")
    if condition.state is FreshnessState.FRESH:
        if current.state is AlertState.RESOLVED:
            return AlertDecision(current, None, False)
        candidate = AlertRecord(
            **{
                **_record_values(current),
                "state": AlertState.RESOLVED,
                "last_observed_at": now,
                "resolved_at": now,
                "current_value": condition.current_value,
                "warning_threshold": condition.warning_threshold,
                "stale_threshold": condition.stale_threshold,
                "state_version": current.state_version + 1,
            }
        )
        return AlertDecision(
            candidate,
            _event(AlertEventType.RESOLVED, candidate, now),
            True,
        )

    observed_severity = _severity(condition.state)
    if current.state is AlertState.RESOLVED:
        candidate = AlertRecord(
            alert_key=current.alert_key,
            check_name=condition.check_name,
            pipeline_name=condition.pipeline_name,
            reason_code=condition.reason_code,
            state=AlertState.OPEN,
            severity=observed_severity,
            opened_at=now,
            last_observed_at=now,
            resolved_at=None,
            incident_generation=current.incident_generation + 1,
            occurrence_count=1,
            current_value=condition.current_value,
            warning_threshold=condition.warning_threshold,
            stale_threshold=condition.stale_threshold,
            state_version=current.state_version + 1,
        )
        return AlertDecision(
            candidate,
            _event(AlertEventType.OPENED, candidate, now),
            True,
        )

    escalated = (
        current.severity is AlertSeverity.WARNING
        and observed_severity is AlertSeverity.CRITICAL
    )
    # CRITICAL remains sticky until full recovery; WARNING never downgrades it.
    severity = (
        AlertSeverity.CRITICAL
        if current.severity is AlertSeverity.CRITICAL or escalated
        else AlertSeverity.WARNING
    )
    candidate = AlertRecord(
        **{
            **_record_values(current),
            "severity": severity,
            "last_observed_at": now,
            "occurrence_count": current.occurrence_count + 1,
            "current_value": condition.current_value,
            "warning_threshold": condition.warning_threshold,
            "stale_threshold": condition.stale_threshold,
            "state_version": current.state_version + 1,
        }
    )
    event = (
        _event(AlertEventType.ESCALATED, candidate, now)
        if escalated
        else None
    )
    return AlertDecision(candidate, event, True)


def _record_values(record: AlertRecord) -> dict[str, object]:
    return {
        name: getattr(record, name)
        for name in AlertRecord.__dataclass_fields__
    }


def evaluate_alerts(
    store: AlertStore,
    conditions: tuple[HealthCondition, ...],
    observed_at: datetime,
    *,
    max_cas_attempts: int = 3,
) -> tuple[AlertDecision, ...]:
    """Evaluate conditions and retry bounded CAS conflicts from fresh state."""
    if max_cas_attempts < 1:
        raise ValueError("max_cas_attempts must be at least one")
    decisions = []
    for condition in conditions:
        for attempt in range(max_cas_attempts):
            current = store.get(condition.alert_key)
            decision = decide_alert_transition(
                current,
                condition,
                observed_at,
            )
            if not decision.changed:
                decisions.append(decision)
                break
            try:
                persisted = store.compare_and_set(
                    current.state_version,
                    decision.record,
                )
            except AlertConcurrencyError:
                if attempt + 1 == max_cas_attempts:
                    raise
                continue
            decisions.append(
                AlertDecision(persisted, decision.event, True)
            )
            break
    return tuple(decisions)
