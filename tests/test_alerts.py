"""Alert lifecycle, deduplication, concurrency, and payload contracts."""

import json
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from orchestration.alerts import (
    AlertConcurrencyError,
    AlertEventType,
    AlertReasonCode,
    AlertRecord,
    AlertSeverity,
    AlertState,
    DuplicateAlertKeyError,
    HealthCondition,
    decide_alert_transition,
    evaluate_alerts,
    freshness_conditions,
    pipeline_failure_conditions,
)
from orchestration.freshness import (
    FreshnessDimension,
    FreshnessReport,
    FreshnessResult,
    FreshnessState,
    FreshnessThreshold,
)
from warehouse.alert_state import SnowflakeAlertStore

NOW = datetime(2026, 10, 22, 12, tzinfo=UTC)


def _condition(
    state: FreshnessState,
    *,
    key: str = "freshness:aave_forward",
    value: int = 10,
) -> HealthCondition:
    return HealthCondition(
        alert_key=key,
        check_name="aave_forward_source_position",
        pipeline_name="aave_forward",
        reason_code=AlertReasonCode.SOURCE_POSITION_LAG,
        state=state,
        current_value=value,
        warning_threshold=5,
        stale_threshold=10,
    )


def _seed(key: str = "freshness:aave_forward") -> AlertRecord:
    return AlertRecord(
        alert_key=key,
        check_name="aave_forward_source_position",
        pipeline_name="aave_forward",
        reason_code=AlertReasonCode.SOURCE_POSITION_LAG,
        state=AlertState.RESOLVED,
        severity=None,
        opened_at=None,
        last_observed_at=None,
        resolved_at=None,
        incident_generation=0,
        occurrence_count=0,
        current_value=None,
        warning_threshold=None,
        stale_threshold=None,
        state_version=0,
    )


class MemoryAlertStore:
    def __init__(self, records: tuple[AlertRecord, ...]) -> None:
        self.records = {record.alert_key: record for record in records}

    def get(self, alert_key: str) -> AlertRecord:
        return self.records[alert_key]

    def compare_and_set(
        self,
        expected_version: int,
        candidate: AlertRecord,
    ) -> AlertRecord:
        current = self.records[candidate.alert_key]
        if current.state_version != expected_version:
            raise AlertConcurrencyError("stale test writer")
        self.records[candidate.alert_key] = candidate
        return candidate


def test_fresh_creates_no_alert_or_state_write() -> None:
    store = MemoryAlertStore((_seed(),))
    decision = evaluate_alerts(
        store,
        (_condition(FreshnessState.FRESH, value=0),),
        NOW,
    )[0]
    assert decision.changed is False
    assert decision.event is None
    assert store.get("freshness:aave_forward").state_version == 0


@pytest.mark.parametrize(
    ("state", "severity"),
    [
        (FreshnessState.WARNING, AlertSeverity.WARNING),
        (FreshnessState.STALE, AlertSeverity.CRITICAL),
    ],
)
def test_unhealthy_condition_opens_expected_severity(
    state: FreshnessState,
    severity: AlertSeverity,
) -> None:
    decision = decide_alert_transition(_seed(), _condition(state), NOW)
    assert decision.record.state is AlertState.OPEN
    assert decision.record.severity is severity
    assert decision.record.occurrence_count == 1
    assert decision.record.incident_generation == 1
    assert decision.event.event_type is AlertEventType.OPENED


@pytest.mark.parametrize(
    "state",
    [FreshnessState.WARNING, FreshnessState.STALE],
)
def test_repeated_unhealthy_deduplicates_and_increments_occurrence(
    state: FreshnessState,
) -> None:
    first = decide_alert_transition(_seed(), _condition(state), NOW)
    second = decide_alert_transition(
        first.record,
        _condition(state),
        NOW + timedelta(minutes=1),
    )
    assert second.record.occurrence_count == 2
    assert second.record.alert_key == first.record.alert_key
    assert second.event is None


def test_warning_to_stale_escalates_exactly_once() -> None:
    warning = decide_alert_transition(
        _seed(),
        _condition(FreshnessState.WARNING),
        NOW,
    )
    stale = decide_alert_transition(
        warning.record,
        _condition(FreshnessState.STALE),
        NOW + timedelta(minutes=1),
    )
    repeated = decide_alert_transition(
        stale.record,
        _condition(FreshnessState.STALE),
        NOW + timedelta(minutes=2),
    )
    assert stale.record.severity is AlertSeverity.CRITICAL
    assert stale.event.event_type is AlertEventType.ESCALATED
    assert repeated.event is None


def test_critical_does_not_downgrade_until_fresh() -> None:
    critical = decide_alert_transition(
        _seed(),
        _condition(FreshnessState.STALE),
        NOW,
    )
    warning = decide_alert_transition(
        critical.record,
        _condition(FreshnessState.WARNING),
        NOW + timedelta(minutes=1),
    )
    assert warning.record.severity is AlertSeverity.CRITICAL
    assert warning.event is None


def test_stale_to_fresh_resolves_once() -> None:
    opened = decide_alert_transition(
        _seed(),
        _condition(FreshnessState.STALE),
        NOW,
    )
    resolved = decide_alert_transition(
        opened.record,
        _condition(FreshnessState.FRESH, value=0),
        NOW + timedelta(minutes=1),
    )
    repeated = decide_alert_transition(
        resolved.record,
        _condition(FreshnessState.FRESH, value=0),
        NOW + timedelta(minutes=2),
    )
    assert resolved.record.state is AlertState.RESOLVED
    assert resolved.record.occurrence_count == 1
    assert resolved.event.event_type is AlertEventType.RESOLVED
    assert repeated.changed is False
    assert repeated.event is None


def test_resolved_alert_reopens_as_new_incident_generation() -> None:
    opened = decide_alert_transition(
        _seed(), _condition(FreshnessState.STALE), NOW
    )
    resolved = decide_alert_transition(
        opened.record,
        _condition(FreshnessState.FRESH, value=0),
        NOW + timedelta(minutes=1),
    )
    reopened = decide_alert_transition(
        resolved.record,
        _condition(FreshnessState.WARNING),
        NOW + timedelta(minutes=2),
    )
    assert reopened.record.incident_generation == 2
    assert reopened.record.occurrence_count == 1
    assert reopened.event.event_type is AlertEventType.OPENED


@pytest.mark.parametrize("recovery", ["SUCCEEDED", "NO_OP"])
def test_pipeline_failure_opens_repeats_and_recovers(recovery: str) -> None:
    failed = pipeline_failure_conditions({"aave_forward": "FAILED"})[0]
    healthy = pipeline_failure_conditions({"aave_forward": recovery})[0]
    seed = replace(
        _seed("pipeline_failure:aave_forward"),
        check_name="aave_forward_execution",
        reason_code=AlertReasonCode.PIPELINE_FAILURE,
    )
    first = decide_alert_transition(seed, failed, NOW)
    second = decide_alert_transition(first.record, failed, NOW + timedelta(minutes=1))
    resolved = decide_alert_transition(
        second.record,
        healthy,
        NOW + timedelta(minutes=2),
    )
    assert first.event.event_type is AlertEventType.OPENED
    assert second.record.occurrence_count == 2
    assert second.event is None
    assert resolved.event.event_type is AlertEventType.RESOLVED


def test_freshness_and_failure_alerts_are_independent() -> None:
    failure = pipeline_failure_conditions({"aave_forward": "FAILED"})[0]
    assert failure.alert_key == "pipeline_failure:aave_forward"
    assert _condition(FreshnessState.STALE).alert_key == "freshness:aave_forward"


def test_freshness_mapping_ignores_informational_and_aggregates_marts() -> None:
    threshold = FreshnessThreshold(10, 20, "seconds")

    def result(
        name: str,
        dimension: FreshnessDimension,
        state: FreshnessState,
        *,
        blocking: bool,
    ) -> FreshnessResult:
        return FreshnessResult(
            check_name=name,
            pipeline_name=(
                "aave_forward"
                if name.startswith("aave")
                else "daily_warehouse_pipeline"
            ),
            dimension=dimension,
            state=state,
            observed_value=20,
            threshold=threshold,
            checked_at=NOW,
            reason="test",
            blocking=blocking,
        )

    report = FreshnessReport(
        NOW,
        (
            result(
                "aave_forward_source_position",
                FreshnessDimension.SOURCE_POSITION,
                FreshnessState.STALE,
                blocking=True,
            ),
            result(
                "aave_forward_event_activity",
                FreshnessDimension.EVENT_TIME,
                FreshnessState.STALE,
                blocking=False,
            ),
            result(
                "mart_a_transformation",
                FreshnessDimension.TRANSFORMATION,
                FreshnessState.FRESH,
                blocking=True,
            ),
            result(
                "mart_b_transformation",
                FreshnessDimension.TRANSFORMATION,
                FreshnessState.WARNING,
                blocking=True,
            ),
        ),
    )
    conditions = freshness_conditions(report)
    assert [item.alert_key for item in conditions] == [
        "freshness:aave_forward",
        "freshness:marts",
    ]
    assert conditions[1].state is FreshnessState.WARNING


def test_optimistic_concurrency_rejects_stale_writer() -> None:
    store = MemoryAlertStore((_seed(),))
    candidate = decide_alert_transition(
        store.get("freshness:aave_forward"),
        _condition(FreshnessState.STALE),
        NOW,
    ).record
    store.records[candidate.alert_key] = replace(
        store.records[candidate.alert_key],
        state_version=1,
    )
    with pytest.raises(AlertConcurrencyError, match="stale"):
        store.compare_and_set(0, candidate)


def test_evaluator_retries_stale_writer_without_duplicate_open_event() -> None:
    class RacingStore(MemoryAlertStore):
        def __init__(self) -> None:
            super().__init__((_seed(),))
            self.first_attempt = True

        def compare_and_set(self, expected_version, candidate):
            if self.first_attempt:
                self.first_attempt = False
                # Another writer commits the same observation first.
                self.records[candidate.alert_key] = candidate
                raise AlertConcurrencyError("stale test writer")
            return super().compare_and_set(expected_version, candidate)

    store = RacingStore()
    decision = evaluate_alerts(
        store,
        (_condition(FreshnessState.STALE),),
        NOW,
    )[0]

    assert decision.record.state is AlertState.OPEN
    assert decision.record.occurrence_count == 2
    assert decision.record.state_version == 2
    assert decision.event is None
    assert len(store.records) == 1


def test_process_restart_uses_same_durable_record() -> None:
    store = MemoryAlertStore((_seed(),))
    first = evaluate_alerts(
        store,
        (_condition(FreshnessState.STALE),),
        NOW,
    )[0]
    restarted_service = store
    second = evaluate_alerts(
        restarted_service,
        (_condition(FreshnessState.STALE),),
        NOW + timedelta(minutes=1),
    )[0]
    assert first.event.event_type is AlertEventType.OPENED
    assert second.event is None
    assert second.record.occurrence_count == 2


def test_notification_payload_is_small_and_secret_free() -> None:
    event = decide_alert_transition(
        _seed(),
        _condition(FreshnessState.STALE),
        NOW,
    ).event
    payload = json.dumps(asdict(event), default=str)
    assert set(asdict(event)) == {
        "event_type",
        "alert_key",
        "pipeline_name",
        "check_name",
        "severity",
        "reason_code",
        "current_value",
        "warning_threshold",
        "stale_threshold",
        "opened_at",
        "resolved_at",
        "observed_at",
        "incident_generation",
    }
    for forbidden in ("password", "private_key", "rpc_url", "api_key", "exception"):
        assert forbidden not in payload.lower()


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _sql, _parameters):
        return self

    def fetchall(self):
        return self.rows

    def close(self):
        return None


class _Connection:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return _Cursor(self.rows)


def test_snowflake_store_detects_duplicate_physical_alert_keys() -> None:
    row = (
        "freshness:aave_forward",
        "aave_forward_source_position",
        "aave_forward",
        "SOURCE_POSITION_LAG",
        "RESOLVED",
        None,
        None,
        None,
        None,
        0,
        0,
        None,
        None,
        None,
        0,
    )
    store = SnowflakeAlertStore(_Connection([row, row]))
    with pytest.raises(DuplicateAlertKeyError, match="Duplicate"):
        store.get("freshness:aave_forward")


def test_alert_store_failure_is_visible_to_caller() -> None:
    class FailingStore(MemoryAlertStore):
        def compare_and_set(self, expected_version, candidate):
            raise RuntimeError("alert persistence unavailable")

    with pytest.raises(RuntimeError, match="persistence unavailable"):
        evaluate_alerts(
            FailingStore((_seed(),)),
            (_condition(FreshnessState.STALE),),
            NOW,
        )


def test_manual_provisioning_uses_preseeded_update_only_runtime() -> None:
    sql = (
        Path(__file__).parents[1]
        / "warehouse"
        / "provision_alert_state.sql"
    ).read_text(encoding="utf-8").upper()
    assert "BLOCKCHAIN_ANALYTICS.CONTROL.ALERT_STATE" in sql
    assert "GRANT SELECT, UPDATE" in sql
    assert "TO ROLE BLOCKCHAIN_PIPELINE_ROLE" in sql
    assert "GRANT INSERT" not in sql
    assert "GRANT DELETE" not in sql
    assert "GRANT CREATE" not in sql
    assert sql.count("('FRESHNESS:") == 9
    assert sql.count("('PIPELINE_FAILURE:") == 4
