"""Focused contracts for the local read-only operational dashboard."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from app.app import available_pages
from app.components.operations import (
    alert_display_rows,
    build_duration_chart,
    build_records_chart,
    feed_freshness_rows,
    filter_run_rows,
    operational_summary,
    run_display_rows,
    source_freshness_rows,
    status_label,
    warehouse_freshness_rows,
    watermark_display_rows,
)
from app.config import AppConfig
from app.data.operations import (
    OPERATIONS_ROLE,
    SnowflakeOperationsRepository,
)
from orchestration.freshness import (
    FreshnessDimension,
    FreshnessReport,
    FreshnessResult,
    FreshnessState,
    FreshnessThreshold,
)

NOW = datetime(2026, 10, 22, 3, tzinfo=UTC)


class FakeCursor:
    def __init__(
        self,
        *,
        identity: tuple[str, str] | None = None,
        rows: list[dict[str, Any]] | None = None,
        calls: list[tuple[str, dict[str, Any]]] | None = None,
    ) -> None:
        self.identity = identity
        self.rows = rows or []
        self.calls = calls if calls is not None else []
        self.closed = False

    def execute(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> FakeCursor:
        self.calls.append((query, params or {}))
        return self

    def fetchone(self) -> tuple[str, str] | None:
        return self.identity

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        calls: list[tuple[str, dict[str, Any]]],
    ) -> None:
        self.cursors = [
            FakeCursor(
                identity=(OPERATIONS_ROLE, '{"roles":"","value":""}'),
                calls=calls,
            ),
            FakeCursor(rows=rows, calls=calls),
        ]
        self.closed = False

    def cursor(self, cursor_class: Any = None) -> FakeCursor:
        return self.cursors.pop(0)

    def close(self) -> None:
        self.closed = True


class CapturingFactory:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.connection_kwargs: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> FakeConnection:
        self.connection_kwargs.append(kwargs)
        return FakeConnection(self.rows, self.calls)


def _result(
    check_name: str,
    pipeline_name: str,
    dimension: FreshnessDimension,
    state: FreshnessState,
    value: int,
    details: dict[str, Any],
) -> FreshnessResult:
    return FreshnessResult(
        check_name=check_name,
        pipeline_name=pipeline_name,
        dimension=dimension,
        state=state,
        observed_value=value,
        threshold=FreshnessThreshold(10, 20, "units"),
        checked_at=NOW,
        reason="test health",
        details=details,
    )


def _report() -> FreshnessReport:
    return FreshnessReport(
        NOW,
        (
            _result(
                "aave_forward_source_position",
                "aave_forward",
                FreshnessDimension.SOURCE_POSITION,
                FreshnessState.STALE,
                25,
                {
                    "source_head": 200,
                    "safe_head": 190,
                    "committed_position": 165,
                },
            ),
            _result(
                "uniswap_forward_source_position",
                "uniswap_forward",
                FreshnessDimension.SOURCE_POSITION,
                FreshnessState.FRESH,
                5,
                {
                    "source_head": 200,
                    "safe_head": 190,
                    "committed_position": 185,
                },
            ),
            _result(
                "chainlink_forward_source_position",
                "chainlink_forward",
                FreshnessDimension.SOURCE_POSITION,
                FreshnessState.WARNING,
                15,
                {
                    "source_head": 200,
                    "safe_head": 190,
                    "committed_position": 175,
                },
            ),
            _result(
                "chainlink_eth_usd",
                "chainlink_forward",
                FreshnessDimension.FEED,
                FreshnessState.STALE,
                25,
                {"latest_feed_updated_at": NOW},
            ),
            _result(
                "daily_warehouse_load",
                "daily_warehouse_pipeline",
                FreshnessDimension.LOAD,
                FreshnessState.FRESH,
                5,
                {"latest_successful_refresh": NOW},
            ),
            _result(
                "mart_uniswap_transformation",
                "daily_warehouse_pipeline",
                FreshnessDimension.TRANSFORMATION,
                FreshnessState.FRESH,
                5,
                {"latest_successful_refresh": NOW},
            ),
        ),
    )


def test_operations_page_imports_without_external_connections() -> None:
    from app.pages.operations import render_operations

    assert callable(render_operations)


def test_operations_is_local_only_and_absent_from_hosted_navigation() -> None:
    local = AppConfig()
    hosted = AppConfig(
        environment="hosted",
        connection_name=None,
        account="example",
        user="BLOCKCHAIN_APP_USER",
        private_key_pem="not-used-in-this-test",
        private_key_passphrase="not-used-in-this-test",
    )

    assert "Operations" in available_pages(local)
    assert "Operations" not in available_pages(hosted)
    with pytest.raises(ValueError, match="public hosted"):
        SnowflakeOperationsRepository(hosted)


def test_operations_connection_uses_pipeline_role_and_disables_secondary_roles() -> None:
    factory = CapturingFactory([])
    repository = SnowflakeOperationsRepository(AppConfig(), factory)

    repository.list_alerts()

    assert factory.connection_kwargs[0]["role"] == OPERATIONS_ROLE
    assert factory.connection_kwargs[0]["schema"] == "CONTROL"
    assert factory.calls[0][0] == "USE SECONDARY ROLES NONE"
    assert "CURRENT_SECONDARY_ROLES" in factory.calls[1][0]


def test_repository_queries_are_select_only_and_exclude_raw_metric_json() -> None:
    factory = CapturingFactory([])
    repository = SnowflakeOperationsRepository(AppConfig(), factory)

    repository.list_alerts()
    repository.list_watermarks(("0xfeed",))
    repository.list_recent_runs(pipeline="aave_forward", status="SUCCEEDED")

    application_queries = "\n".join(query for query, _ in factory.calls[2:]).upper()
    assert "SELECT" in application_queries
    for forbidden in (
        " INSERT ",
        " UPDATE ",
        " DELETE ",
        " MERGE ",
        " CREATE ",
        " TRUNCATE ",
        "CALL ",
    ):
        assert forbidden not in f" {application_queries} "
    assert "METRIC_DETAILS:DBT_MODELS" in application_queries
    assert "SELECT METRIC_DETAILS" not in application_queries


def test_recent_run_filters_are_parameterized_and_validated() -> None:
    factory = CapturingFactory([])
    repository = SnowflakeOperationsRepository(AppConfig(), factory)

    repository.list_recent_runs(
        pipeline="chainlink_forward",
        status="NO_OP",
        limit=25,
    )
    query, params = factory.calls[-1]

    assert "PIPELINE_NAME = %(pipeline)s" in query
    assert "STATUS = %(status)s" in query
    assert params["pipeline"] == "chainlink_forward"
    assert params["status"] == "NO_OP"
    assert params["limit"] == 25
    with pytest.raises(ValueError, match="not approved"):
        repository.list_recent_runs(pipeline="unknown")


def test_alert_rendering_preserves_lifecycle_context() -> None:
    rows = alert_display_rows(
        [
            {
                "alert_key": "freshness:aave_forward",
                "check_name": "aave_forward_source_position",
                "state": "OPEN",
                "severity": "CRITICAL",
                "reason_code": "SOURCE_POSITION_LAG",
                "current_value": 250,
                "warning_threshold": 50,
                "stale_threshold": 200,
                "opened_at": NOW,
                "last_observed_at": NOW,
                "occurrence_count": 3,
                "incident_generation": 1,
            }
        ]
    )

    assert rows[0]["Alert"] == "Aave source coverage"
    assert rows[0]["Severity"] == "Critical"
    assert rows[0]["Occurrences"] == 3
    assert rows[0]["Generation"] == 1


def test_freshness_rendering_reuses_report_measurements_and_thresholds() -> None:
    report = _report()

    sources = source_freshness_rows(report)
    feeds = feed_freshness_rows(report)
    warehouse = warehouse_freshness_rows(report)

    assert sources[0] == {
        "Source": "Aave",
        "Source head": 200,
        "Safe head": 190,
        "Committed": 165,
        "Eligible lag": 25,
        "Warning": 10,
        "Stale": 20,
        "State": "STALE",
    }
    assert feeds[0]["Feed"] == "ETH/USD"
    assert feeds[0]["Age (seconds)"] == 25
    assert len(warehouse) == 2


def test_watermark_rendering_separates_global_and_feed_state() -> None:
    primary, feeds = watermark_display_rows(
        [
            {
                "pipeline_name": "aave_forward",
                "source_name": "aave_v3_pool",
                "watermark_type": "block_number",
                "committed_value": 100,
                "successful_run_id": "run-1",
                "updated_at": NOW,
            },
            {
                "pipeline_name": "chainlink_forward",
                "source_name": "0xfeed",
                "watermark_type": "round_id",
                "committed_value": 200,
                "successful_run_id": "run-2",
                "updated_at": NOW,
            },
        ],
        {"0xfeed": "ETH/USD"},
    )

    assert primary[0]["Watermark"] == "Aave global block"
    assert primary[0]["Position"] == 100
    assert feeds[0]["Feed"] == "ETH/USD"
    assert feeds[0]["Round cursor"] == "200"


def test_recent_run_filtering_status_format_and_safe_fields() -> None:
    runs = [
        {
            "pipeline_name": "aave_forward",
            "status": "NO_OP",
            "run_type": "forward",
            "started_at": NOW,
            "finished_at": NOW,
            "duration_seconds": Decimal("3.5"),
            "made_progress": False,
            "records_written": 0,
            "records_inserted": 0,
            "records_updated": 0,
            "records_quarantined": 0,
            "retry_count": 0,
            "metric_details": {"private": "must-not-render"},
            "error_class": "must-not-render",
        },
        {"pipeline_name": "uniswap_forward", "status": "FAILED"},
    ]

    filtered = filter_run_rows(runs, pipeline="aave_forward", status="NO_OP")
    displayed = run_display_rows(filtered)

    assert len(filtered) == 1
    assert status_label("NO_OP") == "No Op"
    assert displayed[0]["Status"] == "No Op"
    assert "metric_details" not in displayed[0]
    assert "error_class" not in displayed[0]


def test_operational_summary_and_empty_states_remain_explicit() -> None:
    summary = operational_summary([], _report())

    assert summary == {
        "critical_alerts": 0,
        "warning_alerts": 0,
        "fresh_sources": 1,
        "warning_sources": 1,
        "stale_sources": 1,
    }
    assert alert_display_rows([]) == []
    assert run_display_rows([]) == []
    assert filter_run_rows([], pipeline=None, status=None) == []


def test_performance_charts_use_only_approved_run_metrics() -> None:
    runs = [
        {
            "pipeline_name": "aave_forward",
            "started_at": NOW,
            "duration_seconds": Decimal("1.25"),
            "records_written": 4,
        }
    ]

    duration = build_duration_chart(runs).to_dict()
    records = build_records_chart(runs).to_dict()

    assert duration["mark"]["type"] == "line"
    assert duration["data"]["values"][0]["Duration"] == 1.25
    assert records["mark"]["type"] == "bar"
    assert records["data"]["values"][0]["Records written"] == 4


def test_operational_error_is_user_safe_and_has_no_mock_fallback(monkeypatch) -> None:
    from app.pages import operations

    class FakeStreamlit:
        def __init__(self) -> None:
            self.errors: list[str] = []
            self.captions: list[str] = []

        def error(self, message: str) -> None:
            self.errors.append(message)

        def caption(self, message: str) -> None:
            self.captions.append(message)

    fake = FakeStreamlit()
    monkeypatch.setattr(operations, "st", fake)
    monkeypatch.setattr(operations, "render_page_intro", lambda *args: None)
    monkeypatch.setattr(
        operations,
        "load_operations_alerts",
        lambda _config: (_ for _ in ()).throw(RuntimeError("sensitive detail")),
    )

    operations.render_operations(AppConfig())

    output = " ".join((*fake.errors, *fake.captions))
    assert "Operational data is currently unavailable" in output
    assert "sensitive detail" not in output
    assert "fallback data" in output


def test_operations_source_contains_no_mutating_actions_or_secret_output() -> None:
    root = Path(__file__).parents[1]
    source = "\n".join(
        (root / path).read_text(encoding="utf-8")
        for path in (
            "app/pages/operations.py",
            "app/data/operations.py",
        )
    ).upper()

    for forbidden in (
        "TRIGGER_DAG",
        "COMMIT_WATERMARK",
        "ACQUIRE_LEASE",
        "RUN_RECOVERY",
        "PRIVATE_KEY",
        "PASSWORD",
    ):
        assert forbidden not in source
