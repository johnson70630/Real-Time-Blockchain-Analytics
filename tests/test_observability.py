"""Unit tests for durable pipeline run lifecycle and metric translation."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from orchestration.observability import (
    BestEffortRunRecorder,
    DuplicateRunError,
    ErrorStage,
    PipelineRunRecord,
    RunIdentity,
    RunMetrics,
    RunStateConflictError,
    RunStatus,
    RunType,
)
from orchestration.run_metrics import (
    aave_forward_metrics,
    chainlink_forward_metrics,
    uniswap_forward_metrics,
)
from orchestration.run_observer import PipelineRunObserver
from warehouse.pipeline_runs import SnowflakePipelineRunStore

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


class MemoryRunStore:
    def __init__(self) -> None:
        self.records: dict[RunIdentity, PipelineRunRecord] = {}
        self.fail_start = False
        self.fail_finish = False

    def start(self, record: PipelineRunRecord) -> PipelineRunRecord:
        if self.fail_start:
            raise ConnectionError("secret-bearing endpoint must not be logged")
        if record.identity in self.records:
            raise DuplicateRunError("duplicate")
        self.records[record.identity] = record
        return record

    def finish(self, record: PipelineRunRecord) -> PipelineRunRecord:
        if self.fail_finish:
            raise ConnectionError("secret-bearing endpoint must not be logged")
        current = self.records.get(record.identity)
        if current is None or current.status is not RunStatus.RUNNING:
            raise RunStateConflictError("not running")
        self.records[record.identity] = record
        return record


def _clock(values: list[datetime]):
    return lambda: values.pop(0)


def _timer(values: list[float]):
    return lambda: values.pop(0)


def _recorder(store: MemoryRunStore) -> BestEffortRunRecorder:
    return BestEffortRunRecorder(
        store,
        RunIdentity("aave_forward", "run-1"),
        RunType.FORWARD,
        clock=_clock([NOW, NOW + timedelta(seconds=2)]),
        timer=_timer([10.0, 12.25]),
    )


def test_successful_run_records_progress_rows_and_duration() -> None:
    store = MemoryRunStore()
    recorder = _recorder(store)
    metrics = RunMetrics(
        source_start_position=101,
        source_end_position=200,
        source_head_position=1_400,
        safe_head_position=200,
        watermark_before=100,
        watermark_after=200,
        records_read=9,
        records_written=9,
        records_inserted=8,
        records_updated=1,
        records_quarantined=0,
        details={"borrow_records": 5, "repay_records": 4},
    )

    terminal = recorder.succeed(metrics)

    assert terminal.status is RunStatus.SUCCEEDED
    assert terminal.duration_seconds == 2.25
    assert terminal.metrics.made_progress
    assert store.records[terminal.identity] == terminal


def test_noop_run_is_terminal_without_progress() -> None:
    store = MemoryRunStore()
    recorder = _recorder(store)
    terminal = recorder.noop(RunMetrics(watermark_before=100, watermark_after=100))

    assert terminal.status is RunStatus.NO_OP
    assert not terminal.metrics.made_progress
    assert terminal.metrics.records_written == 0


def test_failed_run_stores_only_sanitized_class_and_stage() -> None:
    store = MemoryRunStore()
    recorder = _recorder(store)
    recorder.set_stage(ErrorStage.SOURCE_FETCH)
    error = RuntimeError("https://secret.example/api-key=do-not-store")

    terminal = recorder.fail(error)

    assert terminal.status is RunStatus.FAILED
    assert terminal.error_stage is ErrorStage.SOURCE_FETCH
    assert terminal.error_class == "RuntimeError"
    assert "secret" not in repr(terminal)


def test_process_crash_leaves_detectable_running_record() -> None:
    store = MemoryRunStore()
    recorder = _recorder(store)

    assert store.records[recorder.record.identity].status is RunStatus.RUNNING
    assert store.records[recorder.record.identity].finished_at is None


def test_duplicate_run_identity_is_detected_by_store() -> None:
    store = MemoryRunStore()
    first = _recorder(store)
    duplicate = BestEffortRunRecorder(
        store,
        first.record.identity,
        RunType.FORWARD,
        clock=lambda: NOW,
        timer=lambda: 0.0,
    )

    assert duplicate.store is None
    assert len(store.records) == 1


def test_metrics_store_failure_does_not_change_pipeline_result(caplog) -> None:
    store = MemoryRunStore()
    recorder = _recorder(store)
    store.fail_finish = True

    terminal = recorder.succeed(RunMetrics(records_written=3))

    assert terminal.status is RunStatus.SUCCEEDED
    assert terminal.metrics.records_written == 3
    assert recorder.store is None
    assert "secret-bearing" not in caplog.text
    assert "ConnectionError" in caplog.text


def test_metric_details_reject_secret_shaped_values() -> None:
    with pytest.raises(TypeError, match="must be an integer"):
        RunMetrics(details={"rpc_url": "https://secret.example"})  # type: ignore[arg-type]


def test_aave_instrumentation_preserves_existing_capture_counts() -> None:
    capture = SimpleNamespace(
        start_block=101,
        end_block=200,
        observed_head=1_400,
        safe_head=200,
        committed_block=100,
        logical_ranges=2,
        completed_ranges=2,
        range_attempts=3,
        adaptive_splits=1,
        event_counts=Counter(borrow=4, repay=3, liquidation=1),
        quarantine_count=2,
    )

    metrics = aave_forward_metrics(capture, watermark_after=200)

    assert metrics.records_read == 8
    assert metrics.records_quarantined == 2
    assert metrics.details["borrow_records"] == 4
    assert metrics.details["rpc_requests"] == 3


@dataclass(frozen=True)
class Batch:
    replayed: bool


def test_chainlink_instrumentation_records_global_and_feed_counts() -> None:
    plan = SimpleNamespace(
        committed_block=100,
        round_count=3,
        snapshot=SimpleNamespace(
            observed_head=1_400,
            block=SimpleNamespace(number=200),
        ),
    )
    capture = SimpleNamespace(
        plan=plan,
        observations_by_feed={"0x2": 1, "0x1": 2},
        rpc_requests=5,
        batches=(Batch(False), Batch(True)),
    )

    metrics = chainlink_forward_metrics(capture, watermark_after=200)

    assert metrics.watermark_before == 100
    assert metrics.watermark_after == 200
    assert metrics.records_written == 3
    assert metrics.details["feed_1_observations"] == 2
    assert metrics.details["replayed_batches"] == 1


def test_uniswap_instrumentation_records_event_families_and_overlap() -> None:
    capture = SimpleNamespace(
        plan=SimpleNamespace(
            start_block=101,
            end_block=200,
            safe_head=200,
            committed_block=100,
        ),
        observed_head=1_400,
        logical_ranges=2,
        completed_ranges=2,
        query_requests=7,
        adaptive_splits=0,
        pool_count=68,
        event_counts=Counter(swap=8, mint=2, burn=1),
        quarantine_count=0,
    )

    metrics = uniswap_forward_metrics(
        capture,
        watermark_after=200,
        silver_records=8,
        live_overlap_records=3,
    )

    assert metrics.records_read == 14
    assert metrics.records_written == 11
    assert metrics.details["live_overlap_records"] == 3
    assert metrics.details["silver_records"] == 8
    assert metrics.details["net_new_canonical_records"] == 8


def test_forward_child_raw_metrics_do_not_double_count_source_records(
    monkeypatch,
) -> None:
    persisted = PipelineRunRecord(
        RunIdentity("chainlink_forward", "run-1"),
        RunType.FORWARD,
        NOW,
        metrics=RunMetrics(
            records_read=3,
            records_written=3,
            records_inserted=3,
            details={"copy_files": 1},
        ),
    )

    class FakeSnowflakeStore:
        def get(self, _identity):
            return persisted

    monkeypatch.setattr(
        "orchestration.run_observer.SnowflakePipelineRunStore",
        FakeSnowflakeStore,
    )
    observer = object.__new__(PipelineRunObserver)
    observer.pipeline_name = "chainlink_forward"
    observer.run_id = "run-1"
    observer.recorder = SimpleNamespace(
        store=FakeSnowflakeStore(),
        record=PipelineRunRecord(
            RunIdentity("chainlink_forward", "run-1"),
            RunType.FORWARD,
            NOW,
        ),
    )

    combined = observer._combined_metrics(
        RunMetrics(records_read=3, records_written=3)
    )

    assert combined.records_read == 3
    assert combined.records_written == 3
    assert combined.records_inserted == 3
    assert combined.details["copy_files"] == 1


class FakeCursor:
    def __init__(self, rowcounts: list[int]) -> None:
        self.rowcounts = rowcounts
        self.rowcount = -1
        self.calls: list[tuple[str, tuple | None]] = []

    def execute(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        self.rowcount = self.rowcounts.pop(0)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def close(self):
        return None


class FakeConnection:
    def __init__(self, rowcounts: list[int]) -> None:
        self.cursor_instance = FakeCursor(rowcounts)

    def cursor(self):
        return self.cursor_instance


def test_snowflake_store_uses_insert_guard_and_running_finish_guard() -> None:
    connection = FakeConnection([1, 1])
    store = SnowflakePipelineRunStore(connection)
    recorder = _recorder(MemoryRunStore())
    running = recorder.record

    store.start(running)
    terminal = recorder.succeed(RunMetrics(watermark_before=1, watermark_after=2))
    store.finish(terminal)

    start_sql = connection.cursor_instance.calls[0][0]
    finish_sql = connection.cursor_instance.calls[1][0]
    assert "WHERE NOT EXISTS" in start_sql
    assert "AND status = 'RUNNING'" in finish_sql
    finish_parameters = connection.cursor_instance.calls[1][1]
    assert "RuntimeError" not in str(finish_parameters)


def test_snowflake_store_rejects_duplicate_and_terminal_conflict() -> None:
    running = PipelineRunRecord(
        RunIdentity("aave_forward", "duplicate"),
        RunType.FORWARD,
        NOW,
    )
    with pytest.raises(DuplicateRunError):
        SnowflakePipelineRunStore(FakeConnection([0])).start(running)

    recorder = BestEffortRunRecorder(
        MemoryRunStore(),
        running.identity,
        RunType.FORWARD,
        clock=_clock([NOW, NOW + timedelta(seconds=1)]),
        timer=_timer([0.0, 1.0]),
    )
    terminal = recorder.succeed(RunMetrics())
    with pytest.raises(RunStateConflictError):
        SnowflakePipelineRunStore(FakeConnection([0])).finish(terminal)
