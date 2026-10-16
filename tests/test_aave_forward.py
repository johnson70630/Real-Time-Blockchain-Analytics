"""Tests for contiguous Aave bootstrap planning and failure semantics."""

from datetime import UTC, datetime

import pytest

from backfill.aave_v3 import BackfillIssue, RpcBlockRange
from backfill.run_aave_forward import _persist_completed_batch
from config.settings import (
    get_aave_bootstrap_chunk_size,
    get_aave_bootstrap_max_range_attempts,
    get_aave_bootstrap_max_range_size,
    get_aave_bootstrap_min_range_size,
    get_aave_confirmation_depth,
    get_aave_forward_max_blocks_per_run,
    get_aave_project_collection_start_at,
    get_aave_project_collection_start_block,
)
from orchestration.aave_forward import (
    CompletedBlockRange,
    calculate_safe_head,
    combined_aave_coverage,
    eligible_bootstrap_watermark,
    highest_contiguous_block,
    plan_next_bootstrap_chunk,
    plan_forward_increment,
    process_next_bootstrap_chunk,
    run_commit_last_pipeline,
)
from orchestration.watermark import (
    AAVE_FORWARD_WATERMARK,
    AAVE_LEGACY_BOOTSTRAP_RANGES,
    WatermarkIdentity,
    WatermarkState,
)

NOW = datetime(2026, 10, 13, tzinfo=UTC)


class RecordingBatchStore:
    def __init__(self, calls: list[str], *, fail: bool = False) -> None:
        self.calls = calls
        self.fail = fail

    def write_and_validate(self, **_kwargs) -> None:
        self.calls.append("persist")
        if self.fail:
            raise RuntimeError("source persistence failed")


class RecordingBatchLedger:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.ranges = ()

    def record_ranges(self, identity, ranges) -> None:
        assert identity == AAVE_LEGACY_BOOTSTRAP_RANGES
        self.calls.append("ledger")
        self.ranges = ranges


class MemoryRangeStore:
    """Append-only completed-range store used by planner tests."""

    def __init__(
        self,
        ranges: tuple[CompletedBlockRange, ...] = (),
    ) -> None:
        self.ranges = list(ranges)

    def list_ranges(
        self,
        identity: WatermarkIdentity,
    ) -> tuple[CompletedBlockRange, ...]:
        assert identity == AAVE_LEGACY_BOOTSTRAP_RANGES
        return tuple(self.ranges)

    def record_range(
        self,
        identity: WatermarkIdentity,
        completed_range: CompletedBlockRange,
    ) -> CompletedBlockRange:
        assert identity == AAVE_LEGACY_BOOTSTRAP_RANGES
        if completed_range not in self.ranges:
            self.ranges.append(completed_range)
        return completed_range


class RecordingProcessor:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple[int, int]] = []

    def process(self, start_block: int, end_block: int) -> None:
        self.calls.append((start_block, end_block))
        if self.fail:
            raise RuntimeError("source persistence failed")


class CommitRecordingStore:
    def __init__(self, state: WatermarkState) -> None:
        self.state = state
        self.advance_calls = 0

    def get(self, identity: WatermarkIdentity) -> WatermarkState | None:
        assert identity == self.state.identity
        return self.state

    def initialize(self, *_args, **_kwargs) -> WatermarkState:
        raise AssertionError("initialization is outside forward processing")

    def advance(
        self,
        identity: WatermarkIdentity,
        *,
        expected_value: int,
        candidate_value: int,
        successful_run_id: str,
    ) -> WatermarkState:
        assert identity == self.state.identity
        assert expected_value == self.state.committed_value
        self.advance_calls += 1
        self.state = WatermarkState(
            identity,
            candidate_value,
            successful_run_id,
            NOW,
        )
        return self.state


def test_aave_forward_configuration_defaults_and_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AAVE_PROJECT_COLLECTION_START_AT", raising=False)
    monkeypatch.delenv("AAVE_PROJECT_COLLECTION_START_BLOCK", raising=False)
    monkeypatch.delenv("AAVE_CONFIRMATION_DEPTH", raising=False)
    monkeypatch.delenv("AAVE_BOOTSTRAP_CHUNK_SIZE", raising=False)
    monkeypatch.delenv("AAVE_BOOTSTRAP_MIN_RANGE_SIZE", raising=False)
    monkeypatch.delenv("AAVE_BOOTSTRAP_MAX_RANGE_SIZE", raising=False)
    monkeypatch.delenv("AAVE_BOOTSTRAP_MAX_RANGE_ATTEMPTS", raising=False)
    monkeypatch.delenv("AAVE_FORWARD_MAX_BLOCKS_PER_RUN", raising=False)
    assert get_aave_project_collection_start_at() == datetime(
        2026, 10, 1, tzinfo=UTC
    )
    assert get_aave_project_collection_start_block() == 510_496_864
    assert get_aave_confirmation_depth() == 1_200
    assert get_aave_bootstrap_chunk_size() == 3_000
    assert get_aave_bootstrap_min_range_size() == 100
    assert get_aave_bootstrap_max_range_size() == 3_000
    assert get_aave_bootstrap_max_range_attempts() == 64
    assert get_aave_forward_max_blocks_per_run() == 600_000

    monkeypatch.setenv("AAVE_PROJECT_COLLECTION_START_AT", "2026-10-01")
    monkeypatch.setenv("AAVE_PROJECT_COLLECTION_START_BLOCK", "-1")
    monkeypatch.setenv("AAVE_CONFIRMATION_DEPTH", "-1")
    monkeypatch.setenv("AAVE_BOOTSTRAP_CHUNK_SIZE", "0")
    monkeypatch.setenv("AAVE_BOOTSTRAP_MIN_RANGE_SIZE", "0")
    with pytest.raises(ValueError, match="timezone"):
        get_aave_project_collection_start_at()
    with pytest.raises(ValueError, match="negative"):
        get_aave_project_collection_start_block()
    with pytest.raises(ValueError, match="negative"):
        get_aave_confirmation_depth()
    with pytest.raises(ValueError, match="greater than zero"):
        get_aave_bootstrap_chunk_size()
    with pytest.raises(ValueError, match="greater than zero"):
        get_aave_bootstrap_min_range_size()


def test_safe_head_applies_configured_lag() -> None:
    assert calculate_safe_head(10_000, 1_200) == 8_800
    assert calculate_safe_head(100, 1_200) == 0


@pytest.mark.parametrize(
    ("ranges", "expected"),
    [
        (((100, 109), (110, 119)), 119),
        (((100, 110), (105, 119)), 119),
        (((100, 109), (120, 129)), 109),
        (((90, 105), (106, 110), (115, 120)), 110),
        (((100, 109), (120, 129), (140, 149)), 109),
    ],
)
def test_highest_contiguous_block_handles_adjacent_overlaps_and_gaps(
    ranges: tuple[tuple[int, int], ...],
    expected: int,
) -> None:
    completed = tuple(CompletedBlockRange(*item) for item in ranges)
    assert highest_contiguous_block(100, completed) == expected


def test_plan_resumes_at_first_uncovered_block() -> None:
    plan = plan_next_bootstrap_chunk(
        project_start_block=100,
        safe_end_block=150,
        chunk_size=10,
        completed_ranges=(
            CompletedBlockRange(120, 129),
            CompletedBlockRange(100, 109),
        ),
    )

    assert plan.highest_contiguous_block == 109
    assert plan.next_range == CompletedBlockRange(110, 119)
    assert not plan.is_complete


def test_failed_chunk_is_not_recorded_and_is_retried() -> None:
    store = MemoryRangeStore()
    processor = RecordingProcessor(fail=True)

    with pytest.raises(RuntimeError, match="persistence failed"):
        process_next_bootstrap_chunk(
            store,
            processor,
            project_start_block=100,
            safe_end_block=119,
            chunk_size=10,
            successful_run_id="failed-run",
            completed_at=NOW,
        )

    assert store.ranges == []
    retry = plan_next_bootstrap_chunk(
        project_start_block=100,
        safe_end_block=119,
        chunk_size=10,
        completed_ranges=store.list_ranges(AAVE_LEGACY_BOOTSTRAP_RANGES),
    )
    assert retry.next_range == CompletedBlockRange(100, 109)


def test_successful_chunk_is_recorded_after_processing() -> None:
    store = MemoryRangeStore()
    processor = RecordingProcessor()

    next_plan = process_next_bootstrap_chunk(
        store,
        processor,
        project_start_block=100,
        safe_end_block=119,
        chunk_size=10,
        successful_run_id="bootstrap-run",
        completed_at=NOW,
    )

    assert processor.calls == [(100, 109)]
    assert store.ranges == [
        CompletedBlockRange(100, 109, "bootstrap-run", NOW)
    ]
    assert next_plan.next_range == CompletedBlockRange(110, 119)


def test_watermark_is_eligible_only_for_gap_free_safe_end() -> None:
    incomplete = (CompletedBlockRange(100, 109), CompletedBlockRange(120, 129))
    complete = (*incomplete, CompletedBlockRange(110, 119))

    assert (
        eligible_bootstrap_watermark(
            project_start_block=100,
            safe_end_block=129,
            completed_ranges=incomplete,
        )
        is None
    )
    assert (
        eligible_bootstrap_watermark(
            project_start_block=100,
            safe_end_block=129,
            completed_ranges=complete,
        )
        == 129
    )
    assert (
        eligible_bootstrap_watermark(
            project_start_block=100,
            safe_end_block=125,
            completed_ranges=complete,
        )
        == 125
    )


def test_completed_range_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="invalid"):
        CompletedBlockRange(10, 9)
    with pytest.raises(ValueError, match="timezone-aware"):
        CompletedBlockRange(10, 19, "run", datetime(2026, 10, 13))


def test_forward_batch_is_persisted_before_ledger_evidence() -> None:
    calls: list[str] = []
    ledger = RecordingBatchLedger(calls)

    _persist_completed_batch(
        storage=RecordingBatchStore(calls),
        ledger=ledger,
        run_id="acceptance-run",
        ranges=(RpcBlockRange(100, 109),),
        events=(),
        issues=(),
        processed_at=NOW,
    )

    assert calls == ["persist", "ledger"]
    assert ledger.ranges == (
        CompletedBlockRange(100, 109, "acceptance-run", NOW),
    )


def test_failed_forward_batch_never_records_ledger_evidence() -> None:
    calls: list[str] = []
    ledger = RecordingBatchLedger(calls)

    with pytest.raises(RuntimeError, match="source persistence failed"):
        _persist_completed_batch(
            storage=RecordingBatchStore(calls, fail=True),
            ledger=ledger,
            run_id="failed-run",
            ranges=(RpcBlockRange(100, 109),),
            events=(),
            issues=(BackfillIssue(None, None, None, "bad log", NOW),),
            processed_at=NOW,
        )

    assert calls == ["persist"]
    assert ledger.ranges == ()


def test_forward_identity_is_provider_neutral_and_legacy_evidence_is_stable() -> None:
    assert AAVE_FORWARD_WATERMARK.source_name == "aave_v3_pool"
    assert AAVE_LEGACY_BOOTSTRAP_RANGES.source_name == "alchemy_rpc"
    assert AAVE_FORWARD_WATERMARK.pipeline_name == "aave_forward"
    assert AAVE_LEGACY_BOOTSTRAP_RANGES.pipeline_name == "aave_forward"


def test_forward_plan_is_finite_contiguous_and_supports_noop() -> None:
    plan = plan_forward_increment(
        committed_block=200,
        safe_head=250,
        max_blocks=20,
    )
    assert (plan.start_block, plan.end_block, plan.block_count) == (201, 220, 20)
    assert not plan.is_noop

    noop = plan_forward_increment(
        committed_block=250,
        safe_head=250,
        max_blocks=20,
    )
    assert noop.is_noop
    assert noop.block_count == 0


def test_combined_coverage_bridges_legacy_bootstrap_and_forward_ranges() -> None:
    coverage = combined_aave_coverage(
        (CompletedBlockRange(100, 199),),
        (CompletedBlockRange(200, 249),),
    )
    assert highest_contiguous_block(100, coverage) == 249


@pytest.mark.parametrize(
    "failed_stage",
    ("source", "silver", "snowflake", "dbt"),
)
def test_forward_failures_never_commit_watermark(failed_stage: str) -> None:
    initial = WatermarkState(AAVE_FORWARD_WATERMARK, 100, "bootstrap", NOW)
    store = CommitRecordingStore(initial)
    calls: list[str] = []

    def stage(name: str, result=None):
        def invoke(*_args):
            calls.append(name)
            if name == failed_stage:
                raise RuntimeError(f"{name} failed")
            return result

        return invoke

    with pytest.raises(RuntimeError, match="failed"):
        run_commit_last_pipeline(
            store=store,
            current=initial,
            plan=plan_forward_increment(
                committed_block=100,
                safe_head=110,
                max_blocks=20,
            ),
            successful_run_id="forward-1",
            persist_source=stage("source", "batch"),
            build_silver=stage("silver"),
            load_snowflake=stage("snowflake"),
            run_dbt_and_quality=stage("dbt"),
        )

    assert store.state == initial
    assert store.advance_calls == 0


def test_forward_pipeline_commits_last_and_noop_has_no_side_effects() -> None:
    initial = WatermarkState(AAVE_FORWARD_WATERMARK, 100, "bootstrap", NOW)
    store = CommitRecordingStore(initial)
    calls: list[str] = []

    updated = run_commit_last_pipeline(
        store=store,
        current=initial,
        plan=plan_forward_increment(
            committed_block=100,
            safe_head=110,
            max_blocks=20,
        ),
        successful_run_id="forward-1",
        persist_source=lambda _plan: calls.append("source") or "batch",
        build_silver=lambda _batch: calls.append("silver"),
        load_snowflake=lambda: calls.append("snowflake"),
        run_dbt_and_quality=lambda: calls.append("dbt"),
    )
    assert calls == ["source", "silver", "snowflake", "dbt"]
    assert updated.committed_value == 110
    assert store.advance_calls == 1

    calls.clear()
    unchanged = run_commit_last_pipeline(
        store=store,
        current=updated,
        plan=plan_forward_increment(
            committed_block=110,
            safe_head=110,
            max_blocks=20,
        ),
        successful_run_id="noop",
        persist_source=lambda _plan: calls.append("source"),
        build_silver=lambda _batch: calls.append("silver"),
        load_snowflake=lambda: calls.append("snowflake"),
        run_dbt_and_quality=lambda: calls.append("dbt"),
    )
    assert unchanged == updated
    assert calls == []
    assert store.advance_calls == 1
