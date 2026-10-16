"""Planning contracts for contiguous Aave forward source coverage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from collections.abc import Callable
from typing import Protocol, TypeVar

from orchestration.watermark import (
    AAVE_LEGACY_BOOTSTRAP_RANGES,
    WatermarkIdentity,
    WatermarkState,
    WatermarkStore,
    commit_watermark,
)

T = TypeVar("T")


@dataclass(frozen=True, slots=True, order=True)
class CompletedBlockRange:
    """One inclusive source interval persisted successfully by a job run."""

    start_block: int
    end_block: int
    successful_run_id: str = ""
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        for field_name in ("start_block", "end_block"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field_name} must be an integer")
        if self.start_block < 0 or self.end_block < self.start_block:
            raise ValueError("Completed block range is invalid")
        if self.completed_at is not None and self.completed_at.tzinfo is None:
            raise ValueError("completed_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class AaveBootstrapPlan:
    """The next finite, gap-first bootstrap chunk."""

    project_start_block: int
    safe_end_block: int
    next_range: CompletedBlockRange | None
    highest_contiguous_block: int

    @property
    def is_complete(self) -> bool:
        """Return whether every block through the safe end is covered."""
        return self.highest_contiguous_block >= self.safe_end_block


@dataclass(frozen=True, slots=True)
class AaveForwardPlan:
    """One frozen, finite normal-forward interval."""

    committed_block: int
    safe_head: int
    start_block: int | None
    end_block: int | None

    @property
    def is_noop(self) -> bool:
        """Return whether the source has no newly eligible confirmed blocks."""
        return self.start_block is None

    @property
    def block_count(self) -> int:
        """Return the inclusive number of planned blocks."""
        if self.is_noop:
            return 0
        assert self.start_block is not None and self.end_block is not None
        return self.end_block - self.start_block + 1


class CompletedRangeStore(Protocol):
    """Persistence boundary for append-only completed source ranges."""

    def list_ranges(
        self,
        identity: WatermarkIdentity,
    ) -> tuple[CompletedBlockRange, ...]: ...

    def record_range(
        self,
        identity: WatermarkIdentity,
        completed_range: CompletedBlockRange,
    ) -> CompletedBlockRange: ...


class AaveChunkProcessor(Protocol):
    """Source and Bronze persistence boundary for one finite range."""

    def process(self, start_block: int, end_block: int) -> None: ...


def calculate_safe_head(latest_block: int, confirmation_depth: int) -> int:
    """Apply a configured operational lag without producing negatives."""
    if isinstance(latest_block, bool) or not isinstance(latest_block, int):
        raise TypeError("latest_block must be an integer")
    if isinstance(confirmation_depth, bool) or not isinstance(
        confirmation_depth, int
    ):
        raise TypeError("confirmation_depth must be an integer")
    if latest_block < 0:
        raise ValueError("latest_block must not be negative")
    if confirmation_depth < 0:
        raise ValueError("confirmation_depth must not be negative")
    return max(0, latest_block - confirmation_depth)


def plan_forward_increment(
    *,
    committed_block: int,
    safe_head: int,
    max_blocks: int,
) -> AaveForwardPlan:
    """Plan only the next uncommitted interval, capped for finite execution."""
    if committed_block < 0 or safe_head < 0:
        raise ValueError("Block values must not be negative")
    if max_blocks <= 0:
        raise ValueError("max_blocks must be greater than zero")
    if safe_head <= committed_block:
        return AaveForwardPlan(committed_block, safe_head, None, None)
    start_block = committed_block + 1
    return AaveForwardPlan(
        committed_block,
        safe_head,
        start_block,
        min(safe_head, start_block + max_blocks - 1),
    )


def combined_aave_coverage(
    legacy_ranges: tuple[CompletedBlockRange, ...],
    forward_ranges: tuple[CompletedBlockRange, ...],
) -> tuple[CompletedBlockRange, ...]:
    """Combine immutable bootstrap evidence with provider-neutral progress."""
    return (*legacy_ranges, *forward_ranges)


def highest_contiguous_block(
    project_start_block: int,
    completed_ranges: tuple[CompletedBlockRange, ...],
) -> int:
    """Return the highest block covered without a gap from project start."""
    if project_start_block < 0:
        raise ValueError("project_start_block must not be negative")
    contiguous_end = project_start_block - 1
    for completed_range in sorted(
        completed_ranges,
        key=lambda item: (item.start_block, item.end_block),
    ):
        if completed_range.end_block < project_start_block:
            continue
        if completed_range.start_block > contiguous_end + 1:
            break
        contiguous_end = max(contiguous_end, completed_range.end_block)
    return contiguous_end


def plan_next_bootstrap_chunk(
    *,
    project_start_block: int,
    safe_end_block: int,
    chunk_size: int,
    completed_ranges: tuple[CompletedBlockRange, ...],
) -> AaveBootstrapPlan:
    """Plan from the first uncovered block and never jump across a gap."""
    if safe_end_block < project_start_block:
        raise ValueError("safe_end_block precedes project_start_block")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    contiguous_end = highest_contiguous_block(
        project_start_block,
        completed_ranges,
    )
    if contiguous_end >= safe_end_block:
        next_range = None
    else:
        next_start = contiguous_end + 1
        next_range = CompletedBlockRange(
            next_start,
            min(safe_end_block, next_start + chunk_size - 1),
        )
    return AaveBootstrapPlan(
        project_start_block=project_start_block,
        safe_end_block=safe_end_block,
        next_range=next_range,
        highest_contiguous_block=contiguous_end,
    )


def process_next_bootstrap_chunk(
    store: CompletedRangeStore,
    processor: AaveChunkProcessor,
    *,
    project_start_block: int,
    safe_end_block: int,
    chunk_size: int,
    successful_run_id: str,
    completed_at: datetime,
) -> AaveBootstrapPlan:
    """Persist source output before appending its successful range record."""
    ranges = store.list_ranges(AAVE_LEGACY_BOOTSTRAP_RANGES)
    plan = plan_next_bootstrap_chunk(
        project_start_block=project_start_block,
        safe_end_block=safe_end_block,
        chunk_size=chunk_size,
        completed_ranges=ranges,
    )
    if plan.next_range is None:
        return plan
    if completed_at.tzinfo is None:
        raise ValueError("completed_at must be timezone-aware")

    processor.process(
        plan.next_range.start_block,
        plan.next_range.end_block,
    )
    store.record_range(
        AAVE_LEGACY_BOOTSTRAP_RANGES,
        CompletedBlockRange(
            plan.next_range.start_block,
            plan.next_range.end_block,
            successful_run_id,
            completed_at.astimezone(UTC),
        ),
    )
    return plan_next_bootstrap_chunk(
        project_start_block=project_start_block,
        safe_end_block=safe_end_block,
        chunk_size=chunk_size,
        completed_ranges=store.list_ranges(AAVE_LEGACY_BOOTSTRAP_RANGES),
    )


def eligible_bootstrap_watermark(
    *,
    project_start_block: int,
    safe_end_block: int,
    completed_ranges: tuple[CompletedBlockRange, ...],
) -> int | None:
    """Return the safe end only when the complete interval is gap-free."""
    contiguous_end = highest_contiguous_block(
        project_start_block,
        completed_ranges,
    )
    return safe_end_block if contiguous_end >= safe_end_block else None


def run_commit_last_pipeline(
    *,
    store: WatermarkStore,
    current: WatermarkState,
    plan: AaveForwardPlan,
    successful_run_id: str,
    persist_source: Callable[[AaveForwardPlan], T],
    build_silver: Callable[[T], None],
    load_snowflake: Callable[[], None],
    run_dbt_and_quality: Callable[[], None],
) -> WatermarkState:
    """Execute every required boundary before the optimistic watermark commit."""
    if plan.committed_block != current.committed_value:
        raise ValueError("Plan does not start from the supplied watermark state")
    if plan.is_noop:
        return current
    assert plan.end_block is not None
    source_result = persist_source(plan)
    build_silver(source_result)
    load_snowflake()
    run_dbt_and_quality()
    return commit_watermark(
        store,
        current.identity,
        plan.end_block,
        successful_run_id,
        processing_succeeded=True,
    )
