"""Plan and validate gap-free Chainlink forward price processing."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol, TypeVar

from backfill.chainlink import (
    FeedRoundRange,
    encode_proxy_round_id,
    split_proxy_round_id,
)
from market_data.feeds import ChainlinkFeed
from market_data.models import MarketDataObservation
from market_data.rpc import BlockSnapshot, ChainlinkRound
from orchestration.aave_forward import calculate_safe_head
from orchestration.watermark import (
    AtomicWatermarkStore,
    CHAINLINK_FORWARD_WATERMARK,
    WatermarkIdentity,
    WatermarkState,
    chainlink_feed_watermark,
    commit_watermarks_atomically,
)

T = TypeVar("T")


class ChainlinkForwardRpc(Protocol):
    """RPC operations required to freeze and plan all configured feeds."""

    def get_latest_block(self) -> BlockSnapshot: ...

    def get_block_snapshot(self, block_number: int) -> BlockSnapshot: ...

    def get_round_at_block(
        self, feed_address: str, block_number: int
    ) -> ChainlinkRound: ...

    def get_phase_latest_round(self, feed_address: str, phase_id: int) -> int: ...


@dataclass(frozen=True, slots=True)
class FeedTarget:
    """Latest observable proxy round for one feed at a frozen safe block."""

    feed: ChainlinkFeed
    round: ChainlinkRound

    @property
    def round_id(self) -> int:
        return self.round.round_id

    @property
    def identity(self) -> WatermarkIdentity:
        return chainlink_feed_watermark(self.feed.feed_address)


@dataclass(frozen=True, slots=True)
class FrozenChainlinkSnapshot:
    """One consistent block snapshot shared by every configured feed."""

    observed_head: int
    confirmation_depth: int
    block: BlockSnapshot
    targets: tuple[FeedTarget, ...]


@dataclass(frozen=True, slots=True)
class ChainlinkForwardPlan:
    """Exact missing round ranges required before advancing global coverage."""

    committed_block: int
    snapshot: FrozenChainlinkSnapshot
    ranges: tuple[FeedRoundRange, ...]

    @property
    def is_noop(self) -> bool:
        return self.snapshot.block.number <= self.committed_block

    @property
    def round_count(self) -> int:
        return sum(
            split_proxy_round_id(item.end_round_id)[1]
            - split_proxy_round_id(item.start_round_id)[1]
            + 1
            for item in self.ranges
        )

    @property
    def candidates(self) -> dict[WatermarkIdentity, int]:
        values = {
            target.identity: target.round_id for target in self.snapshot.targets
        }
        values[CHAINLINK_FORWARD_WATERMARK] = self.snapshot.block.number
        return values


@dataclass(frozen=True, slots=True)
class BootstrapCoverage:
    """Explicit expected-versus-persisted proof for a bootstrap interval."""

    start_block: int
    safe_block: int
    expected_rounds: int
    persisted_rounds: int
    missing_observation_ids: tuple[str, ...]
    extra_observation_ids: tuple[str, ...]
    terminal_rounds: Mapping[str, int]

    @property
    def is_complete(self) -> bool:
        return not self.missing_observation_ids


def freeze_chainlink_snapshot(
    rpc: ChainlinkForwardRpc,
    feeds: tuple[ChainlinkFeed, ...],
    confirmation_depth: int,
) -> FrozenChainlinkSnapshot:
    """Resolve one safe block, then pin every feed read to that exact block."""
    if not feeds:
        raise ValueError("At least one Chainlink feed is required")
    latest = rpc.get_latest_block()
    safe_number = calculate_safe_head(latest.number, confirmation_depth)
    safe_block = rpc.get_block_snapshot(safe_number)
    targets = tuple(
        FeedTarget(
            feed,
            rpc.get_round_at_block(feed.feed_address, safe_number),
        )
        for feed in feeds
    )
    return FrozenChainlinkSnapshot(
        observed_head=latest.number,
        confirmation_depth=confirmation_depth,
        block=safe_block,
        targets=targets,
    )


def _ranges_after_cursor(
    rpc: ChainlinkForwardRpc,
    target: FeedTarget,
    current_round_id: int,
) -> tuple[FeedRoundRange, ...]:
    """Plan phase-aware rounds after a persisted composite proxy cursor."""
    current_phase, current_aggregator = split_proxy_round_id(current_round_id)
    target_phase, target_aggregator = split_proxy_round_id(target.round_id)
    if target_phase < current_phase or (
        target_phase == current_phase and target_aggregator < current_aggregator
    ):
        raise ValueError(f"Chainlink round regressed for {target.feed.name}")
    if target.round_id == current_round_id:
        return ()

    ranges: list[FeedRoundRange] = []
    for phase_id in range(current_phase, target_phase + 1):
        first = current_aggregator + 1 if phase_id == current_phase else 1
        last = (
            target_aggregator
            if phase_id == target_phase
            else rpc.get_phase_latest_round(target.feed.feed_address, phase_id)
        )
        if first <= last:
            ranges.append(
                FeedRoundRange(
                    feed=target.feed,
                    phase_id=phase_id,
                    start_round_id=encode_proxy_round_id(phase_id, first),
                    end_round_id=encode_proxy_round_id(phase_id, last),
                )
            )
    return tuple(ranges)


def plan_chainlink_forward(
    rpc: ChainlinkForwardRpc,
    snapshot: FrozenChainlinkSnapshot,
    states: Mapping[WatermarkIdentity, WatermarkState],
    *,
    max_rounds: int,
) -> ChainlinkForwardPlan:
    """Plan every feed from its own cursor without skipping proxy phases."""
    if max_rounds <= 0:
        raise ValueError("max_rounds must be greater than zero")
    required = {
        CHAINLINK_FORWARD_WATERMARK,
        *(target.identity for target in snapshot.targets),
    }
    if set(states) != required:
        raise ValueError("Global and all per-feed watermarks must be initialized")
    committed_block = states[CHAINLINK_FORWARD_WATERMARK].committed_value
    if snapshot.block.number < committed_block:
        raise ValueError("Frozen safe block precedes committed global coverage")
    ranges = tuple(
        round_range
        for target in snapshot.targets
        for round_range in _ranges_after_cursor(
            rpc,
            target,
            states[target.identity].committed_value,
        )
    )
    plan = ChainlinkForwardPlan(committed_block, snapshot, ranges)
    if plan.round_count > max_rounds:
        raise ValueError(
            f"Forward plan contains {plan.round_count} rounds; configured limit "
            f"is {max_rounds}"
        )
    return plan


def expected_observation_ids(
    ranges: Iterable[FeedRoundRange],
) -> tuple[str, ...]:
    """Enumerate deterministic identities without assuming cross-phase continuity."""
    values: list[str] = []
    for item in ranges:
        _phase, first = split_proxy_round_id(item.start_round_id)
        _phase, last = split_proxy_round_id(item.end_round_id)
        values.extend(
            f"{item.feed.chain}:{item.feed.feed_address.lower()}:"
            f"{encode_proxy_round_id(item.phase_id, aggregator_round)}"
            for aggregator_round in range(first, last + 1)
        )
    return tuple(values)


def validate_bootstrap_coverage(
    *,
    start_block: int,
    safe_block: int,
    ranges: tuple[FeedRoundRange, ...],
    observations: Iterable[MarketDataObservation],
) -> BootstrapCoverage:
    """Prove configured feeds have every explicitly discovered round persisted."""
    expected = set(expected_observation_ids(ranges))
    persisted = {item.observation_id for item in observations}
    terminals = {
        item.feed.feed_address.lower(): item.end_round_id for item in ranges
    }
    return BootstrapCoverage(
        start_block=start_block,
        safe_block=safe_block,
        expected_rounds=len(expected),
        persisted_rounds=len(expected & persisted),
        missing_observation_ids=tuple(sorted(expected - persisted)),
        extra_observation_ids=tuple(sorted(persisted - expected)),
        terminal_rounds=terminals,
    )


def run_commit_last_pipeline(
    *,
    store: AtomicWatermarkStore,
    current_states: Mapping[WatermarkIdentity, WatermarkState],
    plan: ChainlinkForwardPlan,
    successful_run_id: str,
    persist_source: Callable[[ChainlinkForwardPlan], T],
    build_silver: Callable[[T], None],
    load_snowflake: Callable[[], None],
    run_dbt_and_quality: Callable[[], None],
) -> tuple[WatermarkState, ...]:
    """Commit all feed and global cursors only after the full boundary succeeds."""
    if plan.is_noop:
        return tuple(current_states.values())
    source_result = persist_source(plan)
    build_silver(source_result)
    load_snowflake()
    run_dbt_and_quality()
    return commit_watermarks_atomically(
        store,
        current_states,
        plan.candidates,
        successful_run_id,
        processing_succeeded=True,
    )
