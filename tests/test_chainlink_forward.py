"""Tests for phase-aware, atomic Chainlink forward processing."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from backfill.chainlink import FeedRoundRange, encode_proxy_round_id
from config.settings import (
    get_chainlink_confirmation_depth,
    get_chainlink_forward_max_rounds_per_run,
    get_chainlink_project_collection_start_block,
)
from market_data.feeds import ChainlinkFeed
from market_data.models import MarketDataObservation
from market_data.rpc import BlockSnapshot, ChainlinkRound
from orchestration.chainlink_forward import (
    FeedTarget,
    FrozenChainlinkSnapshot,
    expected_observation_ids,
    freeze_chainlink_snapshot,
    plan_chainlink_forward,
    run_commit_last_pipeline,
    validate_bootstrap_coverage,
)
from orchestration import run_chainlink_forward as forward_runner
from orchestration.watermark import (
    CHAINLINK_FORWARD_WATERMARK,
    WatermarkCommitConflictError,
    WatermarkIdentity,
    WatermarkState,
    chainlink_feed_watermark,
    commit_watermarks_atomically,
    initialize_watermarks_atomically,
)

NOW = datetime(2026, 10, 16, tzinfo=UTC)
FEEDS = tuple(
    ChainlinkFeed(
        name=f"{asset}/USD",
        feed_address="0x" + f"{index:02x}" * 20,
        base_asset=asset,
        quote_asset="USD",
        chain="arbitrum",
    )
    for index, asset in enumerate(("ETH", "USDC", "USDT", "WBTC"), start=1)
)


def _round(phase: int, value: int) -> ChainlinkRound:
    return ChainlinkRound(
        encode_proxy_round_id(phase, value),
        123_000_000,
        "2026-10-16T12:00:00+00:00",
    )


class FakeRpc:
    def __init__(self) -> None:
        self.block_reads: list[tuple[str, int]] = []
        self.targets = {feed.feed_address.lower(): _round(2, 20) for feed in FEEDS}
        self.phase_latest = {2: 100}

    def get_latest_block(self) -> BlockSnapshot:
        return BlockSnapshot(10_000, "2026-10-16T12:01:00+00:00")

    def get_block_snapshot(self, block_number: int) -> BlockSnapshot:
        return BlockSnapshot(block_number, "2026-10-16T12:00:30+00:00")

    def get_round_at_block(self, address: str, block: int) -> ChainlinkRound:
        self.block_reads.append((address.lower(), block))
        return self.targets[address.lower()]

    def get_phase_latest_round(self, _address: str, phase: int) -> int:
        return self.phase_latest[phase]


class AtomicMemoryStore:
    def __init__(self, states: dict[WatermarkIdentity, WatermarkState] | None = None):
        self.states = dict(states or {})
        self.fail_identity: WatermarkIdentity | None = None
        self.atomic_calls = 0

    def get(self, identity):
        return self.states.get(identity)

    def initialize(self, *_args, **_kwargs):
        raise AssertionError("single initialization is not used")

    def advance(self, *_args, **_kwargs):
        raise AssertionError("single advance is not used")

    def initialize_many(self, values, run_id):
        original = dict(self.states)
        try:
            for identity, value in values.items():
                if identity in self.states:
                    raise WatermarkCommitConflictError("existing")
                self.states[identity] = WatermarkState(identity, value, run_id, NOW)
        except Exception:
            self.states = original
            raise
        return tuple(self.states[identity] for identity in values)

    def advance_many(self, expected, candidates, run_id):
        original = dict(self.states)
        try:
            for identity, value in expected.items():
                if identity == self.fail_identity:
                    raise WatermarkCommitConflictError("stale feed")
                if self.states[identity].committed_value != value:
                    raise WatermarkCommitConflictError("stale feed")
            for identity, value in candidates.items():
                self.states[identity] = WatermarkState(identity, value, run_id, NOW)
            self.atomic_calls += 1
        except Exception:
            self.states = original
            raise
        return tuple(self.states[identity] for identity in candidates)


def _snapshot(rounds: tuple[int, ...], block: int = 9_000) -> FrozenChainlinkSnapshot:
    return FrozenChainlinkSnapshot(
        observed_head=10_000,
        confirmation_depth=1_000,
        block=BlockSnapshot(block, "2026-10-16T12:00:30+00:00"),
        targets=tuple(
            FeedTarget(feed, _round(2, round_id))
            for feed, round_id in zip(FEEDS, rounds, strict=True)
        ),
    )


def _states(rounds=(10, 10, 10, 10), block=8_000):
    values = {
        CHAINLINK_FORWARD_WATERMARK: WatermarkState(
            CHAINLINK_FORWARD_WATERMARK, block, "bootstrap", NOW
        )
    }
    values.update(
        {
            chainlink_feed_watermark(feed.feed_address): WatermarkState(
                chainlink_feed_watermark(feed.feed_address),
                encode_proxy_round_id(2, round_id),
                "bootstrap",
                NOW,
            )
            for feed, round_id in zip(FEEDS, rounds, strict=True)
        }
    )
    return values


def test_configuration_is_independent_from_aave(monkeypatch) -> None:
    monkeypatch.delenv("CHAINLINK_CONFIRMATION_DEPTH", raising=False)
    monkeypatch.delenv("CHAINLINK_PROJECT_COLLECTION_START_BLOCK", raising=False)
    monkeypatch.delenv("CHAINLINK_FORWARD_MAX_ROUNDS_PER_RUN", raising=False)
    assert get_chainlink_confirmation_depth() == 1_200
    assert get_chainlink_project_collection_start_block() == 510_496_864
    assert get_chainlink_forward_max_rounds_per_run() == 10_000


def test_global_and_feed_identities_are_provider_neutral() -> None:
    assert CHAINLINK_FORWARD_WATERMARK.source_name == "chainlink_feeds"
    assert CHAINLINK_FORWARD_WATERMARK.watermark_type == "block_number"
    identities = {chainlink_feed_watermark(feed.feed_address) for feed in FEEDS}
    assert len(identities) == 4
    assert all(identity.watermark_type == "round_id" for identity in identities)
    assert all("alchemy" not in identity.source_name for identity in identities)


def test_all_targets_use_one_frozen_safe_block() -> None:
    rpc = FakeRpc()
    snapshot = freeze_chainlink_snapshot(rpc, FEEDS, confirmation_depth=1_200)
    assert snapshot.observed_head == 10_000
    assert snapshot.block.number == 8_800
    assert {block for _address, block in rpc.block_reads} == {8_800}
    assert len(rpc.block_reads) == 4


def test_unchanged_and_multiple_new_rounds_share_one_plan() -> None:
    rpc = FakeRpc()
    plan = plan_chainlink_forward(
        rpc,
        _snapshot((10, 12, 10, 15)),
        _states(),
        max_rounds=100,
    )
    assert plan.round_count == 7
    assert {item.feed.name for item in plan.ranges} == {"USDC/USD", "WBTC/USD"}
    candidates = plan.candidates
    assert candidates[chainlink_feed_watermark(FEEDS[0].feed_address)] == (
        encode_proxy_round_id(2, 10)
    )
    assert candidates[CHAINLINK_FORWARD_WATERMARK] == 9_000


def test_phase_transition_has_no_composite_numeric_range_assumption() -> None:
    rpc = FakeRpc()
    rpc.phase_latest = {2: 100}
    states = _states(rounds=(98, 10, 10, 10))
    snapshot = _snapshot((3, 10, 10, 10))
    first = snapshot.targets[0]
    snapshot = FrozenChainlinkSnapshot(
        snapshot.observed_head,
        snapshot.confirmation_depth,
        snapshot.block,
        (FeedTarget(first.feed, _round(3, 3)), *snapshot.targets[1:]),
    )
    plan = plan_chainlink_forward(rpc, snapshot, states, max_rounds=100)
    eth_ranges = [item for item in plan.ranges if item.feed.name == "ETH/USD"]
    assert [(item.phase_id, item.start_round_id, item.end_round_id) for item in eth_ranges] == [
        (2, encode_proxy_round_id(2, 99), encode_proxy_round_id(2, 100)),
        (3, encode_proxy_round_id(3, 1), encode_proxy_round_id(3, 3)),
    ]
    assert plan.round_count == 5


def test_bootstrap_coverage_reports_exact_missing_rounds() -> None:
    round_range = FeedRoundRange(
        FEEDS[0],
        2,
        encode_proxy_round_id(2, 10),
        encode_proxy_round_id(2, 11),
    )
    expected = expected_observation_ids((round_range,))
    observation = MarketDataObservation.create(
        chain="arbitrum",
        feed_address=FEEDS[0].feed_address,
        base_asset="ETH",
        quote_asset="USD",
        round_id=encode_proxy_round_id(2, 10),
        answer_raw=123_000_000,
        feed_decimals=8,
        feed_updated_at="2026-10-16T12:00:00+00:00",
        block_number=9_000,
        block_timestamp="2026-10-16T12:00:30+00:00",
    )
    coverage = validate_bootstrap_coverage(
        start_block=8_000,
        safe_block=9_000,
        ranges=(round_range,),
        observations=(observation,),
    )
    assert not coverage.is_complete
    assert coverage.missing_observation_ids == (expected[1],)


def test_atomic_initialization_creates_all_five_states() -> None:
    store = AtomicMemoryStore()
    values = {identity: state.committed_value for identity, state in _states().items()}
    states = initialize_watermarks_atomically(store, values, "bootstrap")
    assert len(states) == 5
    assert len(store.states) == 5


def test_one_stale_feed_rolls_back_whole_atomic_commit() -> None:
    initial = _states()
    store = AtomicMemoryStore(initial)
    stale = chainlink_feed_watermark(FEEDS[2].feed_address)
    store.fail_identity = stale
    candidates = _snapshot((11, 11, 11, 11)).targets
    values = {item.identity: item.round_id for item in candidates}
    values[CHAINLINK_FORWARD_WATERMARK] = 9_000
    before = dict(store.states)
    with pytest.raises(WatermarkCommitConflictError, match="stale feed"):
        commit_watermarks_atomically(
            store,
            initial,
            values,
            "forward",
            processing_succeeded=True,
        )
    assert store.states == before


@pytest.mark.parametrize("failed_step", ("source", "silver", "raw", "dq"))
def test_failure_before_commit_leaves_every_state_unchanged(failed_step: str) -> None:
    initial = _states()
    store = AtomicMemoryStore(initial)
    plan = plan_chainlink_forward(
        FakeRpc(),
        _snapshot((11, 11, 11, 11)),
        initial,
        max_rounds=100,
    )
    calls: list[str] = []

    def step(name):
        def run(_value=None):
            calls.append(name)
            if failed_step == name:
                raise RuntimeError(name)
            return "batch"

        return run

    with pytest.raises(RuntimeError, match=failed_step):
        run_commit_last_pipeline(
            store=store,
            current_states=initial,
            plan=plan,
            successful_run_id="failed",
            persist_source=step("source"),
            build_silver=step("silver"),
            load_snowflake=step("raw"),
            run_dbt_and_quality=step("dq"),
        )
    assert store.states == initial
    assert store.atomic_calls == 0


def test_successful_pipeline_commits_all_state_last() -> None:
    initial = _states()
    store = AtomicMemoryStore(initial)
    plan = plan_chainlink_forward(
        FakeRpc(),
        _snapshot((11, 10, 12, 10)),
        initial,
        max_rounds=100,
    )
    calls = []
    run_commit_last_pipeline(
        store=store,
        current_states=initial,
        plan=plan,
        successful_run_id="success",
        persist_source=lambda _plan: calls.append("source") or "batch",
        build_silver=lambda _batch: calls.append("silver"),
        load_snowflake=lambda: calls.append("raw"),
        run_dbt_and_quality=lambda: calls.append("dq"),
    )
    assert calls == ["source", "silver", "raw", "dq"]
    assert store.atomic_calls == 1
    assert store.states[CHAINLINK_FORWARD_WATERMARK].committed_value == 9_000


def test_global_noop_does_not_call_any_processing_boundary() -> None:
    initial = _states(block=9_000)
    plan = plan_chainlink_forward(
        FakeRpc(),
        _snapshot((10, 10, 10, 10), block=9_000),
        initial,
        max_rounds=100,
    )
    store = AtomicMemoryStore(initial)
    result = run_commit_last_pipeline(
        store=store,
        current_states=initial,
        plan=plan,
        successful_run_id="noop",
        persist_source=lambda _plan: pytest.fail("source called"),
        build_silver=lambda _batch: pytest.fail("silver called"),
        load_snowflake=lambda: pytest.fail("raw called"),
        run_dbt_and_quality=lambda: pytest.fail("dq called"),
    )
    assert len(result) == 5
    assert store.atomic_calls == 0


def test_block_progress_without_new_rounds_skips_downstream_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    states = _states(block=8_000)
    plan = plan_chainlink_forward(
        FakeRpc(),
        _snapshot((10, 10, 10, 10), block=9_000),
        states,
        max_rounds=100,
    )
    capture = SimpleNamespace(
        plan=plan,
        batches=(),
        current_states=states,
        observations_by_feed={},
        rpc_requests=0,
    )
    connection = SimpleNamespace(close=lambda: None)
    commits = []
    monkeypatch.setattr(forward_runner, "run_forward_capture", lambda _run_id: capture)
    monkeypatch.setattr(
        forward_runner,
        "_run",
        lambda _command: pytest.fail("downstream command called"),
    )
    monkeypatch.setattr(
        forward_runner,
        "connect_watermark_store",
        lambda: connection,
    )
    monkeypatch.setattr(
        forward_runner,
        "SnowflakeWatermarkStore",
        lambda _connection: object(),
    )
    monkeypatch.setattr(
        forward_runner,
        "commit_watermarks_atomically",
        lambda *args, **kwargs: commits.append((args, kwargs)) or tuple(states.values()),
    )

    forward_runner.run_pipeline("block-only")

    assert plan.round_count == 0
    assert len(commits) == 1
    assert commits[0][0][2][CHAINLINK_FORWARD_WATERMARK] == 9_000
