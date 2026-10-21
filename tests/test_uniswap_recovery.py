"""Tests for complete-contract Uniswap streaming gap recovery."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from backfill.uniswap_v3 import (
    CompleteRangeFetchError,
    UniswapSourceContract,
    canonical_pool_contract,
    fetch_complete_range_adaptive,
    merge_events,
    query_groups,
)
from config.settings import get_uniswap_recovery_chunk_size
from orchestration.aave_forward import CompletedBlockRange
from orchestration.uniswap_recovery import (
    UNISWAP_FORWARD_LEASE,
    UNISWAP_RECOVERY_CONTRACT_VERSION,
    UniswapSourceContractChangedError,
    UniswapRecoveryPlan,
    assert_source_contract_matches,
    bootstrap_eligible_value,
    plan_recovery_increment,
    run_commit_last_pipeline,
    source_contract_run_id,
)
from orchestration.watermark import (
    UNISWAP_FORWARD_WATERMARK,
    WatermarkCommitConflictError,
    WatermarkState,
)
from producer.models import EventEnvelope
from producer.protocols.uniswap_v3.handlers import SUPPORTED_EVENT_TOPICS
from reference_data.uniswap_v3_pools import UniswapV3Pool
from warehouse.leases import PipelineLeaseHandle, PipelineLeaseHeldError

NOW = datetime(2026, 10, 19, tzinfo=UTC)
POOL_A = "0x1111111111111111111111111111111111111111"
POOL_B = "0x2222222222222222222222222222222222222222"
TOKEN_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
FINGERPRINT = "a" * 64
LEASE_HANDLE = PipelineLeaseHandle(
    identity=UNISWAP_FORWARD_LEASE,
    owner_run_id="run-a",
    acquired_at=NOW,
    expires_at=NOW,
    lease_version=1,
)


def _pool(address: str, *, verified: bool = True) -> UniswapV3Pool:
    return UniswapV3Pool(
        chain="arbitrum",
        protocol="uniswap_v3",
        pool_address=address,
        token0_address=TOKEN_A,
        token1_address=TOKEN_B,
        fee_tier=3000,
        tick_spacing=60,
        factory_address=FACTORY,
        created_block=None,
        created_transaction_hash=None,
        created_log_index=None,
        created_block_timestamp=None,
        ingested_at=NOW,
        producer_version="test",
        schema_version="test",
        processed_at=NOW,
        metadata_source="direct_pool_call",
        factory_verified=verified,
    )


def _event(block: int, *, suffix: int = 0) -> EventEnvelope:
    transaction_hash = "0x" + f"{block + suffix:064x}"
    return EventEnvelope.create(
        protocol="uniswap_v3",
        chain="arbitrum",
        event_type="swap",
        block_number=block,
        transaction_hash=transaction_hash,
        log_index=0,
        block_timestamp="2026-10-01T00:00:00+00:00",
        ingested_at="2026-10-19T00:00:00+00:00",
        payload={"pool_address": POOL_A, "amount0": "1", "amount1": "-1"},
    )


class RecordingRpc:
    def __init__(self, *, fail_parent_topic: str | None = None) -> None:
        self.request_count = 0
        self.calls: list[
            tuple[tuple[str, ...], tuple[str, ...], int, int]
        ] = []
        self.fail_parent_topic = fail_parent_topic

    def get_event_logs(self, addresses, topics, start_block, end_block):
        self.request_count += 1
        self.calls.append((addresses, topics, start_block, end_block))
        if (
            self.fail_parent_topic in topics
            and start_block == 100
            and end_block == 119
        ):
            raise RuntimeError("provider limit")
        return []

    def get_block_timestamp(self, _block_number):
        return NOW


class MemoryWatermarkStore:
    def __init__(self, state: WatermarkState) -> None:
        self.state = state
        self.advance_calls = 0

    def get(self, identity):
        assert identity == UNISWAP_FORWARD_WATERMARK
        return self.state

    def initialize(self, *_args, **_kwargs):
        raise AssertionError("not used")

    def advance(
        self,
        identity,
        *,
        expected_value,
        candidate_value,
        successful_run_id,
    ):
        assert identity == UNISWAP_FORWARD_WATERMARK
        assert expected_value == self.state.committed_value
        self.advance_calls += 1
        self.state = WatermarkState(
            identity,
            candidate_value,
            successful_run_id,
            NOW,
        )
        return self.state


def test_provider_neutral_watermark_identity() -> None:
    assert UNISWAP_FORWARD_WATERMARK.pipeline_name == "uniswap_forward"
    assert UNISWAP_FORWARD_WATERMARK.source_name == "uniswap_v3_pools"
    assert UNISWAP_FORWARD_WATERMARK.chain_id == 42161


def test_calibrated_recovery_chunk_default(monkeypatch) -> None:
    monkeypatch.delenv("UNISWAP_RECOVERY_CHUNK_SIZE", raising=False)
    assert get_uniswap_recovery_chunk_size() == 5_000


def test_canonical_pool_universe_is_normalized_and_deduplicated() -> None:
    contract = canonical_pool_contract(
        (_pool(POOL_B), _pool(POOL_A), _pool(POOL_A)),
        chain="arbitrum",
    )
    assert contract.pool_addresses == (POOL_A, POOL_B)
    assert set(contract.event_topics) == set(SUPPORTED_EVENT_TOPICS)
    assert len(contract.fingerprint) == 64


def test_wrong_chain_pool_is_excluded() -> None:
    pool = _pool(POOL_A)
    other_chain = replace(pool, chain="ethereum")
    contract = canonical_pool_contract((_pool(POOL_B), other_chain), chain="arbitrum")
    assert contract.pool_addresses == (POOL_B,)


def test_query_groups_cover_every_pool_and_topic_once() -> None:
    contract = UniswapSourceContract((POOL_A, POOL_B))
    groups = query_groups(contract, address_batch_size=1)
    assert len(groups) == 2
    assert {group.addresses[0] for group in groups} == {POOL_A, POOL_B}
    assert all(group.topics == contract.event_topics for group in groups)


def test_partial_topic_failure_splits_the_entire_range() -> None:
    contract = UniswapSourceContract((POOL_A,))
    failed_topic = contract.event_topics[0]
    rpc = RecordingRpc(fail_parent_topic=failed_topic)
    result = fetch_complete_range_adaptive(
        rpc,
        contract=contract,
        start_block=100,
        end_block=119,
        address_batch_size=10,
        minimum_range_size=10,
        max_range_attempts=8,
    )
    assert tuple(
        (item.start_block, item.end_block) for item in result.completed_ranges
    ) == ((100, 109), (110, 119))
    assert result.split_count == 1
    assert all(
        any(topic in call[1] for call in rpc.calls if call[2:] == (start, end))
        for start, end in ((100, 109), (110, 119))
        for topic in contract.event_topics
    )


def test_failure_at_minimum_range_never_reports_completion() -> None:
    contract = UniswapSourceContract((POOL_A,))
    rpc = RecordingRpc(fail_parent_topic=contract.event_topics[0])
    with pytest.raises(CompleteRangeFetchError):
        fetch_complete_range_adaptive(
            rpc,
            contract=contract,
            start_block=100,
            end_block=119,
            address_batch_size=10,
            minimum_range_size=20,
            max_range_attempts=2,
        )


def test_stream_and_recovery_overlap_deduplicates() -> None:
    live = (_event(100), _event(101), _event(105))
    recovery = (_event(102), _event(103), _event(104), _event(105))
    merged = merge_events(live, recovery)
    assert [event.block_number for event in merged] == [100, 101, 102, 103, 104, 105]


def test_fully_overlapping_recovery_is_idempotent() -> None:
    live = (_event(100), _event(101))
    assert merge_events(live, live) == live


def test_bootstrap_requires_gap_free_completed_ranges() -> None:
    assert bootstrap_eligible_value(
        project_start_block=100,
        frozen_safe_block=129,
        completed_ranges=(
            CompletedBlockRange(100, 109),
            CompletedBlockRange(120, 129),
        ),
    ) is None
    assert bootstrap_eligible_value(
        project_start_block=100,
        frozen_safe_block=129,
        completed_ranges=(
            CompletedBlockRange(100, 109),
            CompletedBlockRange(110, 129),
        ),
    ) == 129


def test_forward_plan_has_clean_noop() -> None:
    assert plan_recovery_increment(
        committed_block=200,
        safe_head=200,
        max_blocks=100,
    ).is_noop


def test_source_contract_tag_round_trips() -> None:
    state = WatermarkState(
        UNISWAP_FORWARD_WATERMARK,
        100,
        source_contract_run_id("bootstrap", FINGERPRINT),
        NOW,
    )
    assert_source_contract_matches(state, FINGERPRINT)


@pytest.mark.parametrize(
    ("fingerprint", "version"),
    (("b" * 64, UNISWAP_RECOVERY_CONTRACT_VERSION), (FINGERPRINT, "v2")),
)
def test_changed_pool_topic_or_query_contract_is_rejected(
    fingerprint: str,
    version: str,
) -> None:
    state = WatermarkState(
        UNISWAP_FORWARD_WATERMARK,
        100,
        source_contract_run_id("bootstrap", FINGERPRINT),
        NOW,
    )
    with pytest.raises(UniswapSourceContractChangedError):
        assert_source_contract_matches(
            state,
            fingerprint,
            contract_version=version,
        )


def test_changed_contract_stops_pipeline_before_source_persistence() -> None:
    state = WatermarkState(
        UNISWAP_FORWARD_WATERMARK,
        100,
        source_contract_run_id("bootstrap", FINGERPRINT),
        NOW,
    )
    store = MemoryWatermarkStore(state)
    source_called = False

    def persist_source(_plan):
        nonlocal source_called
        source_called = True

    with pytest.raises(UniswapSourceContractChangedError):
        run_commit_last_pipeline(
            store=store,
            current=state,
            plan=UniswapRecoveryPlan(100, 110, 101, 110),
            successful_run_id="changed",
            contract_fingerprint="b" * 64,
            persist_source=persist_source,
            build_silver=lambda _batch: None,
            load_snowflake=lambda: None,
            run_dbt_and_quality=lambda: None,
        )
    assert source_called is False
    assert store.advance_calls == 0


@pytest.mark.parametrize("failed_step", ("source", "silver", "snowflake", "dbt"))
def test_downstream_failure_leaves_watermark_unchanged(failed_step: str) -> None:
    state = WatermarkState(
        UNISWAP_FORWARD_WATERMARK,
        100,
        source_contract_run_id("old", FINGERPRINT),
        NOW,
    )
    store = MemoryWatermarkStore(state)
    plan = UniswapRecoveryPlan(100, 110, 101, 110)

    def step(name, value=None):
        def call(*_args):
            if failed_step == name:
                raise RuntimeError(f"{name} failed")
            return value

        return call

    with pytest.raises(RuntimeError, match="failed"):
        run_commit_last_pipeline(
            store=store,
            current=state,
            plan=plan,
            successful_run_id="test",
            contract_fingerprint=FINGERPRINT,
            persist_source=step("source", "batch"),
            build_silver=step("silver"),
            load_snowflake=step("snowflake"),
            run_dbt_and_quality=step("dbt"),
        )
    assert store.state.committed_value == 100
    assert store.advance_calls == 0


def test_stale_writer_fails_after_processing_boundary() -> None:
    planned = WatermarkState(
        UNISWAP_FORWARD_WATERMARK,
        100,
        source_contract_run_id("old", FINGERPRINT),
        NOW,
    )
    store = MemoryWatermarkStore(
        WatermarkState(UNISWAP_FORWARD_WATERMARK, 105, "other", NOW)
    )
    with pytest.raises(WatermarkCommitConflictError):
        run_commit_last_pipeline(
            store=store,
            current=planned,
            plan=UniswapRecoveryPlan(100, 110, 101, 110),
            successful_run_id="stale",
            contract_fingerprint=FINGERPRINT,
            persist_source=lambda _plan: "batch",
            build_silver=lambda _batch: None,
            load_snowflake=lambda: None,
            run_dbt_and_quality=lambda: None,
        )
    assert store.state.committed_value == 105
    assert store.advance_calls == 0


def test_runner_acquires_before_work_and_releases_after_success(monkeypatch) -> None:
    from orchestration import run_uniswap_recovery as runner

    calls: list[str] = []
    monkeypatch.setattr(
        runner,
        "_acquire_lease",
        lambda run_id: calls.append(f"acquire:{run_id}") or LEASE_HANDLE,
    )
    monkeypatch.setattr(
        runner,
        "_run_pipeline_under_lease",
        lambda run_id, safe_head=None: calls.append(
            f"work:{run_id}:{safe_head}"
        ),
    )
    monkeypatch.setattr(
        runner,
        "_release_lease",
        lambda handle: calls.append(f"release:{handle.owner_run_id}"),
    )

    runner.run_pipeline("run-a", safe_head=123)

    assert calls == ["acquire:run-a", "work:run-a:123", "release:run-a"]


def test_runner_releases_after_failure(monkeypatch) -> None:
    from orchestration import run_uniswap_recovery as runner

    released: list[PipelineLeaseHandle] = []
    monkeypatch.setattr(runner, "_acquire_lease", lambda _run_id: LEASE_HANDLE)
    monkeypatch.setattr(
        runner,
        "_run_pipeline_under_lease",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("failed")),
    )
    monkeypatch.setattr(runner, "_release_lease", released.append)

    with pytest.raises(RuntimeError, match="failed"):
        runner.run_pipeline("run-a")

    assert released == [LEASE_HANDLE]


def test_lease_rejection_prevents_all_recovery_work(monkeypatch) -> None:
    from orchestration import run_uniswap_recovery as runner

    work_called = False

    def reject(_run_id):
        raise PipelineLeaseHeldError("held")

    def work(*_args, **_kwargs):
        nonlocal work_called
        work_called = True

    monkeypatch.setattr(runner, "_acquire_lease", reject)
    monkeypatch.setattr(runner, "_run_pipeline_under_lease", work)

    with pytest.raises(PipelineLeaseHeldError, match="held"):
        runner.run_pipeline("run-b")

    assert work_called is False
