from datetime import UTC, datetime

import pytest
from web3 import Web3

from backfill.aave_v3 import (
    AaveV3RpcClient,
    SearchLimits,
    backwards_block_ranges,
    event_id,
    merge_events,
    preserve_backward_checkpoint,
    search_historical_events,
)
from producer.protocols.aave_v3.handlers import (
    BorrowEventHandler,
    LiquidationCallEventHandler,
    RepayEventHandler,
    SUPPORTED_EVENT_TOPICS,
)

CODEC = Web3().codec
POOL = Web3.to_checksum_address("0x" + "10" * 20)
ASSET = Web3.to_checksum_address("0x" + "20" * 20)
USER = Web3.to_checksum_address("0x" + "30" * 20)
OTHER = Web3.to_checksum_address("0x" + "40" * 20)
TX = "0x" + "ab" * 32
STAMP = datetime(2026, 10, 1, tzinfo=UTC)


def _topic(abi_type: str, value: object) -> str:
    return "0x" + CODEC.encode([abi_type], [value]).hex()


def _log(handler, block: int, index: int = 0) -> dict:
    if isinstance(handler, BorrowEventHandler):
        topics = [_topic("address", ASSET), _topic("address", OTHER), _topic("uint16", 0)]
        types = ["address", "uint256", "uint8", "uint256"]
        values = [USER, 123, 2, 456]
    elif isinstance(handler, RepayEventHandler):
        topics = [_topic("address", ASSET), _topic("address", USER), _topic("address", OTHER)]
        types = ["uint256", "bool"]
        values = [123, False]
    else:
        topics = [_topic("address", ASSET), _topic("address", OTHER), _topic("address", USER)]
        types = ["uint256", "uint256", "address", "bool"]
        values = [123, 100, OTHER, True]
    return {
        "address": POOL,
        "topics": [handler.topic, *topics],
        "data": "0x" + CODEC.encode(types, values).hex(),
        "blockNumber": hex(block),
        "transactionHash": "0x" + f"{index + 1:064x}",
        "transactionIndex": "0x0",
        "blockHash": "0x" + "cd" * 32,
        "logIndex": hex(index),
        "removed": False,
    }


class FakeRpc:
    def __init__(self, logs_by_range: dict[tuple[int, int], list[dict]]) -> None:
        self.logs_by_range = logs_by_range
        self.request_count = 1
        self.ranges = []
        self.timestamp_blocks = []

    def get_event_logs(self, _pool: str, start: int, end: int) -> list[dict]:
        self.request_count += 1
        self.ranges.append((start, end))
        return self.logs_by_range.get((start, end), [])

    def get_block_timestamp(self, block: int) -> datetime:
        self.request_count += 1
        self.timestamp_blocks.append(block)
        return STAMP


def _limits(**overrides) -> SearchLimits:
    values = {
        "chunk_size": 10,
        "max_blocks": 20,
        "max_rpc_requests": 50,
        "target_borrow": 1,
        "target_repay": 1,
        "target_liquidation": 1,
    }
    values.update(overrides)
    return SearchLimits(**values)


def test_backwards_ranges_are_inclusive_gap_free_and_bounded() -> None:
    assert backwards_block_ranges(25, 23, 10) == ((16, 25), (6, 15), (3, 5))


def test_rpc_uses_pool_address_and_topic_zero_or_filter() -> None:
    captured = {}

    class Eth:
        def get_logs(self, request):
            captured.update(request)
            return []

    client = AaveV3RpcClient.__new__(AaveV3RpcClient)
    client.web3 = type("Web3Stub", (), {"eth": Eth()})()
    client.request_count = 0
    client.get_event_logs(POOL, 10, 19)

    assert captured["address"] == POOL
    assert captured["topics"] == [list(SUPPORTED_EVENT_TOPICS)]
    assert captured["fromBlock"] == 10
    assert captured["toBlock"] == 19


@pytest.mark.parametrize(
    ("handler", "event_type", "raw_field"),
    [
        (BorrowEventHandler(), "borrow", "amount_raw"),
        (RepayEventHandler(), "repay", "amount_raw"),
        (LiquidationCallEventHandler(), "liquidation", "debt_to_cover_raw"),
    ],
)
def test_search_decodes_each_aave_event_with_exact_amounts(handler, event_type, raw_field) -> None:
    rpc = FakeRpc({(91, 100): [_log(handler, 100)]})
    result = search_historical_events(
        rpc,
        pool_address=POOL,
        chain="arbitrum",
        end_block=100,
        limits=_limits(target_borrow=0, target_repay=0, target_liquidation=0),
    )

    assert result.events == ()
    result = search_historical_events(
        FakeRpc({(91, 100): [_log(handler, 100)]}),
        pool_address=POOL,
        chain="arbitrum",
        end_block=100,
        limits=_limits(max_blocks=10, target_borrow=1 if event_type == "borrow" else 0, target_repay=1 if event_type == "repay" else 0, target_liquidation=1 if event_type == "liquidation" else 0),
    )
    event = result.events[0]
    assert event.event_type == event_type
    assert event.payload[raw_field] == "123"
    assert event.block_timestamp == STAMP.isoformat()


def test_event_id_and_overlapping_reruns_are_deterministic() -> None:
    rpc = FakeRpc({(91, 100): [_log(BorrowEventHandler(), 100)]})
    first = search_historical_events(rpc, pool_address=POOL, chain="arbitrum", end_block=100, limits=_limits(max_blocks=10, target_repay=0, target_liquidation=0))
    second = search_historical_events(FakeRpc({(91, 100): [_log(BorrowEventHandler(), 100)]}), pool_address=POOL, chain="arbitrum", end_block=100, limits=_limits(max_blocks=10, target_repay=0, target_liquidation=0), existing=first.events)

    assert len(second.events) == 1
    assert merge_events(first.events, second.events) == first.events
    assert event_id(first.events[0]).startswith("arbitrum-0x")


def test_rerun_ignores_null_fields_materialized_by_spark_structs() -> None:
    first = search_historical_events(
        FakeRpc({(91, 100): [_log(BorrowEventHandler(), 100)]}),
        pool_address=POOL,
        chain="arbitrum",
        end_block=100,
        limits=_limits(max_blocks=10, target_repay=0, target_liquidation=0),
    ).events[0]
    reloaded = type(first)(
        **{
            **first.to_dict(),
            "payload": {**first.payload, "unused_schema_field": None},
        }
    )

    assert merge_events((reloaded,), (first,)) == (reloaded,)


def test_malformed_log_is_quarantined_without_aborting() -> None:
    malformed = {**_log(BorrowEventHandler(), 100), "data": "0x01"}
    result = search_historical_events(FakeRpc({(91, 100): [malformed]}), pool_address=POOL, chain="arbitrum", end_block=100, limits=_limits(max_blocks=10))

    assert result.events == ()
    assert len(result.issues) == 1
    assert result.issues[0].transaction_hash is not None


def test_request_budget_stops_without_advancing_incomplete_chunk() -> None:
    logs = [_log(BorrowEventHandler(), 100), _log(RepayEventHandler(), 99, 1)]
    result = search_historical_events(FakeRpc({(91, 100): logs}), pool_address=POOL, chain="arbitrum", end_block=100, limits=_limits(max_blocks=10, max_rpc_requests=3))

    assert result.rpc_requests == 3
    assert result.next_end_block == 100


def test_resume_starts_below_last_complete_chunk() -> None:
    first_rpc = FakeRpc({})
    first = search_historical_events(first_rpc, pool_address=POOL, chain="arbitrum", end_block=100, limits=_limits(max_blocks=10, target_borrow=2, target_repay=2, target_liquidation=2))
    second_rpc = FakeRpc({})
    search_historical_events(second_rpc, pool_address=POOL, chain="arbitrum", end_block=first.next_end_block, limits=_limits(max_blocks=10, target_borrow=2, target_repay=2, target_liquidation=2))

    assert first_rpc.ranges == [(91, 100)]
    assert second_rpc.ranges == [(81, 90)]


def test_explicit_overlap_cannot_regress_backward_checkpoint() -> None:
    assert preserve_backward_checkpoint(80, 95) == 80
    assert preserve_backward_checkpoint(80, 70) == 70
    assert preserve_backward_checkpoint(None, 95) == 95


def test_rpc_range_failure_preserves_prior_chunks_and_retries_failed_range() -> None:
    class FailingRpc(FakeRpc):
        def get_event_logs(self, pool: str, start: int, end: int) -> list[dict]:
            if (start, end) == (81, 90):
                self.request_count += 1
                raise RuntimeError("temporary RPC error")
            return super().get_event_logs(pool, start, end)

    result = search_historical_events(
        FailingRpc({(91, 100): [_log(BorrowEventHandler(), 100)]}),
        pool_address=POOL,
        chain="arbitrum",
        end_block=100,
        limits=_limits(
            max_blocks=20,
            target_borrow=2,
            target_repay=2,
            target_liquidation=2,
        ),
    )

    assert len(result.events) == 1
    assert result.next_end_block == 90
    assert "range 81-90" in result.issues[0].reason
