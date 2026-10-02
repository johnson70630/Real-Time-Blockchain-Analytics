from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from web3 import Web3

from producer.protocols.evm import event_topic
from producer.protocols.uniswap_v3.abi import POOL_CREATED_EVENT_SIGNATURE
from reference_data.uniswap_v3_pools import (
    DirectPoolMetadata,
    POOL_CREATED_TOPIC,
    UniswapV3FactoryRpcClient,
    UniswapV3Pool,
    block_ranges,
    bootstrap_observed_pools,
    decode_pool_created_log,
    extract_pool_metadata,
    merge_pool_records,
    next_start_block,
    reconcile_pool_coverage,
)

CODEC = Web3().codec
FACTORY = Web3.to_checksum_address("0x" + "10" * 20)
TOKEN0 = Web3.to_checksum_address("0x" + "20" * 20)
TOKEN1 = Web3.to_checksum_address("0x" + "30" * 20)
POOL = Web3.to_checksum_address("0x" + "40" * 20)
SECOND_POOL = Web3.to_checksum_address("0x" + "50" * 20)
CREATED_AT = datetime(2026, 7, 20, tzinfo=UTC)
OBSERVED_AT = CREATED_AT + timedelta(minutes=1)


def _encoded_topic(abi_type: str, value: object) -> str:
    return "0x" + CODEC.encode([abi_type], [value]).hex()


def _pool_created_log(
    *,
    pool: str = POOL,
    block_number: int = 200,
    log_index: int = 2,
    transaction_hash: str = "0x" + "ab" * 32,
) -> dict:
    return {
        "address": FACTORY,
        "topics": [
            POOL_CREATED_TOPIC,
            _encoded_topic("address", TOKEN0),
            _encoded_topic("address", TOKEN1),
            _encoded_topic("uint24", 500),
        ],
        "data": "0x" + CODEC.encode(
            ["int24", "address"],
            [10, pool],
        ).hex(),
        "blockNumber": hex(block_number),
        "transactionHash": transaction_hash,
        "transactionIndex": "0x0",
        "blockHash": "0x" + "cd" * 32,
        "logIndex": hex(log_index),
        "removed": False,
    }


def _record(**changes) -> UniswapV3Pool:
    values = {
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "pool_address": POOL,
        "token0_address": TOKEN0,
        "token1_address": TOKEN1,
        "fee_tier": 500,
        "tick_spacing": 10,
        "factory_address": FACTORY,
        "created_block": 200,
        "created_transaction_hash": "0x" + "ab" * 32,
        "created_log_index": 2,
        "created_block_timestamp": CREATED_AT,
        "ingested_at": OBSERVED_AT,
        "producer_version": "1.0.0",
        "schema_version": "1.0.0",
        "processed_at": OBSERVED_AT,
        "metadata_source": "factory_pool_created",
        "factory_verified": True,
    }
    values.update(changes)
    return UniswapV3Pool(**values)


def test_pool_created_topic_is_derived_from_canonical_signature() -> None:
    assert POOL_CREATED_EVENT_SIGNATURE == (
        "PoolCreated(address,address,uint24,int24,address)"
    )
    assert POOL_CREATED_TOPIC == (
        "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
    )
    assert POOL_CREATED_TOPIC == event_topic(POOL_CREATED_EVENT_SIGNATURE)


def test_pool_created_decoding_and_address_normalization() -> None:
    record = decode_pool_created_log(
        _pool_created_log(),
        chain="arbitrum",
        factory_address=FACTORY,
        block_timestamp=CREATED_AT,
        observed_at=OBSERVED_AT,
    )

    assert record.pool_address == POOL.lower()
    assert record.token0_address == TOKEN0.lower()
    assert record.token1_address == TOKEN1.lower()
    assert record.factory_address == FACTORY.lower()
    assert record.fee_tier == 500
    assert record.tick_spacing == 10
    assert record.created_block == 200
    assert record.created_log_index == 2
    assert record.created_transaction_hash == "0x" + "ab" * 32


def test_rpc_filters_by_factory_topic_and_inclusive_range() -> None:
    captured = []
    client = object.__new__(UniswapV3FactoryRpcClient)
    client.web3 = SimpleNamespace(
        eth=SimpleNamespace(
            get_logs=lambda request: captured.append(request) or [],
        )
    )

    assert client.get_pool_created_logs(FACTORY, 165, 174) == []
    assert captured == [
        {
            "address": FACTORY,
            "topics": [POOL_CREATED_TOPIC],
            "fromBlock": 165,
            "toBlock": 174,
        }
    ]


class FakeContractCall:
    def __init__(self, value, calls: list[int]) -> None:
        self.value = value
        self.calls = calls

    def call(self, *, block_identifier: int):
        self.calls.append(block_identifier)
        return self.value


class FakeContractFunctions:
    def __init__(self, calls: list[int]) -> None:
        self.calls = calls

    def token0(self):
        return FakeContractCall(TOKEN0, self.calls)

    def token1(self):
        return FakeContractCall(TOKEN1, self.calls)

    def fee(self):
        return FakeContractCall(500, self.calls)

    def tickSpacing(self):
        return FakeContractCall(10, self.calls)

    def factory(self):
        return FakeContractCall(FACTORY, self.calls)


def test_direct_pool_eth_calls_decode_at_one_pinned_block() -> None:
    calls = []
    client = object.__new__(UniswapV3FactoryRpcClient)
    client.web3 = SimpleNamespace(
        eth=SimpleNamespace(
            contract=lambda **_kwargs: SimpleNamespace(
                functions=FakeContractFunctions(calls)
            )
        )
    )

    metadata = client.get_pool_metadata(POOL, 12345)

    assert metadata == DirectPoolMetadata(
        token0_address=TOKEN0,
        token1_address=TOKEN1,
        fee_tier=500,
        tick_spacing=10,
        factory_address=FACTORY,
    )
    assert calls == [12345] * 5


def test_block_ranges_are_inclusive_without_gaps_or_overlap() -> None:
    assert list(block_ranges(165, 174, 4)) == [
        (165, 168),
        (169, 172),
        (173, 174),
    ]
    assert list(block_ranges(200, 199, 4)) == []


def test_watermark_and_explicit_overlap_behavior() -> None:
    assert next_start_block(165, None) == 165
    assert next_start_block(165, 999) == 1000
    assert next_start_block(165, 999, 900) == 900

    with pytest.raises(ValueError, match="deployment block"):
        next_start_block(165, 999, 164)


def test_exact_overlap_is_idempotent_and_preserves_existing_metadata() -> None:
    existing = _record()
    rerun = _record(
        ingested_at=OBSERVED_AT + timedelta(hours=1),
        processed_at=OBSERVED_AT + timedelta(hours=1),
    )

    assert merge_pool_records((existing,), (rerun,)) == (existing,)


def test_direct_pool_rerun_is_idempotent() -> None:
    existing = _record(
        created_block=None,
        created_transaction_hash=None,
        created_log_index=None,
        created_block_timestamp=None,
        metadata_source="direct_pool_call",
    )
    rerun = _record(
        created_block=None,
        created_transaction_hash=None,
        created_log_index=None,
        created_block_timestamp=None,
        ingested_at=OBSERVED_AT + timedelta(hours=1),
        processed_at=OBSERVED_AT + timedelta(hours=1),
        metadata_source="direct_pool_call",
    )

    assert merge_pool_records((existing,), (rerun,)) == (existing,)


def test_factory_record_reconciles_matching_direct_pool() -> None:
    direct = _record(
        created_block=None,
        created_transaction_hash=None,
        created_log_index=None,
        created_block_timestamp=None,
        metadata_source="direct_pool_call",
    )
    historical = _record()

    assert merge_pool_records((direct,), (historical,)) == (historical,)


def test_factory_reconciliation_rejects_definition_conflict() -> None:
    direct = _record(
        created_block=None,
        created_transaction_hash=None,
        created_log_index=None,
        created_block_timestamp=None,
        metadata_source="direct_pool_call",
    )

    with pytest.raises(ValueError, match="Conflicting metadata"):
        merge_pool_records((direct,), (_record(fee_tier=3000),))


def test_duplicate_event_identity_for_different_pool_is_rejected() -> None:
    with pytest.raises(ValueError, match="multiple pool addresses"):
        merge_pool_records(
            (_record(),),
            (_record(pool_address=SECOND_POOL),),
        )


def test_malformed_pool_metadata_is_rejected() -> None:
    with pytest.raises(ValueError, match="must differ"):
        _record(token1_address=TOKEN0)
    with pytest.raises(ValueError, match="valid EVM address"):
        _record(pool_address="missing")
    with pytest.raises(ValueError, match="must not be negative"):
        _record(created_block=-1)


class FakeRpc:
    def __init__(self) -> None:
        self.ranges = []
        self.timestamp_calls = []

    def get_latest_block_number(self) -> int:
        return 210

    def get_pool_created_logs(
        self,
        _factory_address: str,
        start_block: int,
        end_block: int,
    ) -> list[dict]:
        self.ranges.append((start_block, end_block))
        if start_block <= 200 <= end_block:
            log = _pool_created_log()
            return [log, dict(log)]
        return []

    def get_block_timestamp(self, block_number: int) -> datetime:
        self.timestamp_calls.append(block_number)
        return CREATED_AT


def test_chunked_extraction_deduplicates_and_caches_block_timestamp() -> None:
    rpc = FakeRpc()
    result = extract_pool_metadata(
        rpc,
        chain="arbitrum",
        factory_address=FACTORY,
        start_block=165,
        end_block=210,
        chunk_size=20,
        clock=lambda: OBSERVED_AT,
    )

    assert rpc.ranges == [(165, 184), (185, 204), (205, 210)]
    assert rpc.timestamp_calls == [200]
    assert result.events_retrieved == 2
    assert result.duplicate_events == 1
    assert result.chunks_processed == 3
    assert result.records == (_record(),)


class FakeBootstrapRpc:
    def __init__(self, metadata: DirectPoolMetadata) -> None:
        self.metadata = metadata
        self.addresses = []

    def get_latest_block_number(self) -> int:
        return 999

    def get_pool_metadata(
        self,
        pool_address: str,
        block_number: int,
    ) -> DirectPoolMetadata:
        assert block_number == 999
        self.addresses.append(pool_address)
        return self.metadata


def test_bootstrap_deduplicates_observed_pools_and_verifies_factory() -> None:
    rpc = FakeBootstrapRpc(
        DirectPoolMetadata(TOKEN0, TOKEN1, 500, 10, FACTORY)
    )

    result = bootstrap_observed_pools(
        rpc,
        [POOL, POOL.lower()],
        chain="arbitrum",
        expected_factory=FACTORY,
        clock=lambda: OBSERVED_AT,
    )

    assert rpc.addresses == [POOL.lower()]
    assert len(result.records) == 1
    assert result.records[0].metadata_source == "direct_pool_call"
    assert result.records[0].factory_verified is True
    assert result.records[0].created_block is None
    assert result.issues == ()


def test_bootstrap_quarantines_factory_mismatch_with_reason() -> None:
    other_factory = Web3.to_checksum_address("0x" + "60" * 20)
    rpc = FakeBootstrapRpc(
        DirectPoolMetadata(TOKEN0, TOKEN1, 500, 10, other_factory)
    )

    result = bootstrap_observed_pools(
        rpc,
        [POOL],
        chain="arbitrum",
        expected_factory=FACTORY,
        clock=lambda: OBSERVED_AT,
    )

    assert result.records == ()
    assert result.issues[0].pool_address == POOL.lower()
    assert result.issues[0].reason == (
        f"factory mismatch: expected {FACTORY.lower()}, "
        f"received {other_factory.lower()}"
    )


def test_bootstrap_quarantines_malformed_contract_response() -> None:
    rpc = FakeBootstrapRpc(
        DirectPoolMetadata("malformed", TOKEN1, 500, 10, FACTORY)
    )

    result = bootstrap_observed_pools(
        rpc,
        [POOL],
        chain="arbitrum",
        expected_factory=FACTORY,
        clock=lambda: OBSERVED_AT,
    )

    assert result.records == ()
    assert result.issues[0].pool_address == POOL.lower()
    assert result.issues[0].reason == "token0_address must be a valid EVM address"


def test_swap_pool_coverage_reports_exact_missing_addresses() -> None:
    coverage = reconcile_pool_coverage(
        [POOL, POOL.lower(), SECOND_POOL],
        [_record()],
    )

    assert coverage.observed_pools == 2
    assert coverage.mapped_pools == 1
    assert coverage.missing_pools == (SECOND_POOL.lower(),)
    assert coverage.coverage_percentage == 50.0
