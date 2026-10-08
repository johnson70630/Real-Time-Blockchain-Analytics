from datetime import UTC, datetime

import pytest

from backfill.chainlink import (
    FeedRoundRange,
    discover_round_ranges,
    encode_proxy_round_id,
    extract_historical_rounds,
    merge_observations,
    split_proxy_round_id,
)
from market_data.feeds import ChainlinkFeed
from market_data.models import MarketDataObservation
from market_data.rpc import BlockSnapshot, ChainlinkRound

FEED = ChainlinkFeed(
    name="ETH/USD",
    feed_address="0x" + "12" * 20,
    base_asset="ETH",
    quote_asset="USD",
    chain="arbitrum",
)


class FakeRpc:
    def __init__(self, rounds: dict[int, ChainlinkRound]) -> None:
        self.rounds = rounds
        self.request_count = 0
        self.block_rounds: dict[int, ChainlinkRound] = {}
        self.phase_latest: dict[int, int] = {}

    def get_round_at_block(self, _address: str, block: int) -> ChainlinkRound:
        self.request_count += 1
        return self.block_rounds[block]

    def get_phase_latest_round(self, _address: str, phase: int) -> int:
        self.request_count += 1
        return self.phase_latest[phase]

    def get_round(self, _address: str, round_id: int) -> ChainlinkRound:
        self.request_count += 1
        value = self.rounds[round_id]
        if isinstance(value, Exception):
            raise value
        return value


class BatchedFakeRpc(FakeRpc):
    def __init__(self, rounds: dict[int, ChainlinkRound]) -> None:
        super().__init__(rounds)
        self.batch_sizes: list[int] = []

    def get_rounds(
        self,
        _address: str,
        round_ids: tuple[int, ...],
    ) -> tuple[ChainlinkRound, ...]:
        self.batch_sizes.append(len(round_ids))
        self.request_count += len(round_ids)
        return tuple(self.rounds[round_id] for round_id in round_ids)


def _round(round_id: int, answer: int = 123_456_789) -> ChainlinkRound:
    return ChainlinkRound(
        round_id=round_id,
        answer_raw=answer,
        updated_at="2026-09-30T08:24:00+00:00",
    )


def _range(start: int = 10, end: int = 12) -> FeedRoundRange:
    return FeedRoundRange(
        feed=FEED,
        phase_id=2,
        start_round_id=encode_proxy_round_id(2, start),
        end_round_id=encode_proxy_round_id(2, end),
    )


def test_proxy_round_ids_preserve_phase_and_aggregator_components() -> None:
    encoded = encode_proxy_round_id(3, 42)

    assert split_proxy_round_id(encoded) == (3, 42)
    with pytest.raises(ValueError, match="phase_id"):
        encode_proxy_round_id(0, 42)


def test_round_discovery_handles_proxy_phase_transition() -> None:
    start = encode_proxy_round_id(2, 98)
    end = encode_proxy_round_id(3, 4)
    rpc = FakeRpc({})
    rpc.block_rounds = {100: _round(start), 200: _round(end)}
    rpc.phase_latest = {2: 100}

    ranges = discover_round_ranges(rpc, FEED, start_block=100, end_block=200)

    assert [(item.phase_id, split_proxy_round_id(item.start_round_id)[1], split_proxy_round_id(item.end_round_id)[1]) for item in ranges] == [
        (2, 98, 100),
        (3, 1, 4),
    ]


def test_bounded_extraction_is_exact_and_resumable() -> None:
    round_range = _range()
    rounds = {
        round_id: _round(round_id, 100_000_000 + index)
        for index, round_id in enumerate(
            range(round_range.start_round_id, round_range.end_round_id + 1)
        )
    }
    rpc = FakeRpc(rounds)
    snapshot = BlockSnapshot(512_036_686, "2026-10-06T03:51:24+00:00")
    def clock() -> datetime:
        return datetime(2026, 10, 7, tzinfo=UTC)

    first = extract_historical_rounds(
        rpc,
        ranges=(round_range,),
        feed_decimals={FEED.feed_address.lower(): 8},
        snapshot=snapshot,
        max_rpc_requests=2,
        clock=clock,
        max_workers=1,
    )
    second = extract_historical_rounds(
        rpc,
        ranges=(round_range,),
        feed_decimals={FEED.feed_address.lower(): 8},
        snapshot=snapshot,
        max_rpc_requests=2,
        next_round_ids=first.next_round_ids,
        existing=first.observations,
        clock=clock,
        max_workers=1,
    )

    assert first.rounds_attempted == first.rpc_requests == 2
    assert first.complete is False
    assert second.complete is True
    assert len(second.observations) == 3
    assert len({item.observation_id for item in second.observations}) == 3
    assert all(item.answer_raw.isdigit() for item in second.observations)
    assert all(item.round_id.isdigit() for item in second.observations)
    assert all(item.block_number == snapshot.number for item in second.observations)
    assert all(item.base_asset == "ETH" for item in second.observations)
    assert all(item.quote_asset == "USD" for item in second.observations)
    assert all(
        item.feed_updated_at == "2026-09-30T08:24:00+00:00"
        for item in second.observations
    )


def test_rpc_batch_size_is_bounded_and_counts_logical_requests() -> None:
    round_range = _range(10, 14)
    rounds = {
        round_id: _round(round_id)
        for round_id in range(
            round_range.start_round_id,
            round_range.end_round_id + 1,
        )
    }
    rpc = BatchedFakeRpc(rounds)

    result = extract_historical_rounds(
        rpc,
        ranges=(round_range,),
        feed_decimals={FEED.feed_address.lower(): 8},
        snapshot=BlockSnapshot(200, "2026-09-30T08:30:00+00:00"),
        max_rpc_requests=5,
        rpc_batch_size=2,
    )

    assert rpc.batch_sizes == [2, 2, 1]
    assert result.rpc_requests == 5
    assert len(result.observations) == 5
    assert result.complete is True


def test_malformed_round_is_quarantined_without_stopping_progress() -> None:
    round_range = _range(10, 11)
    bad_round = round_range.start_round_id
    good_round = round_range.end_round_id
    rpc = FakeRpc(
        {
            bad_round: ValueError("malformed historical round"),
            good_round: _round(good_round),
        }
    )

    result = extract_historical_rounds(
        rpc,
        ranges=(round_range,),
        feed_decimals={FEED.feed_address.lower(): 8},
        snapshot=BlockSnapshot(200, "2026-09-30T08:30:00+00:00"),
        max_rpc_requests=2,
        max_workers=1,
    )

    assert len(result.observations) == 1
    assert len(result.issues) == 1
    assert result.issues[0].round_id == str(bad_round)
    assert "malformed" in result.issues[0].reason
    assert result.issues[0].retryable is False
    assert result.complete is True


def test_transient_failure_preserves_checkpoint_for_retry() -> None:
    round_range = _range(10, 11)
    throttled = round_range.start_round_id
    later = round_range.end_round_id
    rpc = FakeRpc(
        {
            throttled: RuntimeError("429 Too Many Requests"),
            later: _round(later),
        }
    )

    result = extract_historical_rounds(
        rpc,
        ranges=(round_range,),
        feed_decimals={FEED.feed_address.lower(): 8},
        snapshot=BlockSnapshot(200, "2026-09-30T08:30:00+00:00"),
        max_rpc_requests=2,
        max_workers=1,
    )

    assert result.issues[0].retryable is True
    assert result.next_round_ids[round_range.key] == throttled
    assert result.complete is False


def test_transient_overlap_does_not_regress_completed_checkpoint() -> None:
    round_range = _range(10, 10)
    round_id = round_range.start_round_id
    existing = MarketDataObservation.create(
        chain="arbitrum",
        feed_address=FEED.feed_address,
        base_asset="ETH",
        quote_asset="USD",
        round_id=round_id,
        answer_raw=123_456_789,
        feed_decimals=8,
        feed_updated_at="2026-09-30T08:24:00+00:00",
        block_number=200,
        block_timestamp="2026-09-30T08:30:00+00:00",
    )
    rpc = FakeRpc({round_id: RuntimeError("429 Too Many Requests")})

    result = extract_historical_rounds(
        rpc,
        ranges=(round_range,),
        feed_decimals={FEED.feed_address.lower(): 8},
        snapshot=BlockSnapshot(200, "2026-09-30T08:30:00+00:00"),
        max_rpc_requests=1,
        next_round_ids={round_range.key: round_id},
        existing=(existing,),
        max_workers=1,
    )

    assert result.next_round_ids[round_range.key] == round_id + 1
    assert result.complete is True


def test_overlap_deduplicates_and_conflicting_rounds_are_rejected() -> None:
    round_id = encode_proxy_round_id(2, 10)
    original = MarketDataObservation.create(
        chain="arbitrum",
        feed_address=FEED.feed_address,
        base_asset="ETH",
        quote_asset="USD",
        round_id=round_id,
        answer_raw=123_000_000,
        feed_decimals=8,
        feed_updated_at="2026-09-30T08:24:00+00:00",
        block_number=200,
        block_timestamp="2026-09-30T08:30:00+00:00",
    )
    repeated = MarketDataObservation.create(
        chain="arbitrum",
        feed_address=FEED.feed_address,
        base_asset="ETH",
        quote_asset="USD",
        round_id=round_id,
        answer_raw=123_000_000,
        feed_decimals=8,
        feed_updated_at="2026-09-30T08:24:00+00:00",
        block_number=201,
        block_timestamp="2026-09-30T08:31:00+00:00",
    )
    conflicting = MarketDataObservation.create(
        chain="arbitrum",
        feed_address=FEED.feed_address,
        base_asset="ETH",
        quote_asset="USD",
        round_id=round_id,
        answer_raw=999_000_000,
        feed_decimals=8,
        feed_updated_at="2026-09-30T08:24:00+00:00",
        block_number=201,
        block_timestamp="2026-09-30T08:31:00+00:00",
    )

    assert merge_observations((original,), (repeated,)) == (original,)
    with pytest.raises(ValueError, match="Conflicting Chainlink observation"):
        merge_observations((original,), (conflicting,))
