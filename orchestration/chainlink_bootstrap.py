"""Immutable Chainlink bootstrap contract and resumable batch planning."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from backfill.chainlink import FeedRoundRange, encode_proxy_round_id, split_proxy_round_id
from market_data.feeds import ChainlinkFeed
from market_data.models import MarketDataObservation
from orchestration.chainlink_forward import expected_observation_ids

CHAINLINK_BOOTSTRAP_START_BLOCK = 510_496_864
CHAINLINK_BOOTSTRAP_SAFE_BLOCK = 515_520_400

# Accepted immutable targets from the pinned-block bootstrap audit. Feed
# addresses, rather than RPC-provider names, are the stable identities.
CHAINLINK_BOOTSTRAP_ROUNDS: Mapping[str, tuple[int, int]] = {
    "0x639fe6ab55c921f74e7fac1ee960c0b6293ba612": (
        36_893_488_147_419_231_838,
        36_893_488_147_419_240_964,
    ),
    "0x50834f3163758fcc1df9973b6e91f0f0f0434ad3": (
        55_340_232_221_128_668_016,
        55_340_232_221_128_673_130,
    ),
    "0x3f3f5df88dc9f13eac63df89ec16ef6e7e25dde7": (
        55_340_232_221_128_668_017,
        55_340_232_221_128_673_128,
    ),
    "0xd0c7101eacbb49f3decccc166d238410d6d46d57": (
        36_893_488_147_419_158_988,
        36_893_488_147_419_163_145,
    ),
}


@dataclass(frozen=True, slots=True)
class RoundBatch:
    """One feed/phase-local, independently retryable round interval."""

    round_range: FeedRoundRange

    @property
    def expected_ids(self) -> tuple[str, ...]:
        return expected_observation_ids((self.round_range,))

    @property
    def size(self) -> int:
        return len(self.expected_ids)


@dataclass(frozen=True, slots=True)
class FeedCoverage:
    """Required and durable coverage for one accepted feed contract."""

    feed: ChainlinkFeed
    ranges: tuple[FeedRoundRange, ...]
    required_ids: frozenset[str]
    persisted_ids: frozenset[str]
    duplicate_count: int

    @property
    def missing_ids(self) -> frozenset[str]:
        return self.required_ids - self.persisted_ids

    @property
    def phases(self) -> tuple[int, ...]:
        return tuple(item.phase_id for item in self.ranges)


def frozen_feed_ranges(
    feeds: tuple[ChainlinkFeed, ...],
) -> tuple[FeedRoundRange, ...]:
    """Build accepted phase-local ranges without consulting a moving chain head."""
    configured = {feed.feed_address.lower(): feed for feed in feeds}
    if set(configured) != set(CHAINLINK_BOOTSTRAP_ROUNDS):
        raise ValueError("Configured Chainlink feeds differ from bootstrap contract")
    ranges: list[FeedRoundRange] = []
    for address, (start, target) in CHAINLINK_BOOTSTRAP_ROUNDS.items():
        start_phase, start_aggregator = split_proxy_round_id(start)
        target_phase, target_aggregator = split_proxy_round_id(target)
        if start_phase != target_phase:
            raise ValueError(
                "Accepted bootstrap range requires explicit intermediate phase bounds"
            )
        ranges.append(
            FeedRoundRange(
                configured[address],
                start_phase,
                encode_proxy_round_id(start_phase, start_aggregator),
                encode_proxy_round_id(target_phase, target_aggregator),
            )
        )
    return tuple(ranges)


def calculate_feed_coverage(
    ranges: tuple[FeedRoundRange, ...],
    observations: Iterable[MarketDataObservation],
) -> tuple[FeedCoverage, ...]:
    """Recompute coverage from canonical persisted identities, never memory state."""
    observations_tuple = tuple(observations)
    counts: dict[str, int] = {}
    for item in observations_tuple:
        counts[item.observation_id] = counts.get(item.observation_id, 0) + 1
    result = []
    feeds = {item.feed.feed_address.lower(): item.feed for item in ranges}
    for address, feed in feeds.items():
        feed_ranges = tuple(
            item for item in ranges if item.feed.feed_address.lower() == address
        )
        required = frozenset(expected_observation_ids(feed_ranges))
        persisted = frozenset(
            item.observation_id
            for item in observations_tuple
            if item.feed_address.lower() == address
        )
        result.append(
            FeedCoverage(
                feed,
                feed_ranges,
                required,
                persisted & required,
                sum(max(0, counts.get(item, 0) - 1) for item in required),
            )
        )
    return tuple(result)


def plan_missing_batches(
    coverage: Iterable[FeedCoverage],
    *,
    batch_size: int,
) -> tuple[RoundBatch, ...]:
    """Split only contiguous missing aggregator rounds by feed and proxy phase."""
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    batches: list[RoundBatch] = []
    for item in coverage:
        missing = item.missing_ids
        for source_range in item.ranges:
            _phase, first = split_proxy_round_id(source_range.start_round_id)
            _phase, last = split_proxy_round_id(source_range.end_round_id)
            missing_aggregators = [
                value
                for value in range(first, last + 1)
                if (
                    f"{item.feed.chain}:{item.feed.feed_address.lower()}:"
                    f"{encode_proxy_round_id(source_range.phase_id, value)}"
                )
                in missing
            ]
            offset = 0
            while offset < len(missing_aggregators):
                run_start = missing_aggregators[offset]
                run_end = run_start
                offset += 1
                while (
                    offset < len(missing_aggregators)
                    and missing_aggregators[offset] == run_end + 1
                    and missing_aggregators[offset] - run_start < batch_size
                ):
                    run_end = missing_aggregators[offset]
                    offset += 1
                batches.append(
                    RoundBatch(
                        FeedRoundRange(
                            item.feed,
                            source_range.phase_id,
                            encode_proxy_round_id(source_range.phase_id, run_start),
                            encode_proxy_round_id(source_range.phase_id, run_end),
                        )
                    )
                )
    return tuple(batches)
