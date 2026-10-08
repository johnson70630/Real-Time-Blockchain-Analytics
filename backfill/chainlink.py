"""Bounded, resumable historical extraction from Chainlink feed proxies."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock
from time import monotonic, sleep
from typing import Callable, Iterable, Mapping, Protocol

from web3 import Web3

from market_data.feeds import ChainlinkFeed
from market_data.models import MarketDataObservation
from market_data.rpc import AGGREGATOR_V3_ABI, BlockSnapshot, ChainlinkRound

ROUND_PHASE_OFFSET = 64
AGGREGATOR_ROUND_MASK = (1 << ROUND_PHASE_OFFSET) - 1

_HISTORICAL_PROXY_ABI = [
    *AGGREGATOR_V3_ABI,
    {
        "inputs": [
            {"internalType": "uint80", "name": "_roundId", "type": "uint80"}
        ],
        "name": "getRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {
                "internalType": "uint80",
                "name": "answeredInRound",
                "type": "uint80",
            },
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint16", "name": "", "type": "uint16"}],
        "name": "phaseAggregators",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

_AGGREGATOR_ABI = [
    {
        "inputs": [],
        "name": "latestRound",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]


def encode_proxy_round_id(phase_id: int, aggregator_round_id: int) -> int:
    """Encode Chainlink proxy phase and aggregator round identifiers."""
    if not 0 < phase_id < 1 << 16:
        raise ValueError("phase_id must fit an unsigned 16-bit integer")
    if not 0 < aggregator_round_id <= AGGREGATOR_ROUND_MASK:
        raise ValueError("aggregator_round_id must fit an unsigned 64-bit integer")
    return (phase_id << ROUND_PHASE_OFFSET) | aggregator_round_id


def split_proxy_round_id(round_id: int) -> tuple[int, int]:
    """Return the phase and aggregator components of a proxy round ID."""
    if round_id <= 0:
        raise ValueError("round_id must be positive")
    return round_id >> ROUND_PHASE_OFFSET, round_id & AGGREGATOR_ROUND_MASK


@dataclass(frozen=True, slots=True)
class FeedRoundRange:
    """One contiguous aggregator-phase range exposed through a feed proxy."""

    feed: ChainlinkFeed
    phase_id: int
    start_round_id: int
    end_round_id: int

    def __post_init__(self) -> None:
        start_phase, start_aggregator = split_proxy_round_id(self.start_round_id)
        end_phase, end_aggregator = split_proxy_round_id(self.end_round_id)
        if start_phase != self.phase_id or end_phase != self.phase_id:
            raise ValueError("round range must remain within one proxy phase")
        if start_aggregator > end_aggregator:
            raise ValueError("round range start must not exceed its end")

    @property
    def key(self) -> tuple[str, int]:
        return self.feed.feed_address.lower(), self.phase_id


@dataclass(frozen=True, slots=True)
class HistoricalIssue:
    """One historical round rejected without aborting the bounded run."""

    feed_address: str
    round_id: str
    reason: str
    processed_at: datetime
    retryable: bool


@dataclass(frozen=True, slots=True)
class HistoricalExtractionResult:
    """Canonical observations and progress from one bounded extraction."""

    observations: tuple[MarketDataObservation, ...]
    issues: tuple[HistoricalIssue, ...]
    next_round_ids: Mapping[tuple[str, int], int]
    rpc_requests: int
    rounds_attempted: int
    complete: bool


class ChainlinkHistoricalRpc(Protocol):
    request_count: int

    def get_latest_block(self) -> BlockSnapshot: ...

    def get_block_snapshot(self, block_number: int) -> BlockSnapshot: ...

    def get_feed_decimals(self, feed_address: str) -> int: ...

    def get_round_at_block(
        self, feed_address: str, block_number: int
    ) -> ChainlinkRound: ...

    def get_round(self, feed_address: str, round_id: int) -> ChainlinkRound: ...

    def get_rounds(
        self, feed_address: str, round_ids: tuple[int, ...]
    ) -> tuple[ChainlinkRound, ...]: ...

    def get_phase_latest_round(
        self, feed_address: str, phase_id: int
    ) -> int: ...


class ChainlinkHistoricalRpcClient:
    """Read phase-aware historical rounds through official feed proxies."""

    def __init__(
        self,
        rpc_url: str,
        timeout_seconds: float = 30,
        min_request_interval_seconds: float = 1.0,
    ) -> None:
        if min_request_interval_seconds < 0:
            raise ValueError("RPC request interval must not be negative")
        self.web3 = Web3(
            Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": timeout_seconds})
        )
        self.request_count = 0
        self._request_lock = Lock()
        self._min_request_interval_seconds = min_request_interval_seconds
        self._last_request_started = 0.0

    def _count_request(self) -> None:
        self._count_requests(1)

    def _count_requests(self, count: int) -> None:
        with self._request_lock:
            remaining = (
                self._min_request_interval_seconds
                - (monotonic() - self._last_request_started)
            )
            if remaining > 0:
                sleep(remaining)
            self._last_request_started = monotonic()
            self.request_count += count

    def _proxy(self, feed_address: str):
        return self.web3.eth.contract(
            address=Web3.to_checksum_address(feed_address),
            abi=_HISTORICAL_PROXY_ABI,
        )

    @staticmethod
    def _decode_round(values: tuple, *, expected_round_id: int | None = None) -> ChainlinkRound:
        round_id, answer, _started_at, updated_at, answered_in_round = map(int, values)
        if expected_round_id is not None and round_id != expected_round_id:
            raise ValueError("Chainlink proxy returned an unexpected round ID")
        if round_id <= 0 or answer <= 0 or updated_at <= 0:
            raise ValueError("Chainlink round contains non-positive required values")
        if answered_in_round < round_id:
            raise ValueError("Chainlink answeredInRound precedes the requested round")
        return ChainlinkRound(
            round_id=round_id,
            answer_raw=answer,
            updated_at=datetime.fromtimestamp(updated_at, tz=UTC).isoformat(),
        )

    def is_connected(self) -> bool:
        return self.web3.is_connected()

    def get_latest_block(self) -> BlockSnapshot:
        return self.get_block_snapshot("latest")

    def get_block_snapshot(self, block_number: int | str) -> BlockSnapshot:
        """Return one pinned block used as extraction provenance."""
        self._count_request()
        block = self.web3.eth.get_block(block_number)
        return BlockSnapshot(
            number=int(block["number"]),
            timestamp=datetime.fromtimestamp(int(block["timestamp"]), tz=UTC).isoformat(),
        )

    def get_feed_decimals(self, feed_address: str) -> int:
        self._count_request()
        return int(self._proxy(feed_address).functions.decimals().call())

    def get_round_at_block(
        self, feed_address: str, block_number: int
    ) -> ChainlinkRound:
        self._count_request()
        values = self._proxy(feed_address).functions.latestRoundData().call(
            block_identifier=block_number
        )
        return self._decode_round(values)

    def get_round(self, feed_address: str, round_id: int) -> ChainlinkRound:
        self._count_request()
        values = self._proxy(feed_address).functions.getRoundData(round_id).call()
        return self._decode_round(values, expected_round_id=round_id)

    def get_rounds(
        self,
        feed_address: str,
        round_ids: tuple[int, ...],
    ) -> tuple[ChainlinkRound, ...]:
        """Read many proxy rounds through one standard JSON-RPC batch."""
        if not round_ids:
            return ()
        proxy = self._proxy(feed_address)
        batch = self.web3.batch_requests()
        for round_id in round_ids:
            batch.add(proxy.functions.getRoundData(round_id).call())
        self._count_requests(len(round_ids))
        values = batch.execute()
        if len(values) != len(round_ids):
            raise ValueError("Chainlink RPC batch returned an unexpected result count")
        return tuple(
            self._decode_round(item, expected_round_id=round_id)
            for item, round_id in zip(values, round_ids)
        )

    def get_phase_latest_round(self, feed_address: str, phase_id: int) -> int:
        self._count_request()
        aggregator_address = (
            self._proxy(feed_address).functions.phaseAggregators(phase_id).call()
        )
        if int(aggregator_address, 16) == 0:
            raise ValueError(f"Chainlink phase {phase_id} has no aggregator")
        self._count_request()
        aggregator = self.web3.eth.contract(
            address=Web3.to_checksum_address(aggregator_address),
            abi=_AGGREGATOR_ABI,
        )
        return int(aggregator.functions.latestRound().call())


def is_retryable_rpc_error(error: Exception | str) -> bool:
    """Identify transient provider failures that must not advance a checkpoint."""
    message = str(error).lower()
    return any(
        marker in message
        for marker in (
            "429",
            "too many requests",
            "rate limit",
            "timed out",
            "timeout",
            "connection error",
            "connection reset",
            "temporarily unavailable",
            "service unavailable",
        )
    )


def discover_round_ranges(
    rpc: ChainlinkHistoricalRpc,
    feed: ChainlinkFeed,
    *,
    start_block: int,
    end_block: int,
) -> tuple[FeedRoundRange, ...]:
    """Discover phase-aware proxy rounds spanning an inclusive block window."""
    if start_block < 0 or end_block < start_block:
        raise ValueError("invalid historical block window")
    start = rpc.get_round_at_block(feed.feed_address, start_block)
    end = rpc.get_round_at_block(feed.feed_address, end_block)
    start_phase, start_aggregator = split_proxy_round_id(start.round_id)
    end_phase, end_aggregator = split_proxy_round_id(end.round_id)
    if end_phase < start_phase:
        raise ValueError("Chainlink proxy phase regressed across the block window")

    ranges = []
    for phase_id in range(start_phase, end_phase + 1):
        first = start_aggregator if phase_id == start_phase else 1
        last = (
            end_aggregator
            if phase_id == end_phase
            else rpc.get_phase_latest_round(feed.feed_address, phase_id)
        )
        ranges.append(
            FeedRoundRange(
                feed=feed,
                phase_id=phase_id,
                start_round_id=encode_proxy_round_id(phase_id, first),
                end_round_id=encode_proxy_round_id(phase_id, last),
            )
        )
    return tuple(ranges)


def merge_observations(
    existing: Iterable[MarketDataObservation],
    discovered: Iterable[MarketDataObservation],
) -> tuple[MarketDataObservation, ...]:
    """Deduplicate observation identities while rejecting data conflicts."""
    merged: dict[str, MarketDataObservation] = {}
    for observation in (*tuple(existing), *tuple(discovered)):
        current = merged.get(observation.observation_id)
        if current is not None:
            comparable = (
                observation.protocol,
                observation.event_type,
                observation.chain,
                observation.feed_address.lower(),
                observation.base_asset,
                observation.quote_asset,
                observation.round_id,
                observation.answer_raw,
                observation.feed_decimals,
                observation.feed_updated_at,
            )
            current_comparable = (
                current.protocol,
                current.event_type,
                current.chain,
                current.feed_address.lower(),
                current.base_asset,
                current.quote_asset,
                current.round_id,
                current.answer_raw,
                current.feed_decimals,
                current.feed_updated_at,
            )
            if comparable != current_comparable:
                raise ValueError(
                    f"Conflicting Chainlink observation: {observation.observation_id}"
                )
            continue
        merged[observation.observation_id] = observation
    return tuple(merged[key] for key in sorted(merged))


def extract_historical_rounds(
    rpc: ChainlinkHistoricalRpc,
    *,
    ranges: tuple[FeedRoundRange, ...],
    feed_decimals: Mapping[str, int],
    snapshot: BlockSnapshot,
    max_rpc_requests: int,
    next_round_ids: Mapping[tuple[str, int], int] | None = None,
    existing: Iterable[MarketDataObservation] = (),
    clock: Callable[[], datetime] | None = None,
    max_workers: int = 8,
    rpc_batch_size: int = 100,
) -> HistoricalExtractionResult:
    """Fetch a finite slice of proxy rounds and return resumable progress."""
    if max_rpc_requests <= 0 or max_workers <= 0 or rpc_batch_size <= 0:
        raise ValueError("request and worker limits must be greater than zero")
    processed_at = (clock or (lambda: datetime.now(UTC)))()
    existing_observations = tuple(existing)
    existing_observation_ids = {
        observation.observation_id for observation in existing_observations
    }
    checkpoints = dict(next_round_ids or {})
    targets: list[tuple[FeedRoundRange, int]] = []
    for round_range in ranges:
        current = max(
            round_range.start_round_id,
            checkpoints.get(round_range.key, round_range.start_round_id),
        )
        while current <= round_range.end_round_id and len(targets) < max_rpc_requests:
            targets.append((round_range, current))
            current += 1
        if len(targets) >= max_rpc_requests:
            break

    def fetch(target: tuple[FeedRoundRange, int]):
        round_range, round_id = target
        try:
            round_data = rpc.get_round(round_range.feed.feed_address, round_id)
            observation = MarketDataObservation.create(
                chain=round_range.feed.chain,
                feed_address=round_range.feed.feed_address,
                base_asset=round_range.feed.base_asset,
                quote_asset=round_range.feed.quote_asset,
                round_id=round_data.round_id,
                answer_raw=round_data.answer_raw,
                feed_decimals=feed_decimals[round_range.feed.feed_address.lower()],
                feed_updated_at=round_data.updated_at,
                block_number=snapshot.number,
                block_timestamp=snapshot.timestamp,
                observed_at=processed_at.isoformat(),
            )
            return observation, None
        except Exception as error:
            return None, HistoricalIssue(
                feed_address=round_range.feed.feed_address.lower(),
                round_id=str(round_id),
                reason=str(error),
                processed_at=processed_at,
                retryable=is_retryable_rpc_error(error),
            )

    request_count_before = rpc.request_count
    batch_reader = getattr(rpc, "get_rounds", None)
    if callable(batch_reader):
        collected = []
        offset = 0
        while offset < len(targets):
            first_range = targets[offset][0]
            batch_targets = []
            while (
                offset < len(targets)
                and len(batch_targets) < rpc_batch_size
                and targets[offset][0].key == first_range.key
            ):
                batch_targets.append(targets[offset])
                offset += 1
            round_ids = tuple(round_id for _range, round_id in batch_targets)
            try:
                round_data_batch = batch_reader(
                    first_range.feed.feed_address,
                    round_ids,
                )
                for target, round_data in zip(batch_targets, round_data_batch):
                    round_range, _round_id = target
                    observation = MarketDataObservation.create(
                        chain=round_range.feed.chain,
                        feed_address=round_range.feed.feed_address,
                        base_asset=round_range.feed.base_asset,
                        quote_asset=round_range.feed.quote_asset,
                        round_id=round_data.round_id,
                        answer_raw=round_data.answer_raw,
                        feed_decimals=feed_decimals[
                            round_range.feed.feed_address.lower()
                        ],
                        feed_updated_at=round_data.updated_at,
                        block_number=snapshot.number,
                        block_timestamp=snapshot.timestamp,
                        observed_at=processed_at.isoformat(),
                    )
                    collected.append((observation, None))
            except Exception as error:
                collected.extend(
                    (
                        None,
                        HistoricalIssue(
                            feed_address=round_range.feed.feed_address.lower(),
                            round_id=str(round_id),
                            reason=str(error),
                            processed_at=processed_at,
                            retryable=is_retryable_rpc_error(error),
                        ),
                    )
                    for round_range, round_id in batch_targets
                )
        outcomes = tuple(collected)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            outcomes = tuple(executor.map(fetch, targets))

    discovered = tuple(item for item, _issue in outcomes if item is not None)
    issues = tuple(issue for _item, issue in outcomes if issue is not None)
    attempted_by_range: dict[tuple[str, int], list[int]] = {}
    retryable_by_range: dict[tuple[str, int], list[int]] = {}
    for (round_range, round_id), (_observation, issue) in zip(targets, outcomes):
        attempted_by_range.setdefault(round_range.key, []).append(round_id)
        observation_id = (
            f"{round_range.feed.chain}:"
            f"{round_range.feed.feed_address.lower()}:{round_id}"
        )
        if (
            issue is not None
            and issue.retryable
            and observation_id not in existing_observation_ids
        ):
            retryable_by_range.setdefault(round_range.key, []).append(round_id)
    for key, attempted in attempted_by_range.items():
        retryable = retryable_by_range.get(key, [])
        checkpoints[key] = min(retryable) if retryable else max(attempted) + 1
    canonical = merge_observations(existing_observations, discovered)
    complete = all(
        checkpoints.get(round_range.key, round_range.start_round_id)
        > round_range.end_round_id
        for round_range in ranges
    )
    return HistoricalExtractionResult(
        observations=canonical,
        issues=issues,
        next_round_ids=checkpoints,
        rpc_requests=rpc.request_count - request_count_before,
        rounds_attempted=len(targets),
        complete=complete,
    )
