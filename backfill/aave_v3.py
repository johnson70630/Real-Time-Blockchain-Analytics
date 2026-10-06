"""Bounded, resumable extraction of historical Aave V3 Pool events."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from time import sleep
from typing import Any, Callable, Iterable, Protocol

from web3 import Web3

from producer.models import EventEnvelope
from producer.protocols.aave_v3.handlers import (
    SUPPORTED_EVENT_TOPICS,
    get_event_handler,
)
from producer.protocols.evm import hex_int, hex_string


@dataclass(frozen=True, slots=True)
class SearchLimits:
    """Finite limits and collection targets for one backwards search."""

    chunk_size: int = 10
    max_blocks: int = 2_000
    max_rpc_requests: int = 500
    target_borrow: int = 20
    target_repay: int = 20
    target_liquidation: int = 3

    def __post_init__(self) -> None:
        for name in ("chunk_size", "max_blocks", "max_rpc_requests"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be greater than zero")
        for name in ("target_borrow", "target_repay", "target_liquidation"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative")

    @property
    def targets(self) -> dict[str, int]:
        return {
            "borrow": self.target_borrow,
            "repay": self.target_repay,
            "liquidation": self.target_liquidation,
        }


@dataclass(frozen=True, slots=True)
class BackfillIssue:
    """One RPC log rejected without stopping the bounded extraction."""

    block_number: int | None
    transaction_hash: str | None
    log_index: int | None
    reason: str
    processed_at: datetime


@dataclass(frozen=True, slots=True)
class SearchResult:
    """Canonical events and operational statistics from one search."""

    events: tuple[EventEnvelope, ...]
    issues: tuple[BackfillIssue, ...]
    start_block: int
    end_block: int
    blocks_scanned: int
    rpc_requests: int
    next_end_block: int
    targets_met: bool


class AaveHistoricalRpc(Protocol):
    request_count: int

    def get_event_logs(
        self, pool_address: str, start_block: int, end_block: int
    ) -> list[dict[str, Any]]: ...

    def get_block_timestamp(self, block_number: int) -> datetime: ...


class AaveV3RpcClient:
    """Read official Aave Pool logs through standard Ethereum JSON-RPC."""

    def __init__(self, rpc_url: str, timeout_seconds: float = 30) -> None:
        self.web3 = Web3(
            Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": timeout_seconds})
        )
        self.request_count = 0

    def is_connected(self) -> bool:
        return self.web3.is_connected()

    def get_latest_block_number(self) -> int:
        self.request_count += 1
        try:
            return int(self.web3.eth.block_number)
        except Exception:
            raise RuntimeError("Failed to read the latest Arbitrum block") from None

    def get_event_logs(
        self, pool_address: str, start_block: int, end_block: int
    ) -> list[dict[str, Any]]:
        """Query all supported topic-zero values in one bounded request."""
        request = {
            "address": Web3.to_checksum_address(pool_address),
            "topics": [list(SUPPORTED_EVENT_TOPICS)],
            "fromBlock": start_block,
            "toBlock": end_block,
        }
        for attempt in range(4):
            self.request_count += 1
            try:
                return list(self.web3.eth.get_logs(request))
            except Exception:
                if attempt == 3:
                    raise RuntimeError(
                        "Aave eth_getLogs failed after retries for blocks "
                        f"{start_block}-{end_block}"
                    ) from None
                sleep(2**attempt)
        raise AssertionError("unreachable")

    def get_block_timestamp(self, block_number: int) -> datetime:
        self.request_count += 1
        try:
            block = self.web3.eth.get_block(block_number)
        except Exception:
            raise RuntimeError(
                f"Aave block lookup failed for block {block_number}"
            ) from None
        return datetime.fromtimestamp(int(block["timestamp"]), tz=UTC)


def backwards_block_ranges(
    end_block: int, max_blocks: int, chunk_size: int
) -> tuple[tuple[int, int], ...]:
    """Return descending, inclusive, gap-free ranges within a finite budget."""
    if end_block < 0:
        raise ValueError("end_block must not be negative")
    if max_blocks <= 0 or chunk_size <= 0:
        raise ValueError("block limits must be greater than zero")
    floor = max(0, end_block - max_blocks + 1)
    ranges = []
    current_end = end_block
    while current_end >= floor:
        start = max(floor, current_end - chunk_size + 1)
        ranges.append((start, current_end))
        current_end = start - 1
    return tuple(ranges)


def event_id(event: EventEnvelope) -> str:
    """Return the same deterministic identity used by Aave Silver."""
    return f"{event.chain}-{event.transaction_hash}-{event.log_index}"


def preserve_backward_checkpoint(
    current_next_end: int | None, candidate_next_end: int
) -> int:
    """Never regress a backwards checkpoint during an explicit overlap run."""
    if current_next_end is None:
        return candidate_next_end
    return min(current_next_end, candidate_next_end)


def merge_events(
    existing: Iterable[EventEnvelope], discovered: Iterable[EventEnvelope]
) -> tuple[EventEnvelope, ...]:
    """Deduplicate reruns by natural identity and reject conflicting events."""
    merged: dict[tuple[str, str, int], EventEnvelope] = {}

    def payload_signature(event: EventEnvelope) -> dict[str, Any]:
        return {key: value for key, value in event.payload.items() if value is not None}

    for event in (*tuple(existing), *tuple(discovered)):
        key = (event.chain, event.transaction_hash.lower(), event.log_index)
        current = merged.get(key)
        if current is not None:
            comparable = (
                event.protocol,
                event.chain,
                event.event_type,
                event.block_number,
                event.transaction_hash.lower(),
                event.log_index,
                event.block_timestamp,
                payload_signature(event),
                event.producer_version,
                event.schema_version,
            )
            current_comparable = (
                current.protocol,
                current.chain,
                current.event_type,
                current.block_number,
                current.transaction_hash.lower(),
                current.log_index,
                current.block_timestamp,
                payload_signature(current),
                current.producer_version,
                current.schema_version,
            )
            if comparable != current_comparable:
                raise ValueError(f"Conflicting Aave event identity: {key}")
            continue
        merged[key] = event
    return tuple(merged[key] for key in sorted(merged))


def _safe_log_identity(log: dict[str, Any]) -> tuple[int | None, str | None, int | None]:
    try:
        block = hex_int(log["blockNumber"])
    except (KeyError, TypeError, ValueError):
        block = None
    try:
        transaction = hex_string(log["transactionHash"]).lower()
    except (KeyError, TypeError, ValueError):
        transaction = None
    try:
        index = hex_int(log["logIndex"])
    except (KeyError, TypeError, ValueError):
        index = None
    return block, transaction, index


def search_historical_events(
    rpc: AaveHistoricalRpc,
    *,
    pool_address: str,
    chain: str,
    end_block: int,
    limits: SearchLimits,
    existing: Iterable[EventEnvelope] = (),
    clock: Callable[[], datetime] | None = None,
) -> SearchResult:
    """Scan backwards until targets or finite block/request limits are reached."""
    processed_at = (clock or (lambda: datetime.now(UTC)))()
    canonical = merge_events((), existing)
    counts = Counter(event.event_type for event in canonical)
    issues: list[BackfillIssue] = []
    timestamps: dict[int, datetime] = {}
    blocks_scanned = 0
    next_end = end_block
    searched_start = end_block
    failed_end_block: int | None = None

    def targets_met() -> bool:
        return all(counts[name] >= target for name, target in limits.targets.items())

    for chunk_start, chunk_end in backwards_block_ranges(
        end_block, limits.max_blocks, limits.chunk_size
    ):
        if targets_met() or rpc.request_count >= limits.max_rpc_requests:
            break
        try:
            logs = rpc.get_event_logs(pool_address, chunk_start, chunk_end)
        except Exception as error:
            issues.append(
                BackfillIssue(
                    chunk_start,
                    None,
                    None,
                    f"range {chunk_start}-{chunk_end}: {error}",
                    processed_at,
                )
            )
            searched_start = chunk_start
            failed_end_block = chunk_end
            break
        blocks_scanned += chunk_end - chunk_start + 1
        searched_start = chunk_start
        chunk_complete = True
        decoded: list[EventEnvelope] = []
        for log in logs:
            block_number, transaction_hash, log_index = _safe_log_identity(log)
            try:
                if str(log.get("address", "")).lower() != pool_address.lower():
                    raise ValueError("log address does not match configured Aave Pool")
                if block_number is None or not chunk_start <= block_number <= chunk_end:
                    raise ValueError("log block is outside the requested range")
                if block_number not in timestamps:
                    if rpc.request_count >= limits.max_rpc_requests:
                        chunk_complete = False
                        break
                    timestamps[block_number] = rpc.get_block_timestamp(block_number)
                handler = get_event_handler(log)
                decoded.append(
                    handler.build_event(
                        log,
                        timestamps[block_number].isoformat(),
                        chain=chain,
                        ingested_at=processed_at.isoformat(),
                    )
                )
            except Exception as error:
                issues.append(
                    BackfillIssue(
                        block_number,
                        transaction_hash,
                        log_index,
                        str(error),
                        processed_at,
                    )
                )
        canonical = merge_events(canonical, decoded)
        counts = Counter(event.event_type for event in canonical)
        if not chunk_complete:
            break
        next_end = chunk_start - 1

    if failed_end_block is not None:
        next_end = failed_end_block

    return SearchResult(
        events=canonical,
        issues=tuple(issues),
        start_block=searched_start,
        end_block=end_block,
        blocks_scanned=blocks_scanned,
        rpc_requests=rpc.request_count,
        next_end_block=next_end,
        targets_met=targets_met(),
    )
