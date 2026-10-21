"""Bounded recovery contracts for canonical Uniswap V3 pool events."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from time import sleep
from typing import Any, Iterable, Protocol

from web3 import Web3

from backfill.aave_v3 import BackfillIssue, RpcBlockRange
from producer.models import EventEnvelope
from producer.protocols.evm import hex_int, hex_string
from producer.protocols.uniswap_v3.handlers import (
    SUPPORTED_EVENT_TOPICS,
    get_event_handler,
)
from reference_data.uniswap_v3_pools import UniswapV3Pool, normalize_address


@dataclass(frozen=True, slots=True)
class UniswapSourceContract:
    """Frozen canonical pool and event-topic universe for one recovery run."""

    pool_addresses: tuple[str, ...]
    event_topics: tuple[str, ...] = SUPPORTED_EVENT_TOPICS

    def __post_init__(self) -> None:
        pools = tuple(
            sorted(
                {
                    normalize_address(address, "pool_address")
                    for address in self.pool_addresses
                }
            )
        )
        topics = tuple(sorted({topic.lower() for topic in self.event_topics}))
        if not pools:
            raise ValueError("Uniswap source contract requires canonical pools")
        if set(topics) != {topic.lower() for topic in SUPPORTED_EVENT_TOPICS}:
            raise ValueError("Uniswap source contract must cover Swap, Mint, and Burn")
        object.__setattr__(self, "pool_addresses", pools)
        object.__setattr__(self, "event_topics", topics)

    @property
    def fingerprint(self) -> str:
        """Return a deterministic audit fingerprint for the complete contract."""
        payload = json.dumps(
            {
                "pool_addresses": self.pool_addresses,
                "event_topics": self.event_topics,
            },
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()


def canonical_pool_contract(
    records: Iterable[UniswapV3Pool],
    *,
    chain: str,
) -> UniswapSourceContract:
    """Select only verified canonical pools for the requested chain."""
    addresses = tuple(
        record.pool_address
        for record in records
        if record.chain == chain.lower()
        and record.protocol == "uniswap_v3"
        and record.factory_verified is True
    )
    return UniswapSourceContract(addresses)


@dataclass(frozen=True, slots=True)
class QueryGroup:
    """One address-batch and event-topic sub-query required for completeness."""

    addresses: tuple[str, ...]
    topics: tuple[str, ...]


def query_groups(
    contract: UniswapSourceContract,
    address_batch_size: int,
) -> tuple[QueryGroup, ...]:
    """Return deterministic source sub-queries covering every pool/topic pair."""
    if address_batch_size <= 0:
        raise ValueError("address_batch_size must be greater than zero")
    batches = tuple(
        contract.pool_addresses[index : index + address_batch_size]
        for index in range(0, len(contract.pool_addresses), address_batch_size)
    )
    return tuple(
        QueryGroup(addresses, contract.event_topics)
        for addresses in batches
    )


class UniswapHistoricalRpc(Protocol):
    """RPC operations required by bounded Uniswap source recovery."""

    request_count: int

    def get_event_logs(
        self,
        addresses: tuple[str, ...],
        topics: tuple[str, ...],
        start_block: int,
        end_block: int,
    ) -> list[dict[str, Any]]: ...

    def get_block_timestamp(self, block_number: int) -> datetime: ...


@dataclass(frozen=True, slots=True)
class CompleteRangeFetch:
    """Logs and gap-free leaves after every source query group succeeds."""

    requested_range: RpcBlockRange
    completed_ranges: tuple[RpcBlockRange, ...]
    logs: tuple[dict[str, Any], ...]
    range_attempts: int
    query_requests: int
    split_count: int


class CompleteRangeFetchError(RuntimeError):
    """Raised when the complete pool/topic contract cannot be queried."""


def fetch_complete_range_adaptive(
    rpc: UniswapHistoricalRpc,
    *,
    contract: UniswapSourceContract,
    start_block: int,
    end_block: int,
    address_batch_size: int,
    minimum_range_size: int,
    max_range_attempts: int,
) -> CompleteRangeFetch:
    """Fetch every query group, splitting the whole range after any failure."""
    requested = RpcBlockRange(start_block, end_block)
    if minimum_range_size <= 0 or max_range_attempts <= 0:
        raise ValueError("Recovery limits must be greater than zero")
    groups = query_groups(contract, address_batch_size)
    pending = [requested]
    completed: list[RpcBlockRange] = []
    logs: list[dict[str, Any]] = []
    attempts = 0
    query_requests = 0
    splits = 0
    while pending:
        current = pending.pop()
        if attempts >= max_range_attempts:
            raise CompleteRangeFetchError(
                f"Uniswap range budget exhausted at "
                f"{current.start_block}-{current.end_block}"
            )
        attempts += 1
        current_logs: list[dict[str, Any]] = []
        try:
            for group in groups:
                query_requests += 1
                current_logs.extend(
                    rpc.get_event_logs(
                        group.addresses,
                        group.topics,
                        current.start_block,
                        current.end_block,
                    )
                )
        except Exception as error:
            if current.size <= minimum_range_size:
                raise CompleteRangeFetchError(
                    "Uniswap source query failed at minimum range "
                    f"{current.start_block}-{current.end_block}"
                ) from error
            midpoint = (current.start_block + current.end_block) // 2
            pending.extend(
                (
                    RpcBlockRange(midpoint + 1, current.end_block),
                    RpcBlockRange(current.start_block, midpoint),
                )
            )
            splits += 1
            continue
        completed.append(current)
        logs.extend(current_logs)
    completed.sort(key=lambda item: (item.start_block, item.end_block))
    return CompleteRangeFetch(
        requested,
        tuple(completed),
        tuple(logs),
        attempts,
        query_requests,
        splits,
    )


def _identity(log: dict[str, Any]) -> tuple[int | None, str | None, int | None]:
    try:
        block_number = hex_int(log["blockNumber"])
    except (KeyError, TypeError, ValueError):
        block_number = None
    try:
        transaction_hash = hex_string(log["transactionHash"]).lower()
    except (KeyError, TypeError, ValueError):
        transaction_hash = None
    try:
        log_index = hex_int(log["logIndex"])
    except (KeyError, TypeError, ValueError):
        log_index = None
    return block_number, transaction_hash, log_index


def merge_events(
    existing: Iterable[EventEnvelope],
    recovered: Iterable[EventEnvelope],
) -> tuple[EventEnvelope, ...]:
    """Converge stream and recovery copies on transaction hash plus log index."""
    by_identity: dict[tuple[str, str, int], EventEnvelope] = {}
    for event in (*tuple(existing), *tuple(recovered)):
        key = (event.chain, event.transaction_hash.lower(), event.log_index)
        current = by_identity.get(key)
        if current is not None:
            comparable = (
                event.protocol,
                event.event_type,
                event.block_number,
                event.block_timestamp,
                {key: value for key, value in event.payload.items() if value is not None},
            )
            current_comparable = (
                current.protocol,
                current.event_type,
                current.block_number,
                current.block_timestamp,
                {
                    key: value
                    for key, value in current.payload.items()
                    if value is not None
                },
            )
            if comparable != current_comparable:
                raise ValueError(f"Conflicting Uniswap event identity: {key}")
            continue
        by_identity[key] = event
    return tuple(by_identity[key] for key in sorted(by_identity))


def decode_recovery_logs(
    rpc: UniswapHistoricalRpc,
    *,
    logs: Iterable[dict[str, Any]],
    contract: UniswapSourceContract,
    chain: str,
    start_block: int,
    end_block: int,
    processed_at: datetime,
) -> tuple[tuple[EventEnvelope, ...], tuple[BackfillIssue, ...]]:
    """Decode valid canonical logs and quarantine malformed provider results."""
    allowed_addresses = set(contract.pool_addresses)
    allowed_topics = set(contract.event_topics)
    timestamps: dict[int, datetime] = {}
    events: list[EventEnvelope] = []
    issues: list[BackfillIssue] = []
    for log in logs:
        block_number, transaction_hash, log_index = _identity(log)
        try:
            address = normalize_address(log.get("address"), "log.address")
            if address not in allowed_addresses:
                raise ValueError("log address is outside the canonical pool universe")
            if block_number is None or not start_block <= block_number <= end_block:
                raise ValueError("log block is outside the requested range")
            handler = get_event_handler(log)
            if handler.topic.lower() not in allowed_topics:
                raise ValueError("log topic is outside the source contract")
            if block_number not in timestamps:
                raw_timestamp = log.get("blockTimestamp")
                timestamps[block_number] = (
                    datetime.fromtimestamp(hex_int(raw_timestamp), tz=UTC)
                    if raw_timestamp is not None
                    else rpc.get_block_timestamp(block_number)
                )
            events.append(
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
    return merge_events((), events), tuple(issues)


class UniswapV3RecoveryRpcClient:
    """Read canonical pool events from a bounded Ethereum JSON-RPC endpoint."""

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
        self,
        addresses: tuple[str, ...],
        topics: tuple[str, ...],
        start_block: int,
        end_block: int,
    ) -> list[dict[str, Any]]:
        request = {
            "address": [Web3.to_checksum_address(value) for value in addresses],
            "topics": [list(topics)],
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
                        "Uniswap eth_getLogs failed after retries for blocks "
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
                f"Uniswap block lookup failed for block {block_number}"
            ) from None
        return datetime.fromtimestamp(int(block["timestamp"]), tz=UTC)
