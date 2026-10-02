"""Uniswap V3 Factory PoolCreated extraction and reconciliation logic."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from web3 import Web3
from web3._utils.events import get_event_data

from config.versions import PRODUCER_VERSION, SCHEMA_VERSION
from producer.protocols.evm import event_topic, hex_int, hex_string
from producer.protocols.uniswap_v3.abi import (
    POOL_CREATED_EVENT_ABI,
    POOL_CREATED_EVENT_SIGNATURE,
    POOL_REFERENCE_ABI,
)
from producer.protocols.uniswap_v3.constants import PROTOCOL_NAME

POOL_CREATED_TOPIC = event_topic(POOL_CREATED_EVENT_SIGNATURE)
_ADDRESS_LENGTH = 42
_TRANSACTION_HASH_LENGTH = 66
_CODEC = Web3().codec


def normalize_address(value: str, field: str) -> str:
    """Return a validated lowercase EVM address."""
    if not isinstance(value, str) or not Web3.is_address(value):
        raise ValueError(f"{field} must be a valid EVM address")
    normalized = value.lower()
    if len(normalized) != _ADDRESS_LENGTH:
        raise ValueError(f"{field} must be a 20-byte EVM address")
    return normalized


def normalize_transaction_hash(value: Any) -> str:
    """Return a validated lowercase transaction hash."""
    normalized = hex_string(value).lower()
    if len(normalized) != _TRANSACTION_HASH_LENGTH:
        raise ValueError("created_transaction_hash must be 32 bytes")
    try:
        int(normalized[2:], 16)
    except ValueError as error:
        raise ValueError(
            "created_transaction_hash must be hexadecimal"
        ) from error
    return normalized


def _utc_timestamp(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class UniswapV3Pool:
    """Canonical immutable metadata for one Uniswap V3 pool."""

    chain: str
    protocol: str
    pool_address: str
    token0_address: str
    token1_address: str
    fee_tier: int
    tick_spacing: int
    factory_address: str
    created_block: int | None
    created_transaction_hash: str | None
    created_log_index: int | None
    created_block_timestamp: datetime | None
    ingested_at: datetime
    producer_version: str
    schema_version: str
    processed_at: datetime
    metadata_source: str
    factory_verified: bool

    def __post_init__(self) -> None:
        if not self.chain.strip():
            raise ValueError("chain must not be empty")
        if not self.protocol.strip():
            raise ValueError("protocol must not be empty")
        for field in (
            "pool_address",
            "token0_address",
            "token1_address",
            "factory_address",
        ):
            object.__setattr__(
                self,
                field,
                normalize_address(getattr(self, field), field),
            )
        if self.token0_address == self.token1_address:
            raise ValueError("token0_address and token1_address must differ")
        if self.fee_tier < 0:
            raise ValueError("fee_tier must not be negative")
        if self.metadata_source not in {
            "factory_pool_created",
            "direct_pool_call",
        }:
            raise ValueError("metadata_source is not supported")
        if self.factory_verified is not True:
            raise ValueError("canonical pools must have a verified Factory")

        provenance = (
            self.created_block,
            self.created_transaction_hash,
            self.created_log_index,
            self.created_block_timestamp,
        )
        if any(value is not None for value in provenance) and any(
            value is None for value in provenance
        ):
            raise ValueError("creation provenance must be complete or entirely null")
        if self.metadata_source == "factory_pool_created" and all(
            value is None for value in provenance
        ):
            raise ValueError("Factory event metadata requires creation provenance")
        if self.created_transaction_hash is not None:
            object.__setattr__(
                self,
                "created_transaction_hash",
                normalize_transaction_hash(self.created_transaction_hash),
            )
        if self.created_block is not None and self.created_block < 0:
            raise ValueError("created_block must not be negative")
        if self.created_log_index is not None and self.created_log_index < 0:
            raise ValueError("created_log_index must not be negative")
        if self.created_block_timestamp is not None:
            object.__setattr__(
                self,
                "created_block_timestamp",
                _utc_timestamp(
                    self.created_block_timestamp,
                    "created_block_timestamp",
                ),
            )
        for field in ("ingested_at", "processed_at"):
            object.__setattr__(
                self,
                field,
                _utc_timestamp(getattr(self, field), field),
            )

    @property
    def natural_key(self) -> tuple[str, str, str]:
        """Return the canonical pool uniqueness key."""
        return self.chain, self.protocol, self.pool_address

    @property
    def event_key(self) -> tuple[str, str, int] | None:
        """Return the source event identity when provenance is available."""
        if (
            self.created_transaction_hash is None
            or self.created_log_index is None
        ):
            return None
        return (
            self.chain,
            self.created_transaction_hash,
            self.created_log_index,
        )

    @property
    def pool_definition(self) -> tuple[Any, ...]:
        """Return contract values shared by both metadata sources."""
        return (
            self.natural_key,
            self.token0_address,
            self.token1_address,
            self.fee_tier,
            self.tick_spacing,
            self.factory_address,
        )

    @property
    def creation_provenance(self) -> tuple[Any, ...]:
        """Return nullable authoritative Factory event provenance."""
        return (
            self.created_block,
            self.event_key,
            self.created_block_timestamp,
        )


def decode_pool_created_log(
    log: dict[str, Any],
    *,
    chain: str,
    factory_address: str,
    block_timestamp: datetime,
    observed_at: datetime,
) -> UniswapV3Pool:
    """Decode one canonical Factory PoolCreated log."""
    expected_factory = normalize_address(factory_address, "factory_address")
    log_factory = normalize_address(log.get("address"), "log.address")
    if log_factory != expected_factory:
        raise ValueError("PoolCreated log address does not match the Factory")
    topics = log.get("topics") or []
    if not topics or hex_string(topics[0]).lower() != POOL_CREATED_TOPIC.lower():
        raise ValueError("Log does not have the canonical PoolCreated topic")

    args = get_event_data(_CODEC, POOL_CREATED_EVENT_ABI, log)["args"]
    timestamp = _utc_timestamp(observed_at, "observed_at")
    return UniswapV3Pool(
        chain=chain.lower(),
        protocol=PROTOCOL_NAME,
        pool_address=args["pool"],
        token0_address=args["token0"],
        token1_address=args["token1"],
        fee_tier=int(args["fee"]),
        tick_spacing=int(args["tickSpacing"]),
        factory_address=log_factory,
        created_block=hex_int(log["blockNumber"]),
        created_transaction_hash=hex_string(log["transactionHash"]),
        created_log_index=hex_int(log["logIndex"]),
        created_block_timestamp=block_timestamp,
        ingested_at=timestamp,
        producer_version=PRODUCER_VERSION,
        schema_version=SCHEMA_VERSION,
        processed_at=timestamp,
        metadata_source="factory_pool_created",
        factory_verified=True,
    )


@dataclass(frozen=True, slots=True)
class DirectPoolMetadata:
    """Values returned directly by the immutable V3 pool interface."""

    token0_address: str
    token1_address: str
    fee_tier: int
    tick_spacing: int
    factory_address: str


class ObservedPoolRpc(Protocol):
    """RPC operations required by the observed-pool bootstrap."""

    def get_latest_block_number(self) -> int: ...

    def get_pool_metadata(
        self,
        pool_address: str,
        block_number: int,
    ) -> DirectPoolMetadata: ...


def block_ranges(
    start_block: int,
    end_block: int,
    chunk_size: int,
) -> Iterator[tuple[int, int]]:
    """Yield inclusive, gap-free block ranges."""
    if start_block < 0 or end_block < 0:
        raise ValueError("Block boundaries must not be negative")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if end_block < start_block:
        return
    current = start_block
    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)
        yield current, chunk_end
        current = chunk_end + 1


def next_start_block(
    configured_start: int,
    watermark: int | None,
    requested_start: int | None = None,
) -> int:
    """Resolve a restartable start block with optional overlap override."""
    if configured_start < 0:
        raise ValueError("configured_start must not be negative")
    if requested_start is not None:
        if requested_start < configured_start:
            raise ValueError(
                "requested start block precedes the configured deployment block"
            )
        return requested_start
    if watermark is None:
        return configured_start
    if watermark < 0:
        raise ValueError("watermark must not be negative")
    return max(configured_start, watermark + 1)


class PoolMetadataRpc(Protocol):
    """RPC operations required by the historical extractor."""

    def get_latest_block_number(self) -> int: ...

    def get_pool_created_logs(
        self,
        factory_address: str,
        start_block: int,
        end_block: int,
    ) -> list[dict[str, Any]]: ...

    def get_block_timestamp(self, block_number: int) -> datetime: ...


class UniswapV3FactoryRpcClient:
    """Read historical PoolCreated logs through standard Ethereum RPC."""

    def __init__(self, rpc_url: str, timeout_seconds: float = 30) -> None:
        self.web3 = Web3(
            Web3.HTTPProvider(
                rpc_url,
                request_kwargs={"timeout": timeout_seconds},
            )
        )

    def is_connected(self) -> bool:
        """Return whether the configured endpoint is reachable."""
        return self.web3.is_connected()

    def get_latest_block_number(self) -> int:
        """Return the current chain head block number."""
        try:
            return int(self.web3.eth.block_number)
        except Exception:
            raise RuntimeError(
                "Alchemy RPC request failed while reading the latest block"
            ) from None

    def get_pool_created_logs(
        self,
        factory_address: str,
        start_block: int,
        end_block: int,
    ) -> list[dict[str, Any]]:
        """Read canonical Factory logs for one inclusive block range."""
        try:
            return list(
                self.web3.eth.get_logs(
                    {
                        "address": Web3.to_checksum_address(factory_address),
                        "topics": [POOL_CREATED_TOPIC],
                        "fromBlock": start_block,
                        "toBlock": end_block,
                    }
                )
            )
        except Exception:
            raise RuntimeError(
                "Alchemy RPC eth_getLogs failed for blocks "
                f"{start_block}-{end_block}"
            ) from None

    def get_block_timestamp(self, block_number: int) -> datetime:
        """Resolve one creation block timestamp in UTC."""
        try:
            block = self.web3.eth.get_block(block_number)
        except Exception:
            raise RuntimeError(
                f"Alchemy RPC block lookup failed for block {block_number}"
            ) from None
        return datetime.fromtimestamp(int(block["timestamp"]), tz=UTC)

    def get_pool_metadata(
        self,
        pool_address: str,
        block_number: int,
    ) -> DirectPoolMetadata:
        """Read immutable pool metadata at one pinned block."""
        try:
            contract = self.web3.eth.contract(
                address=Web3.to_checksum_address(pool_address),
                abi=POOL_REFERENCE_ABI,
            )
        except Exception:
            raise RuntimeError("pool contract initialization failed") from None

        def read(method_name: str) -> Any:
            try:
                method = getattr(contract.functions, method_name)
                return method().call(block_identifier=block_number)
            except Exception:
                raise RuntimeError(f"{method_name} eth_call failed") from None

        return DirectPoolMetadata(
            token0_address=read("token0"),
            token1_address=read("token1"),
            fee_tier=int(read("fee")),
            tick_spacing=int(read("tickSpacing")),
            factory_address=read("factory"),
        )


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """PoolCreated records and deterministic extraction statistics."""

    records: tuple[UniswapV3Pool, ...]
    events_retrieved: int
    duplicate_events: int
    chunks_processed: int


def merge_pool_records(
    existing: Iterable[UniswapV3Pool],
    discovered: Iterable[UniswapV3Pool],
) -> tuple[UniswapV3Pool, ...]:
    """Merge exact reruns while rejecting conflicting pool identities."""
    by_pool: dict[tuple[str, str, str], UniswapV3Pool] = {}
    by_event: dict[tuple[str, str, int], UniswapV3Pool] = {}
    for record in (*tuple(existing), *tuple(discovered)):
        pool_match = by_pool.get(record.natural_key)
        event_match = (
            by_event.get(record.event_key)
            if record.event_key is not None
            else None
        )
        if pool_match is not None:
            if pool_match.pool_definition != record.pool_definition:
                raise ValueError(
                    f"Conflicting metadata for pool {record.pool_address}"
                )
            if (
                pool_match.event_key is not None
                and record.event_key is not None
                and pool_match.creation_provenance
                != record.creation_provenance
            ):
                raise ValueError(
                    f"Conflicting creation provenance for pool "
                    f"{record.pool_address}"
                )
            if (
                pool_match.metadata_source == "direct_pool_call"
                and record.metadata_source == "factory_pool_created"
            ):
                by_pool[record.natural_key] = record
                if record.event_key is not None:
                    by_event[record.event_key] = record
            continue
        if event_match is not None:
            raise ValueError(
                "One PoolCreated event resolves to multiple pool addresses"
            )
        by_pool[record.natural_key] = record
        if record.event_key is not None:
            by_event[record.event_key] = record
    return tuple(by_pool[key] for key in sorted(by_pool))


def extract_pool_metadata(
    rpc: PoolMetadataRpc,
    *,
    chain: str,
    factory_address: str,
    start_block: int,
    end_block: int,
    chunk_size: int,
    clock: Callable[[], datetime] | None = None,
) -> ExtractionResult:
    """Extract, decode, and deduplicate PoolCreated logs by block range."""
    now = (clock or (lambda: datetime.now(UTC)))()
    records: list[UniswapV3Pool] = []
    timestamp_cache: dict[int, datetime] = {}
    chunks = 0
    for chunk_start, chunk_end in block_ranges(
        start_block,
        end_block,
        chunk_size,
    ):
        chunks += 1
        logs = rpc.get_pool_created_logs(
            factory_address,
            chunk_start,
            chunk_end,
        )
        for log in logs:
            block_number = hex_int(log["blockNumber"])
            if not chunk_start <= block_number <= chunk_end:
                raise ValueError("RPC returned a log outside the requested range")
            if block_number not in timestamp_cache:
                timestamp_cache[block_number] = rpc.get_block_timestamp(
                    block_number
                )
            records.append(
                decode_pool_created_log(
                    log,
                    chain=chain,
                    factory_address=factory_address,
                    block_timestamp=timestamp_cache[block_number],
                    observed_at=now,
                )
            )
    unique = merge_pool_records((), records)
    return ExtractionResult(
        records=unique,
        events_retrieved=len(records),
        duplicate_events=len(records) - len(unique),
        chunks_processed=chunks,
    )


@dataclass(frozen=True, slots=True)
class PoolBootstrapIssue:
    """One observed address rejected from the canonical registry."""

    pool_address: str
    reason: str
    checked_block: int
    processed_at: datetime


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    """Verified direct-call records and explicit rejected addresses."""

    records: tuple[UniswapV3Pool, ...]
    issues: tuple[PoolBootstrapIssue, ...]
    checked_block: int


def _direct_pool_record(
    *,
    pool_address: str,
    metadata: DirectPoolMetadata,
    chain: str,
    expected_factory: str,
    observed_at: datetime,
) -> UniswapV3Pool:
    actual_factory = normalize_address(
        metadata.factory_address,
        "factory_address",
    )
    configured_factory = normalize_address(
        expected_factory,
        "expected_factory",
    )
    if actual_factory != configured_factory:
        raise ValueError(
            f"factory mismatch: expected {configured_factory}, "
            f"received {actual_factory}"
        )
    timestamp = _utc_timestamp(observed_at, "observed_at")
    return UniswapV3Pool(
        chain=chain.lower(),
        protocol=PROTOCOL_NAME,
        pool_address=pool_address,
        token0_address=metadata.token0_address,
        token1_address=metadata.token1_address,
        fee_tier=metadata.fee_tier,
        tick_spacing=metadata.tick_spacing,
        factory_address=actual_factory,
        created_block=None,
        created_transaction_hash=None,
        created_log_index=None,
        created_block_timestamp=None,
        ingested_at=timestamp,
        producer_version=PRODUCER_VERSION,
        schema_version=SCHEMA_VERSION,
        processed_at=timestamp,
        metadata_source="direct_pool_call",
        factory_verified=True,
    )


def bootstrap_observed_pools(
    rpc: ObservedPoolRpc,
    observed_pool_addresses: Iterable[str],
    *,
    chain: str,
    expected_factory: str,
    clock: Callable[[], datetime] | None = None,
) -> BootstrapResult:
    """Verify observed contracts and build canonical direct-call records."""
    observed_at = (clock or (lambda: datetime.now(UTC)))()
    checked_block = rpc.get_latest_block_number()
    addresses = sorted(
        {
            normalize_address(address, "observed_pool_address")
            for address in observed_pool_addresses
        }
    )
    records: list[UniswapV3Pool] = []
    issues: list[PoolBootstrapIssue] = []
    for address in addresses:
        try:
            metadata = rpc.get_pool_metadata(address, checked_block)
            records.append(
                _direct_pool_record(
                    pool_address=address,
                    metadata=metadata,
                    chain=chain,
                    expected_factory=expected_factory,
                    observed_at=observed_at,
                )
            )
        except Exception as error:
            issues.append(
                PoolBootstrapIssue(
                    pool_address=address,
                    reason=str(error) or type(error).__name__,
                    checked_block=checked_block,
                    processed_at=_utc_timestamp(observed_at, "observed_at"),
                )
            )
    return BootstrapResult(
        records=merge_pool_records((), records),
        issues=tuple(issues),
        checked_block=checked_block,
    )


@dataclass(frozen=True, slots=True)
class CoverageResult:
    """Coverage of observed Swap pools by the canonical registry."""

    observed_pools: int
    mapped_pools: int
    missing_pools: tuple[str, ...]

    @property
    def coverage_percentage(self) -> float:
        if self.observed_pools == 0:
            return 100.0
        return self.mapped_pools * 100.0 / self.observed_pools


def reconcile_pool_coverage(
    observed_pool_addresses: Iterable[str],
    pool_registry: Iterable[UniswapV3Pool],
) -> CoverageResult:
    """Compare distinct observed Swap pools with registered pool metadata."""
    observed = {
        normalize_address(address, "observed_pool_address")
        for address in observed_pool_addresses
    }
    mapped = {record.pool_address for record in pool_registry}
    missing = tuple(sorted(observed - mapped))
    return CoverageResult(
        observed_pools=len(observed),
        mapped_pools=len(observed) - len(missing),
        missing_pools=missing,
    )
