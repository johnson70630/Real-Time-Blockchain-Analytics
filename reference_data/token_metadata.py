"""Canonical ERC-20 token metadata extraction and reconciliation logic."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from web3 import Web3

from reference_data.uniswap_v3_pools import normalize_address

METADATA_SOURCE = "erc20_call"
NATIVE_ASSET_SENTINELS = frozenset(
    {
        "0x" + "00" * 20,
        "0x" + "ee" * 20,
    }
)

ERC20_METADATA_ABI = [
    {
        "inputs": [],
        "name": name,
        "outputs": [{"name": "", "type": output_type}],
        "stateMutability": "view",
        "type": "function",
    }
    for name, output_type in (
        ("symbol", "string"),
        ("name", "string"),
        ("decimals", "uint8"),
    )
]


def _selector(signature: str) -> str:
    return "0x" + Web3.keccak(text=signature)[:4].hex()


ERC20_CALL_DATA = {
    entry["name"]: _selector(f"{entry['name']}()")
    for entry in ERC20_METADATA_ABI
}
_CODEC = Web3().codec


def _utc_timestamp(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")
    return value.astimezone(UTC)


def _raw_bytes(value: Any, field: str) -> bytes:
    if isinstance(value, str):
        if not value.startswith("0x"):
            raise ValueError(f"{field} result is malformed")
        try:
            return bytes.fromhex(value[2:])
        except ValueError:
            raise ValueError(f"{field} result is malformed") from None
    try:
        return bytes(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field} result is malformed") from None


def _validate_text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} result is malformed")
    normalized = value.strip().strip("\x00")
    if not normalized or not normalized.isprintable():
        raise ValueError(f"{field} result is empty or non-printable")
    return normalized


def decode_erc20_text(value: Any, field: str) -> str:
    """Decode canonical ABI strings with a deterministic bytes32 fallback."""
    raw = _raw_bytes(value, field)
    if len(raw) == 32:
        try:
            decoded = raw.rstrip(b"\x00").decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError(f"{field} bytes32 result is not UTF-8") from None
        return _validate_text(decoded, field)
    try:
        decoded = _CODEC.decode(["string"], raw)[0]
    except Exception:
        raise ValueError(f"{field} result is malformed") from None
    return _validate_text(decoded, field)


def decode_erc20_decimals(value: Any) -> int:
    """Decode and validate the canonical uint8 decimals response."""
    raw = _raw_bytes(value, "decimals")
    if len(raw) != 32:
        raise ValueError("decimals result is malformed")
    decimals = int.from_bytes(raw, byteorder="big", signed=False)
    if not 0 <= decimals <= 255:
        raise ValueError("decimals must be between 0 and 255")
    return decimals


@dataclass(frozen=True, slots=True)
class TokenMetadata:
    """Canonical metadata for one ERC-20 contract on one chain."""

    chain: str
    token_address: str
    symbol: str
    name: str
    decimals: int
    metadata_source: str
    metadata_block_number: int
    ingested_at: datetime
    processed_at: datetime

    def __post_init__(self) -> None:
        if not self.chain.strip():
            raise ValueError("chain must not be empty")
        object.__setattr__(self, "chain", self.chain.lower())
        object.__setattr__(
            self,
            "token_address",
            normalize_address(self.token_address, "token_address"),
        )
        object.__setattr__(self, "symbol", _validate_text(self.symbol, "symbol"))
        object.__setattr__(self, "name", _validate_text(self.name, "name"))
        if isinstance(self.decimals, bool) or not isinstance(self.decimals, int):
            raise ValueError("decimals must be an integer")
        if not 0 <= self.decimals <= 255:
            raise ValueError("decimals must be between 0 and 255")
        if self.metadata_source != METADATA_SOURCE:
            raise ValueError("metadata_source must be erc20_call")
        if self.metadata_block_number < 0:
            raise ValueError("metadata_block_number must not be negative")
        for field in ("ingested_at", "processed_at"):
            object.__setattr__(
                self,
                field,
                _utc_timestamp(getattr(self, field), field),
            )

    @property
    def natural_key(self) -> tuple[str, str]:
        return self.chain, self.token_address

    @property
    def token_definition(self) -> tuple[Any, ...]:
        return (
            self.natural_key,
            self.symbol,
            self.name,
            self.decimals,
            self.metadata_source,
        )


@dataclass(frozen=True, slots=True)
class TokenMetadataIssue:
    """One token address rejected from the canonical registry."""

    token_address: str
    reason: str
    metadata_block_number: int
    processed_at: datetime


@dataclass(frozen=True, slots=True)
class TokenDiscovery:
    """Normalized token addresses discovered from protocol datasets."""

    uniswap_addresses: tuple[str, ...]
    aave_addresses: tuple[str, ...]
    native_assets: tuple[str, ...]
    invalid_addresses: tuple[tuple[str, str], ...]

    @property
    def all_addresses(self) -> tuple[str, ...]:
        return tuple(sorted(set(self.uniswap_addresses) | set(self.aave_addresses)))

    @property
    def overlap(self) -> tuple[str, ...]:
        return tuple(sorted(set(self.uniswap_addresses) & set(self.aave_addresses)))


def _discover_source(
    values: Iterable[str | None],
    source: str,
) -> tuple[set[str], set[str], list[tuple[str, str]]]:
    addresses: set[str] = set()
    native_assets: set[str] = set()
    invalid: list[tuple[str, str]] = []
    for value in values:
        raw = "null" if value is None else str(value).strip()
        if not raw:
            native_assets.add(f"{source}:empty")
            continue
        lowered = raw.lower()
        if lowered == "null" or lowered in NATIVE_ASSET_SENTINELS:
            native_assets.add(f"{source}:{lowered}")
            continue
        try:
            addresses.add(normalize_address(raw, "token_address"))
        except ValueError as error:
            invalid.append((raw, str(error)))
    return addresses, native_assets, invalid


def discover_token_addresses(
    uniswap_values: Iterable[str | None],
    aave_values: Iterable[str | None],
) -> TokenDiscovery:
    """Normalize and deduplicate addresses while preserving source coverage."""
    uniswap, uniswap_native, uniswap_invalid = _discover_source(
        uniswap_values,
        "uniswap_v3",
    )
    aave, aave_native, aave_invalid = _discover_source(
        aave_values,
        "aave_v3",
    )
    return TokenDiscovery(
        uniswap_addresses=tuple(sorted(uniswap)),
        aave_addresses=tuple(sorted(aave)),
        native_assets=tuple(sorted(uniswap_native | aave_native)),
        invalid_addresses=tuple(sorted(uniswap_invalid + aave_invalid)),
    )


@dataclass(frozen=True, slots=True)
class RawTokenMetadata:
    symbol: str
    name: str
    decimals: int


class TokenMetadataRpc(Protocol):
    def get_latest_block_number(self) -> int: ...

    def get_token_metadata(
        self,
        token_address: str,
        block_number: int,
    ) -> RawTokenMetadata: ...


class Erc20MetadataRpcClient:
    """Read ERC-20 metadata through pinned, read-only Ethereum calls."""

    def __init__(self, rpc_url: str, timeout_seconds: float = 30) -> None:
        self.web3 = Web3(
            Web3.HTTPProvider(
                rpc_url,
                request_kwargs={"timeout": timeout_seconds},
            )
        )

    def is_connected(self) -> bool:
        return self.web3.is_connected()

    def get_latest_block_number(self) -> int:
        try:
            return int(self.web3.eth.block_number)
        except Exception:
            raise RuntimeError(
                "Alchemy RPC request failed while reading the latest block"
            ) from None

    def _call(
        self,
        token_address: str,
        method: str,
        block_number: int,
    ) -> Any:
        try:
            return self.web3.eth.call(
                {
                    "to": Web3.to_checksum_address(token_address),
                    "data": ERC20_CALL_DATA[method],
                },
                block_identifier=block_number,
            )
        except Exception:
            raise RuntimeError(f"{method} eth_call failed") from None

    def get_token_metadata(
        self,
        token_address: str,
        block_number: int,
    ) -> RawTokenMetadata:
        return RawTokenMetadata(
            symbol=decode_erc20_text(
                self._call(token_address, "symbol", block_number),
                "symbol",
            ),
            name=decode_erc20_text(
                self._call(token_address, "name", block_number),
                "name",
            ),
            decimals=decode_erc20_decimals(
                self._call(token_address, "decimals", block_number)
            ),
        )


@dataclass(frozen=True, slots=True)
class TokenBootstrapResult:
    records: tuple[TokenMetadata, ...]
    issues: tuple[TokenMetadataIssue, ...]
    metadata_block_number: int


def merge_token_records(
    existing: Iterable[TokenMetadata],
    discovered: Iterable[TokenMetadata],
) -> tuple[TokenMetadata, ...]:
    """Preserve exact reruns and reject conflicting canonical metadata."""
    by_token: dict[tuple[str, str], TokenMetadata] = {}
    for record in (*tuple(existing), *tuple(discovered)):
        match = by_token.get(record.natural_key)
        if match is not None:
            if match.token_definition != record.token_definition:
                raise ValueError(
                    f"Conflicting metadata for token {record.token_address}"
                )
            continue
        by_token[record.natural_key] = record
    return tuple(by_token[key] for key in sorted(by_token))


def bootstrap_token_metadata(
    rpc: TokenMetadataRpc,
    token_addresses: Iterable[str],
    *,
    chain: str,
    clock: Callable[[], datetime] | None = None,
) -> TokenBootstrapResult:
    """Read each distinct ERC-20 contract at one consistent block."""
    processed_at = (clock or (lambda: datetime.now(UTC)))()
    block_number = rpc.get_latest_block_number()
    addresses = sorted(
        {normalize_address(address, "token_address") for address in token_addresses}
    )
    records: list[TokenMetadata] = []
    issues: list[TokenMetadataIssue] = []
    for address in addresses:
        try:
            metadata = rpc.get_token_metadata(address, block_number)
            records.append(
                TokenMetadata(
                    chain=chain,
                    token_address=address,
                    symbol=metadata.symbol,
                    name=metadata.name,
                    decimals=metadata.decimals,
                    metadata_source=METADATA_SOURCE,
                    metadata_block_number=block_number,
                    ingested_at=processed_at,
                    processed_at=processed_at,
                )
            )
        except Exception as error:
            issues.append(
                TokenMetadataIssue(
                    token_address=address,
                    reason=str(error) or type(error).__name__,
                    metadata_block_number=block_number,
                    processed_at=_utc_timestamp(processed_at, "processed_at"),
                )
            )
    return TokenBootstrapResult(
        records=merge_token_records((), records),
        issues=tuple(issues),
        metadata_block_number=block_number,
    )
