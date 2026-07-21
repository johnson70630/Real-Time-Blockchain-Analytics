"""Shared EVM log normalization used by protocol plugins."""

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from web3 import Web3

from config.settings import CHAIN
from producer.models import EventEnvelope


def event_topic(signature: str) -> str:
    """Return the Keccak topic for a canonical EVM event signature."""
    return "0x" + Web3.keccak(text=signature).hex()


def hex_string(value: Any) -> str:
    """Normalize string and bytes-like RPC values to 0x-prefixed hex."""
    if isinstance(value, str):
        return value
    if not hasattr(value, "hex"):
        raise TypeError(f"Expected a string or bytes-like value, got {type(value)}")

    hex_value = value.hex()
    return hex_value if hex_value.startswith("0x") else f"0x{hex_value}"


def hex_int(value: int | str) -> int:
    """Normalize integer and hexadecimal RPC quantities to an integer."""
    return int(value, 16) if isinstance(value, str) else value


def first_log_topic(log: dict[str, Any]) -> str | None:
    """Return a normalized topic zero, or None for malformed topic data."""
    topics = log.get("topics") or []
    if not topics:
        return None

    try:
        return hex_string(topics[0]).lower()
    except TypeError:
        return None


def raw_log_payload(
    log: dict[str, Any],
    *,
    address_field: str,
) -> dict[str, Any]:
    """Return physical log provenance shared by EVM event payloads."""
    return {
        address_field: log["address"].lower(),
        "raw_data": hex_string(log["data"]),
        "raw_topics": [hex_string(topic) for topic in log["topics"]],
    }


class EvmEventHandler(ABC):
    """Decode an EVM log and wrap it in the protocol-neutral envelope."""

    protocol: ClassVar[str]
    event_type: ClassVar[str]
    topic: ClassVar[str]
    event_abi: ClassVar[dict[str, Any]]

    @abstractmethod
    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        """Decode handler-specific event arguments."""

    def build_event(
        self,
        log: dict[str, Any],
        block_timestamp: str | None,
        *,
        chain: str = CHAIN,
        protocol: str | None = None,
        ingested_at: str | None = None,
    ) -> EventEnvelope:
        """Build the common event envelope around a decoded payload."""
        return EventEnvelope.create(
            protocol=protocol or self.protocol,
            chain=chain,
            event_type=self.event_type,
            block_number=hex_int(log["blockNumber"]),
            transaction_hash=hex_string(log["transactionHash"]),
            log_index=hex_int(log["logIndex"]),
            block_timestamp=block_timestamp,
            ingested_at=ingested_at,
            payload=self.decode_payload(log),
        )
