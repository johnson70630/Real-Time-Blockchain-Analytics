"""Protocol-neutral event models shared by every ingestion plugin."""

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from config.versions import PRODUCER_VERSION, SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """Normalized event contract published to Kafka by every plugin."""

    protocol: str
    chain: str
    event_type: str
    block_number: int
    transaction_hash: str
    log_index: int
    block_timestamp: str | None
    ingested_at: str
    payload: dict[str, Any]
    producer_version: str = PRODUCER_VERSION
    schema_version: str = SCHEMA_VERSION

    @classmethod
    def create(
        cls,
        *,
        protocol: str,
        chain: str,
        event_type: str,
        block_number: int,
        transaction_hash: str,
        log_index: int,
        block_timestamp: str | None,
        payload: dict[str, Any],
        ingested_at: str | None = None,
    ) -> "EventEnvelope":
        """Create an envelope with a generated UTC ingestion timestamp."""
        return cls(
            protocol=protocol,
            chain=chain,
            event_type=event_type,
            block_number=block_number,
            transaction_hash=transaction_hash,
            log_index=log_index,
            block_timestamp=block_timestamp,
            ingested_at=ingested_at or datetime.now(UTC).isoformat(),
            payload=payload,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-serializable Kafka message representation."""
        return asdict(self)
