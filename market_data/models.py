"""Protocol-neutral message model for polled market observations."""

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from config.versions import PRODUCER_VERSION, SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class MarketDataObservation:
    """A point-in-time RPC observation with no fabricated event identity."""

    observation_id: str
    protocol: str
    event_type: str
    chain: str
    feed_address: str
    base_asset: str
    quote_asset: str
    round_id: str
    answer_raw: str
    feed_decimals: int
    feed_updated_at: str
    observed_at: str
    block_number: int
    block_timestamp: str
    ingested_at: str
    producer_version: str = PRODUCER_VERSION
    schema_version: str = SCHEMA_VERSION

    @classmethod
    def create(
        cls,
        *,
        chain: str,
        feed_address: str,
        base_asset: str,
        quote_asset: str,
        round_id: int,
        answer_raw: int,
        feed_decimals: int,
        feed_updated_at: str,
        block_number: int,
        block_timestamp: str,
        observed_at: str | None = None,
    ) -> "MarketDataObservation":
        """Create an exact Chainlink observation at a pinned block."""
        normalized_address = feed_address.lower()
        round_id_text = str(round_id)
        timestamp = observed_at or datetime.now(UTC).isoformat()
        return cls(
            observation_id=f"{chain}:{normalized_address}:{round_id_text}",
            protocol="chainlink",
            event_type="price_update",
            chain=chain,
            feed_address=normalized_address,
            base_asset=base_asset,
            quote_asset=quote_asset,
            round_id=round_id_text,
            answer_raw=str(answer_raw),
            feed_decimals=feed_decimals,
            feed_updated_at=feed_updated_at,
            observed_at=timestamp,
            block_number=block_number,
            block_timestamp=block_timestamp,
            ingested_at=timestamp,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-serializable Kafka message representation."""
        return asdict(self)
