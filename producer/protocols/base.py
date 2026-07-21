"""Interface implemented by protocol-specific ingestion plugins."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from producer.models import EventEnvelope


@dataclass(frozen=True, slots=True)
class LogSubscription:
    """Protocol-neutral Ethereum log subscription filter."""

    topics: tuple[str, ...]
    addresses: tuple[str, ...] = ()


class ProtocolPlugin(ABC):
    """Decode supported raw logs for one blockchain protocol."""

    @property
    @abstractmethod
    def protocol(self) -> str:
        """Return the stable protocol identifier used in event envelopes."""

    @property
    @abstractmethod
    def subscription_topics(self) -> tuple[str, ...]:
        """Return log topics required by this plugin's websocket subscription."""

    @property
    @abstractmethod
    def event_types(self) -> tuple[str, ...]:
        """Return normalized event types supported by this plugin."""

    @property
    def subscriptions(self) -> tuple[LogSubscription, ...]:
        """Return websocket filters required by this plugin."""
        return (LogSubscription(self.subscription_topics),)

    @abstractmethod
    def can_handle(self, raw_event: dict[str, Any]) -> bool:
        """Return whether this plugin recognizes the raw blockchain event."""

    @abstractmethod
    def normalize(
        self,
        raw_event: dict[str, Any],
        block_timestamp: str | None,
        *,
        chain: str,
    ) -> EventEnvelope:
        """Decode a raw blockchain event into the common envelope."""
