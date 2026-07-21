"""Protocol-neutral routing for raw blockchain events."""

from typing import Any

from producer.models import EventEnvelope
from producer.protocols.base import LogSubscription
from producer.registry import ProtocolRegistry, UnsupportedEventError


class EventParsingError(ValueError):
    """Raised when a recognized event cannot be decoded by its plugin."""


class EventDispatcher:
    """Route raw events to enabled protocol plugins and normalize them."""

    def __init__(
        self,
        registry: ProtocolRegistry,
        enabled_protocols: tuple[str, ...],
    ) -> None:
        if not enabled_protocols:
            raise ValueError("At least one protocol plugin must be enabled")

        self.registry = registry
        self.enabled_protocols = enabled_protocols
        for protocol in enabled_protocols:
            registry.get(protocol)

    @property
    def subscription_topics(self) -> tuple[str, ...]:
        """Return unique websocket topics required by enabled plugins."""
        topics = (
            topic
            for protocol in self.enabled_protocols
            for topic in self.registry.get(protocol).subscription_topics
        )
        return tuple(dict.fromkeys(topics))

    @property
    def subscriptions(self) -> tuple[LogSubscription, ...]:
        """Return websocket filters supplied by enabled plugins."""
        return tuple(
            subscription
            for protocol in self.enabled_protocols
            for subscription in self.registry.get(protocol).subscriptions
        )

    @property
    def event_types(self) -> tuple[str, ...]:
        """Return event types exposed by enabled plugins."""
        return tuple(
            event_type
            for protocol in self.enabled_protocols
            for event_type in self.registry.get(protocol).event_types
        )

    def dispatch(
        self,
        raw_event: dict[str, Any],
        block_timestamp: str | None,
        *,
        chain: str,
    ) -> EventEnvelope:
        """Identify the protocol plugin and normalize one raw event."""
        plugin = None
        try:
            plugin = self.registry.identify(raw_event, self.enabled_protocols)
            return plugin.normalize(
                raw_event,
                block_timestamp,
                chain=chain,
            )
        except UnsupportedEventError:
            raise
        except Exception as error:
            signature = (raw_event.get("topics") or ["missing"])[0]
            protocol = plugin.protocol if plugin is not None else "unknown"
            raise EventParsingError(
                f"Failed to parse {protocol} event {signature}: {error}"
            ) from error
