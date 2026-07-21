"""Uniswap V3 protocol plugin registration unit."""

from typing import Any

from producer.models import EventEnvelope
from producer.protocols.base import ProtocolPlugin
from producer.protocols.uniswap_v3.constants import PROTOCOL_NAME
from producer.protocols.uniswap_v3.handlers import (
    EVENT_HANDLERS,
    EVENT_HANDLERS_BY_TOPIC,
    SUPPORTED_EVENT_TOPICS,
    get_event_handler,
)


class UniswapV3Plugin(ProtocolPlugin):
    """Normalize supported Uniswap V3 pool events."""

    @property
    def protocol(self) -> str:
        return PROTOCOL_NAME

    @property
    def subscription_topics(self) -> tuple[str, ...]:
        return SUPPORTED_EVENT_TOPICS

    @property
    def event_types(self) -> tuple[str, ...]:
        return tuple(handler.event_type for handler in EVENT_HANDLERS)

    def can_handle(self, raw_event: dict[str, Any]) -> bool:
        topics = raw_event.get("topics") or []
        if not topics:
            return False

        topic = topics[0]
        if not isinstance(topic, str) and hasattr(topic, "hex"):
            topic = topic.hex()
            topic = topic if topic.startswith("0x") else f"0x{topic}"
        if not isinstance(topic, str):
            return False
        return topic.lower() in EVENT_HANDLERS_BY_TOPIC

    def normalize(
        self,
        raw_event: dict[str, Any],
        block_timestamp: str | None,
        *,
        chain: str,
    ) -> EventEnvelope:
        handler = get_event_handler(raw_event)
        return handler.build_event(
            raw_event,
            block_timestamp,
            chain=chain,
            protocol=self.protocol,
        )
