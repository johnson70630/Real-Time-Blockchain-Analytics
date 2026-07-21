"""Aave V3 protocol plugin and subscription configuration."""

from typing import Any

from web3 import Web3

from config.settings import get_aave_v3_pool_address
from producer.models import EventEnvelope
from producer.protocols.aave_v3.constants import PROTOCOL_NAME
from producer.protocols.aave_v3.handlers import (
    EVENT_HANDLERS,
    EVENT_HANDLERS_BY_TOPIC,
    SUPPORTED_EVENT_TOPICS,
    get_event_handler,
)
from producer.protocols.base import LogSubscription, ProtocolPlugin
from producer.protocols.evm import first_log_topic


class AaveV3Plugin(ProtocolPlugin):
    """Normalize supported events emitted by a configured Aave V3 Pool."""

    @property
    def protocol(self) -> str:
        return PROTOCOL_NAME

    @property
    def subscription_topics(self) -> tuple[str, ...]:
        return SUPPORTED_EVENT_TOPICS

    @property
    def subscriptions(self) -> tuple[LogSubscription, ...]:
        pool_address = get_aave_v3_pool_address()
        if not Web3.is_address(pool_address):
            raise ValueError(
                "AAVE_V3_POOL_ADDRESS must be a valid Ethereum address"
            )
        return (
            LogSubscription(
                topics=self.subscription_topics,
                addresses=(Web3.to_checksum_address(pool_address),),
            ),
        )

    @property
    def event_types(self) -> tuple[str, ...]:
        return tuple(handler.event_type for handler in EVENT_HANDLERS)

    def can_handle(self, raw_event: dict[str, Any]) -> bool:
        topic = first_log_topic(raw_event)
        if topic is None:
            return False
        return topic.lower() in EVENT_HANDLERS_BY_TOPIC

    def normalize(
        self,
        raw_event: dict[str, Any],
        block_timestamp: str | None,
        *,
        chain: str,
    ) -> EventEnvelope:
        return get_event_handler(raw_event).build_event(
            raw_event,
            block_timestamp,
            chain=chain,
        )
