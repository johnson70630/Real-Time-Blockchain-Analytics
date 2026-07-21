from typing import Any

import pytest

from producer.dispatcher import EventDispatcher
from producer.models import EventEnvelope
from producer.protocols.base import ProtocolPlugin
from producer.protocols.aave_v3 import AaveV3Plugin
from producer.protocols.uniswap_v3 import UniswapV3Plugin
from producer.protocols.uniswap_v3.handlers import SwapEventHandler
from producer.registry import ProtocolRegistry, protocol_registry


class StubPlugin(ProtocolPlugin):
    protocol = "stub"
    subscription_topics = ("0xstub",)
    event_types = ("sample",)

    def can_handle(self, raw_event: dict[str, Any]) -> bool:
        return raw_event.get("topics", [None])[0] == "0xstub"

    def normalize(
        self,
        raw_event: dict[str, Any],
        block_timestamp: str | None,
        *,
        chain: str,
    ) -> EventEnvelope:
        return EventEnvelope.create(
            protocol=self.protocol,
            chain=chain,
            event_type="sample",
            block_number=int(raw_event["blockNumber"], 16),
            transaction_hash=raw_event["transactionHash"],
            log_index=int(raw_event["logIndex"], 16),
            block_timestamp=block_timestamp,
            ingested_at="2026-07-20T00:00:01+00:00",
            payload={"value": 1},
        )


def _stub_event() -> dict[str, Any]:
    return {
        "topics": ["0xstub"],
        "blockNumber": "0x10",
        "transactionHash": "0xabc",
        "logIndex": "0x2",
    }


def test_default_registry_contains_registered_protocols() -> None:
    uniswap_plugin = protocol_registry.get("uniswap_v3")
    aave_plugin = protocol_registry.get("aave_v3")

    assert isinstance(uniswap_plugin, ProtocolPlugin)
    assert isinstance(uniswap_plugin, UniswapV3Plugin)
    assert uniswap_plugin.event_types == ("swap", "mint", "burn")
    assert SwapEventHandler.topic in uniswap_plugin.subscription_topics
    assert isinstance(aave_plugin, AaveV3Plugin)
    assert aave_plugin.event_types == ("borrow", "repay", "liquidation")


def test_registry_lookup_and_duplicate_protection() -> None:
    registry = ProtocolRegistry()
    plugin = StubPlugin()
    registry.register(plugin)

    assert registry.get("stub") is plugin
    with pytest.raises(ValueError, match="already registered"):
        registry.register(StubPlugin())
    with pytest.raises(ValueError, match="Unknown protocol plugin"):
        registry.get("missing")


def test_dispatcher_identifies_plugin_and_routes_event() -> None:
    registry = ProtocolRegistry()
    registry.register(StubPlugin())
    dispatcher = EventDispatcher(registry, ("stub",))

    event = dispatcher.dispatch(
        _stub_event(),
        "2026-07-20T00:00:00+00:00",
        chain="arbitrum",
    )

    assert dispatcher.subscription_topics == ("0xstub",)
    assert event.protocol == "stub"
    assert event.event_type == "sample"
    assert event.payload == {"value": 1}


def test_common_event_envelope_is_json_ready() -> None:
    registry = ProtocolRegistry()
    registry.register(StubPlugin())
    event = EventDispatcher(registry, ("stub",)).dispatch(
        _stub_event(),
        "2026-07-20T00:00:00+00:00",
        chain="arbitrum",
    )
    message = event.to_dict()

    assert {
        "protocol",
        "chain",
        "event_type",
        "block_number",
        "transaction_hash",
        "log_index",
        "block_timestamp",
        "ingested_at",
        "payload",
    } <= set(message)
    assert message["producer_version"]
    assert message["schema_version"]
