import importlib

import pytest


PRODUCER_MODULES = [
    "config.metadata",
    "config.settings",
    "config.versions",
    "producer.alchemy_client",
    "producer.config",
    "producer.event_handlers",
    "producer.kafka_producer",
    "producer.parser",
]


def test_import_producer_modules() -> None:
    """Verify core producer modules can be imported in CI."""
    for module in PRODUCER_MODULES:
        importlib.import_module(module)


def test_swap_topic_signature_exists() -> None:
    """Verify the Uniswap V3 Swap event topic is formatted correctly."""
    from producer.parser import UNISWAP_V3_SWAP_TOPIC

    assert UNISWAP_V3_SWAP_TOPIC.startswith("0x")
    assert len(UNISWAP_V3_SWAP_TOPIC) == 66


def test_alchemy_websocket_url_is_validated_lazily(monkeypatch) -> None:
    """Require the Alchemy URL only when the producer requests it."""
    from config.settings import get_alchemy_websocket_url

    monkeypatch.delenv("ALCHEMY_WEBSOCKET_URL", raising=False)

    with pytest.raises(
        ValueError,
        match="Missing required environment variable: ALCHEMY_WEBSOCKET_URL",
    ):
        get_alchemy_websocket_url()
