import importlib

import pytest


PRODUCER_MODULES = [
    "config.metadata",
    "config.logging",
    "config.settings",
    "config.versions",
    "producer.alchemy_client",
    "producer.config",
    "producer.dispatcher",
    "producer.event_handlers",
    "producer.kafka_producer",
    "producer.models",
    "producer.protocols.evm",
    "producer.protocols.aave_v3",
    "producer.parser",
    "producer.protocols.uniswap_v3",
    "producer.registry",
    "spark.kafka_stream",
    "spark.parquet",
]

EXAMPLE_MODULES = [
    "examples.alchemy_blocks_example",
    "examples.kafka_test",
    "examples.read_kafka_stream_example",
    "examples.uniswap_swap_logs_example",
]


def test_import_producer_modules() -> None:
    """Verify core producer modules can be imported in CI."""
    for module in PRODUCER_MODULES:
        importlib.import_module(module)


def test_examples_import_without_external_connections() -> None:
    """Keep examples safe to inspect without starting live services."""
    for module in EXAMPLE_MODULES:
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
