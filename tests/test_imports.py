import importlib


PRODUCER_MODULES = [
    "producer.alchemy_client",
    "producer.config",
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