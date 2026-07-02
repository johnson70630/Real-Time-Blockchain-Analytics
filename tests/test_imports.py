import importlib


def test_import_producer_modules():
    modules = [
        "producer.alchemy_client",
        "producer.config",
        "producer.kafka_producer",
        "producer.parser",
    ]

    for module in modules:
        importlib.import_module(module)


def test_swap_topic_signature_exists():
    from producer.parser import UNISWAP_V3_SWAP_TOPIC

    assert UNISWAP_V3_SWAP_TOPIC.startswith("0x")
    assert len(UNISWAP_V3_SWAP_TOPIC) == 66