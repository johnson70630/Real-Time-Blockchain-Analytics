def test_import_producer_modules():
    import producer.alchemy_client
    import producer.config
    import producer.kafka_producer
    import producer.parser
    import producer.run_producer


def test_swap_topic_signature_exists():
    from producer.parser import UNISWAP_V3_SWAP_TOPIC

    assert UNISWAP_V3_SWAP_TOPIC.startswith("0x")
    assert len(UNISWAP_V3_SWAP_TOPIC) == 66