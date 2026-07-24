import pytest

from market_data import run_poller


def test_configuration_is_validated_before_kafka_initialization(
    monkeypatch,
) -> None:
    kafka_created = False

    def fail_configuration() -> str:
        raise ValueError("missing RPC configuration")

    def create_kafka(_servers, _topic):
        nonlocal kafka_created
        kafka_created = True

    monkeypatch.setattr(run_poller, "get_alchemy_rpc_url", fail_configuration)
    monkeypatch.setattr(run_poller, "KafkaEventProducer", create_kafka)

    with pytest.raises(ValueError, match="missing RPC"):
        run_poller.run_poller()

    assert kafka_created is False
