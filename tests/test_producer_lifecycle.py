import pytest

from producer import run_producer


class ConfigurationError(RuntimeError):
    pass


def test_protocol_configuration_is_validated_before_external_clients(
    monkeypatch,
) -> None:
    external_client_created = False

    class InvalidDispatcher:
        def __init__(self, _registry, _protocols) -> None:
            pass

        @property
        def subscriptions(self):
            raise ConfigurationError("invalid protocol configuration")

    def create_external_client(_url):
        nonlocal external_client_created
        external_client_created = True

    monkeypatch.setattr(run_producer, "EventDispatcher", InvalidDispatcher)
    monkeypatch.setattr(run_producer, "AlchemyClient", create_external_client)

    with pytest.raises(ConfigurationError, match="invalid protocol"):
        run_producer.run_producer()

    assert external_client_created is False


def test_alchemy_client_closes_when_kafka_initialization_fails(monkeypatch) -> None:
    alchemy_closed = False

    class ValidDispatcher:
        subscriptions = ()
        event_types = ()

        def __init__(self, _registry, _protocols) -> None:
            pass

    class FakeAlchemyClient:
        def __init__(self, _url) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
            nonlocal alchemy_closed
            alchemy_closed = True

    def fail_kafka_initialization(_servers, _topic):
        raise RuntimeError("Kafka initialization failed")

    monkeypatch.setattr(run_producer, "EventDispatcher", ValidDispatcher)
    monkeypatch.setattr(run_producer, "AlchemyClient", FakeAlchemyClient)
    monkeypatch.setattr(
        run_producer,
        "KafkaEventProducer",
        fail_kafka_initialization,
    )
    monkeypatch.setattr(
        run_producer,
        "get_alchemy_websocket_url",
        lambda: "wss://example.invalid",
    )

    with pytest.raises(RuntimeError, match="Kafka initialization failed"):
        run_producer.run_producer()

    assert alchemy_closed is True
