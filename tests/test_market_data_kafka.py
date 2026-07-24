import json

from producer import kafka_producer


class FakeConfluentProducer:
    def __init__(self, config) -> None:
        self.config = config
        self.records = []
        self.poll_calls = []

    def produce(self, **record) -> None:
        self.records.append(record)

    def poll(self, timeout) -> None:
        self.poll_calls.append(timeout)

    def flush(self) -> None:
        pass


def test_market_data_uses_observation_id_as_kafka_key(monkeypatch) -> None:
    monkeypatch.setattr(kafka_producer, "Producer", FakeConfluentProducer)
    publisher = kafka_producer.KafkaEventProducer(
        "kafka:9092",
        "events",
        client_id="market-data-poller",
    )
    message = {
        "observation_id": "arbitrum:0xfeed:123",
        "protocol": "chainlink",
    }

    publisher.send(message)

    assert publisher.producer.config["client.id"] == "market-data-poller"
    assert publisher.producer.records == [
        {
            "topic": "events",
            "key": "arbitrum:0xfeed:123",
            "value": json.dumps(message),
        }
    ]
