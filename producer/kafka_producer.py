import json
from typing import Any

from confluent_kafka import Producer


class KafkaEventProducer:
    """Kafka producer wrapper for blockchain event messages."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "blockchain-event-producer",
            }
        )

    def send(self, event: dict[str, Any]) -> None:
        """Publish a blockchain event to Kafka as JSON."""
        key = event.get("transaction_hash", "")

        self.producer.produce(
            topic=self.topic,
            key=key,
            value=json.dumps(event),
        )
        self.producer.poll(0)

    def flush(self) -> None:
        """Flush buffered Kafka messages."""
        self.producer.flush()

    def __enter__(self) -> "KafkaEventProducer":
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback) -> None:
        self.flush()
