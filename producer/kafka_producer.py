import json
from typing import Any

from confluent_kafka import Producer


class KafkaEventProducer:
    """Kafka producer wrapper for normalized pipeline messages."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        *,
        client_id: str = "blockchain-event-producer",
    ) -> None:
        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
            }
        )

    def send(self, event: dict[str, Any]) -> None:
        """Publish a normalized message to Kafka as JSON."""
        key = event.get("transaction_hash") or event.get("observation_id", "")

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
