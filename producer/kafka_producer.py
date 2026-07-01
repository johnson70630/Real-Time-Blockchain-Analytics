import json
from typing import Any

from confluent_kafka import Producer


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "blockchain-event-producer",
            }
        )

    def send(self, event: dict[str, Any]) -> None:
        key = event.get("transaction_hash", "")

        self.producer.produce(
            topic=self.topic,
            key=key,
            value=json.dumps(event),
        )
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()