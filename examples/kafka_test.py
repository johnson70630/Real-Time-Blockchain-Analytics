"""Publish one message to an isolated Kafka connectivity-test topic."""

import logging
import os

from config.logging import configure_logging
from config.settings import KAFKA_BOOTSTRAP_SERVERS
from producer.kafka_producer import KafkaEventProducer

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    topic = os.getenv("KAFKA_TEST_TOPIC", "pipeline_connectivity_test")
    event = {
        "transaction_hash": "connectivity-test",
        "message": "Kafka connectivity test",
    }

    with KafkaEventProducer(KAFKA_BOOTSTRAP_SERVERS, topic) as producer:
        producer.send(event)

    logger.info("Published connectivity message to Kafka topic: %s", topic)


if __name__ == "__main__":
    main()
