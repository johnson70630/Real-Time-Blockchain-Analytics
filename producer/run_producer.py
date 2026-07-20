import logging
import signal
from collections import Counter
from types import FrameType

from config.settings import (
    CHAIN,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PROTOCOL,
    get_alchemy_websocket_url,
)
from producer.alchemy_client import AlchemyClient
from producer.event_handlers import (
    EVENT_HANDLERS,
    SUPPORTED_EVENT_TOPICS,
    get_event_handler,
)
from producer.kafka_producer import KafkaEventProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def handle_shutdown_signal(_signum: int, _frame: FrameType | None) -> None:
    """Convert SIGTERM into the producer's existing graceful shutdown path."""
    logger.info("Producer shutdown signal received")
    raise KeyboardInterrupt


def run_producer() -> None:
    """Stream supported Uniswap V3 logs from Alchemy and publish them to Kafka."""
    client = AlchemyClient(get_alchemy_websocket_url())
    kafka = KafkaEventProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    processed_counts: Counter[str] = Counter()

    try:
        logger.info("Starting blockchain event producer")
        logger.info("Chain: %s", CHAIN)
        logger.info("Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
        logger.info("Kafka topic: %s", KAFKA_TOPIC)
        logger.info("Protocol: %s", PROTOCOL)
        logger.info(
            "Enabled event types: %s",
            ", ".join(handler.event_type for handler in EVENT_HANDLERS),
        )

        client.subscribe_logs(SUPPORTED_EVENT_TOPICS)

        while True:
            message = client.receive()

            if "error" in message:
                raise RuntimeError(f"Alchemy subscription error: {message['error']}")

            if "result" in message and message.get("id") == 1:
                logger.info("Subscription confirmed: %s", message["result"])
                continue

            if message.get("method") == "eth_subscription":
                log = message["params"]["result"]
                handler = get_event_handler(log)
                block_timestamp = client.get_block_timestamp(log["blockNumber"])
                event = handler.build_event(
                    log,
                    block_timestamp,
                    chain=CHAIN,
                    protocol=PROTOCOL,
                )

                kafka.send(event)
                processed_counts[handler.event_type] += 1
                event_count = processed_counts[handler.event_type]
                if event_count == 1 or event_count % 100 == 0:
                    logger.info(
                        "Processed %s events: %s",
                        handler.event_type,
                        event_count,
                    )

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    finally:
        kafka.flush()
        client.close()
        logger.info("Producer shutdown complete")


def main() -> None:
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    run_producer()


if __name__ == "__main__":
    main()
