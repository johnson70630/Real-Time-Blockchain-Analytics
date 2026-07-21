import logging
import signal
from collections import Counter
from types import FrameType

from config.settings import (
    CHAIN,
    ENABLED_PROTOCOLS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    get_alchemy_websocket_url,
)
from producer.alchemy_client import AlchemyClient
from producer.dispatcher import EventDispatcher, EventParsingError
from producer.kafka_producer import KafkaEventProducer
from producer.registry import UnsupportedEventError, protocol_registry

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
    """Receive blockchain logs, normalize them, and publish them to Kafka."""
    client = AlchemyClient(get_alchemy_websocket_url())
    kafka = KafkaEventProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    dispatcher = EventDispatcher(protocol_registry, ENABLED_PROTOCOLS)
    processed_counts: Counter[str] = Counter()

    try:
        logger.info("Starting blockchain event producer")
        logger.info("Chain: %s", CHAIN)
        logger.info("Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
        logger.info("Kafka topic: %s", KAFKA_TOPIC)
        logger.info("Enabled protocols: %s", ", ".join(ENABLED_PROTOCOLS))
        logger.info(
            "Enabled event types: %s",
            ", ".join(dispatcher.event_types),
        )

        subscription_request_ids = {
            client.subscribe_logs(
                subscription.topics,
                subscription.addresses,
            )
            for subscription in dispatcher.subscriptions
        }

        while True:
            message = client.receive()

            if "error" in message:
                raise RuntimeError(f"Alchemy subscription error: {message['error']}")

            if (
                "result" in message
                and message.get("id") in subscription_request_ids
            ):
                logger.info("Subscription confirmed: %s", message["result"])
                continue

            if message.get("method") == "eth_subscription":
                log = message["params"]["result"]
                try:
                    block_number = log.get("blockNumber")
                    if block_number is None:
                        raise EventParsingError(
                            "Event is missing required blockNumber"
                        )
                    block_timestamp = client.get_block_timestamp(block_number)
                    event = dispatcher.dispatch(
                        log,
                        block_timestamp,
                        chain=CHAIN,
                    )
                except UnsupportedEventError as error:
                    logger.warning("Ignoring unsupported event: %s", error)
                    continue
                except EventParsingError as error:
                    logger.error("Ignoring malformed event: %s", error)
                    continue

                kafka.send(event.to_dict())
                processed_counts[event.event_type] += 1
                event_count = processed_counts[event.event_type]
                if event_count == 1 or event_count % 100 == 0:
                    logger.info(
                        "Processed %s events: %s",
                        event.event_type,
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
