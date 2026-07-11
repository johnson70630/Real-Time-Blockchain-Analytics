import json
import logging

from producer.alchemy_client import AlchemyClient
from producer.config import (
    CHAIN,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PROTOCOL,
    get_alchemy_websocket_url,
)
from producer.kafka_producer import KafkaEventProducer
from producer.parser import UNISWAP_V3_SWAP_TOPIC, parse_swap_log

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def run_producer() -> None:
    """Stream Uniswap V3 swap logs from Alchemy and publish them to Kafka."""
    client = AlchemyClient(get_alchemy_websocket_url())
    kafka = KafkaEventProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    logger.info("Starting blockchain event producer")
    logger.info("Chain: %s", CHAIN)
    logger.info("Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Kafka topic: %s", KAFKA_TOPIC)
    logger.info("Alchemy subscription topic: %s", UNISWAP_V3_SWAP_TOPIC)
    logger.info("Protocol: %s", PROTOCOL)

    client.subscribe_logs([UNISWAP_V3_SWAP_TOPIC])

    try:
        while True:
            message = client.receive()

            if "error" in message:
                raise RuntimeError(f"Alchemy subscription error: {message['error']}")

            if "result" in message and message.get("id") == 1:
                logger.info("Subscription confirmed: %s", message["result"])
                continue

            if message.get("method") == "eth_subscription":
                log = message["params"]["result"]
                event = parse_swap_log(
                    log,
                    chain=CHAIN,
                    protocol=PROTOCOL,
                )

                kafka.send(event)
                logger.info("Published event: %s", json.dumps(event, sort_keys=True))

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    finally:
        kafka.flush()
        client.close()
        logger.info("Producer shutdown complete")


def main() -> None:
    run_producer()


if __name__ == "__main__":
    main()