import json

from producer.alchemy_client import AlchemyClient
from producer.config import ALCHEMY_WEBSOCKET_URL, CHAIN
from producer.parser import UNISWAP_V3_SWAP_TOPIC, parse_swap_log
from producer.kafka_producer import KafkaEventProducer
from producer.config import (
    ALCHEMY_WEBSOCKET_URL,
    CHAIN,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)


def main() -> None:
    if not ALCHEMY_WEBSOCKET_URL:
        raise ValueError("ALCHEMY_WEBSOCKET_URL is missing from .env")

    client = AlchemyClient(ALCHEMY_WEBSOCKET_URL)
    kafka = KafkaEventProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    print(f"Starting producer for chain={CHAIN}")
    print(f"Subscribing to Uniswap V3 Swap topic={UNISWAP_V3_SWAP_TOPIC}")
    print(f"Publishing events to Kafka topic={KAFKA_TOPIC}")

    client.subscribe_logs([UNISWAP_V3_SWAP_TOPIC])

    while True:
        message = client.receive()

        if "error" in message:
            raise RuntimeError(f"Alchemy subscription error: {message['error']}")

        if "result" in message and message.get("id") == 1:
            print(f"Subscription confirmed: {message['result']}")
            continue

        if message.get("method") == "eth_subscription":
            log = message["params"]["result"]
            event = parse_swap_log(log, chain=CHAIN)
            kafka.send(event)
            print(json.dumps(event, sort_keys=True))


if __name__ == "__main__":
    main()