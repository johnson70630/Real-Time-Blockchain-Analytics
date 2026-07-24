"""Command-line entry point for the Chainlink market-data poller."""

import logging
import signal
from types import FrameType

from config.logging import configure_logging
from config.settings import (
    CHAIN,
    CHAINLINK_FEEDS_CONFIG,
    KAFKA_BOOTSTRAP_SERVERS,
    MARKET_DATA_KAFKA_TOPIC,
    get_alchemy_rpc_url,
    get_chainlink_poll_interval_seconds,
)
from market_data.feeds import load_chainlink_feeds
from market_data.poller import ChainlinkMarketDataPoller
from market_data.rpc import ChainlinkRpcClient
from producer.kafka_producer import KafkaEventProducer

logger = logging.getLogger(__name__)


def handle_shutdown_signal(_signum: int, _frame: FrameType | None) -> None:
    """Convert SIGTERM into the poller's graceful shutdown path."""
    logger.info("Market-data poller shutdown signal received")
    raise KeyboardInterrupt


def run_poller() -> None:
    """Validate configuration, create clients, and run the polling service."""
    rpc_url = get_alchemy_rpc_url()
    interval = get_chainlink_poll_interval_seconds()
    feeds = load_chainlink_feeds(
        CHAINLINK_FEEDS_CONFIG,
        expected_chain=CHAIN,
    )

    logger.info("Starting Chainlink market-data poller")
    logger.info("Feeds loaded: %s", len(feeds))
    logger.info("Chain: %s", CHAIN)
    logger.info("Polling interval: %s seconds", interval)
    logger.info("Kafka topic: %s", MARKET_DATA_KAFKA_TOPIC)

    with KafkaEventProducer(
        KAFKA_BOOTSTRAP_SERVERS,
        MARKET_DATA_KAFKA_TOPIC,
        client_id="market-data-poller",
    ) as kafka:
        poller = ChainlinkMarketDataPoller(
            rpc=ChainlinkRpcClient(rpc_url),
            publisher=kafka,
            feeds=feeds,
            poll_interval_seconds=interval,
        )
        try:
            poller.run()
        except KeyboardInterrupt:
            logger.info("Market-data poller stopped by user")

    logger.info("Market-data poller shutdown complete")


def main() -> None:
    configure_logging()
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    run_poller()


if __name__ == "__main__":
    main()
