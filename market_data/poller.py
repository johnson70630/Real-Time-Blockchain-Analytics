"""Polling workflow for Chainlink market-data observations."""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from market_data.feeds import ChainlinkFeed
from market_data.models import MarketDataObservation
from market_data.rpc import BlockSnapshot, ChainlinkRound

logger = logging.getLogger(__name__)


class ChainlinkRpc(Protocol):
    """RPC operations required by the poller."""

    def is_connected(self) -> bool: ...

    def get_latest_block(self) -> BlockSnapshot: ...

    def get_feed_decimals(self, feed_address: str) -> int: ...

    def get_latest_round(
        self,
        feed_address: str,
        block_number: int,
    ) -> ChainlinkRound: ...


class MessagePublisher(Protocol):
    """Kafka-compatible publisher used by the poller."""

    def send(self, message: dict[str, Any]) -> None: ...


@dataclass(frozen=True, slots=True)
class PollingStats:
    """Operational counts for one completed polling cycle."""

    block_number: int
    feeds_checked: int
    observations_published: int
    unchanged_observations: int
    failed_feeds: int


class ChainlinkMarketDataPoller:
    """Poll configured feeds and publish each new round exactly once per run."""

    def __init__(
        self,
        *,
        rpc: ChainlinkRpc,
        publisher: MessagePublisher,
        feeds: tuple[ChainlinkFeed, ...],
        poll_interval_seconds: float,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not feeds:
            raise ValueError("At least one Chainlink feed must be configured")
        if poll_interval_seconds <= 0:
            raise ValueError("Polling interval must be greater than zero")

        self.rpc = rpc
        self.publisher = publisher
        self.feeds = feeds
        self.poll_interval_seconds = poll_interval_seconds
        self.clock = clock or (lambda: datetime.now(UTC))
        self.sleeper = sleeper
        self.feed_decimals: dict[str, int] = {}
        self.published_rounds: dict[str, str] = {}
        self._initialized = False

    def initialize(self) -> None:
        """Validate RPC connectivity and cache feed decimals once."""
        if self._initialized:
            return
        if not self.rpc.is_connected():
            raise ConnectionError("Unable to connect to the configured Alchemy RPC")

        decimals: dict[str, int] = {}
        for feed in self.feeds:
            value = self.rpc.get_feed_decimals(feed.feed_address)
            if not 0 <= value <= 255:
                raise ValueError(
                    f"Invalid decimals for {feed.base_asset}/"
                    f"{feed.quote_asset}: {value}"
                )
            decimals[feed.feed_address.lower()] = value

        self.feed_decimals = decimals
        self._initialized = True
        logger.info("RPC connected; cached metadata for %s feeds", len(self.feeds))

    def poll_once(self) -> PollingStats:
        """Poll every feed against one pinned block and publish new rounds."""
        if not self._initialized:
            raise RuntimeError("Poller must be initialized before polling")

        block = self.rpc.get_latest_block()
        published = 0
        unchanged = 0
        failed = 0

        for feed in self.feeds:
            try:
                round_data = self.rpc.get_latest_round(
                    feed.feed_address,
                    block.number,
                )
                feed_key = feed.feed_address.lower()
                round_id = str(round_data.round_id)
                if self.published_rounds.get(feed_key) == round_id:
                    unchanged += 1
                    continue

                observed_at = self.clock().astimezone(UTC).isoformat()
                observation = MarketDataObservation.create(
                    chain=feed.chain,
                    feed_address=feed.feed_address,
                    base_asset=feed.base_asset,
                    quote_asset=feed.quote_asset,
                    round_id=round_data.round_id,
                    answer_raw=round_data.answer_raw,
                    feed_decimals=self.feed_decimals[feed_key],
                    feed_updated_at=round_data.updated_at,
                    observed_at=observed_at,
                    block_number=block.number,
                    block_timestamp=block.timestamp,
                )
                self.publisher.send(observation.to_dict())
                self.published_rounds[feed_key] = round_id
                published += 1
            except Exception:
                failed += 1
                logger.exception(
                    "Failed to poll Chainlink feed %s/%s",
                    feed.base_asset,
                    feed.quote_asset,
                )

        stats = PollingStats(
            block_number=block.number,
            feeds_checked=len(self.feeds),
            observations_published=published,
            unchanged_observations=unchanged,
            failed_feeds=failed,
        )
        logger.info(
            "Polling cycle block=%s checked=%s published=%s unchanged=%s "
            "failed=%s",
            stats.block_number,
            stats.feeds_checked,
            stats.observations_published,
            stats.unchanged_observations,
            stats.failed_feeds,
        )
        return stats

    def run(self, *, max_cycles: int | None = None) -> None:
        """Run continuously, retrying initialization and cycle failures."""
        completed_cycles = 0
        while max_cycles is None or completed_cycles < max_cycles:
            try:
                self.initialize()
                self.poll_once()
            except Exception:
                logger.exception(
                    "Market-data polling cycle failed; retrying in %s seconds",
                    self.poll_interval_seconds,
                )

            completed_cycles += 1
            if max_cycles is None or completed_cycles < max_cycles:
                self.sleeper(self.poll_interval_seconds)
