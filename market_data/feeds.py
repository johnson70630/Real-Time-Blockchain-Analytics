"""External Chainlink feed definitions used by the market-data poller."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from web3 import Web3


@dataclass(frozen=True, slots=True)
class ChainlinkFeed:
    """A configured Chainlink feed proxy and its human-readable pair."""

    name: str
    feed_address: str
    base_asset: str
    quote_asset: str
    chain: str


def _required_string(item: dict[str, Any], field: str, index: int) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Chainlink feed {index} requires a non-empty '{field}'"
        )
    return value.strip()


def _feed_address(item: dict[str, Any], index: int) -> str:
    address = _required_string(item, "address", index)
    if not Web3.is_address(address):
        raise ValueError(f"Chainlink feed {index} has an invalid address")
    return Web3.to_checksum_address(address)


def parse_chainlink_feeds(
    raw_config: str,
    *,
    expected_chain: str | None = None,
) -> tuple[ChainlinkFeed, ...]:
    """Parse and validate Chainlink feed definitions without network access."""
    try:
        configured = json.loads(raw_config)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"Chainlink feed configuration must be valid JSON: {error.msg}"
        ) from error

    if not isinstance(configured, dict):
        raise ValueError(
            "Chainlink feed configuration must be a JSON object"
        )
    feed_entries = configured.get("feeds")
    if not isinstance(feed_entries, list) or not feed_entries:
        raise ValueError("Chainlink feed configuration requires a non-empty 'feeds' array")

    feeds: list[ChainlinkFeed] = []
    identities: set[tuple[str, str]] = set()
    for index, item in enumerate(feed_entries):
        if not isinstance(item, dict):
            raise ValueError(f"Chainlink feed {index} must be a JSON object")

        base_asset = _required_string(item, "base_asset", index).upper()
        quote_asset = _required_string(item, "quote_asset", index).upper()
        feed = ChainlinkFeed(
            name=_required_string(item, "name", index).upper(),
            feed_address=_feed_address(item, index),
            base_asset=base_asset,
            quote_asset=quote_asset,
            chain=_required_string(item, "chain", index).lower(),
        )
        expected_name = f"{base_asset}/{quote_asset}"
        if feed.name != expected_name:
            raise ValueError(
                f"Chainlink feed {index} name must match its asset pair "
                f"'{expected_name}'"
            )
        if expected_chain is not None and feed.chain != expected_chain:
            raise ValueError(
                f"Chainlink feed {index} expects chain '{feed.chain}', "
                f"but the service is configured for '{expected_chain}'"
            )

        identity = (feed.chain, feed.feed_address.lower())
        if identity in identities:
            raise ValueError(
                f"Duplicate Chainlink feed for {feed.chain}: "
                f"{feed.feed_address}"
            )
        identities.add(identity)
        feeds.append(feed)

    return tuple(feeds)


def load_chainlink_feeds(
    config_path: Path,
    *,
    expected_chain: str | None = None,
) -> tuple[ChainlinkFeed, ...]:
    """Load feed definitions lazily from an external JSON file."""
    if not config_path.is_file():
        raise ValueError(
            f"Chainlink feed configuration file not found: {config_path}"
        )
    return parse_chainlink_feeds(
        config_path.read_text(encoding="utf-8"),
        expected_chain=expected_chain,
    )
