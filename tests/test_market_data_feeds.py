import json
from pathlib import Path

import pytest
from web3 import Web3

from config.settings import (
    get_alchemy_rpc_url,
    get_chainlink_poll_interval_seconds,
)
from market_data.feeds import load_chainlink_feeds, parse_chainlink_feeds

ETH_FEED = Web3.to_checksum_address("0x" + "10" * 20)
USDC_FEED = Web3.to_checksum_address("0x" + "20" * 20)


def _config() -> str:
    return json.dumps(
        {
            "feeds": [
                {
                    "name": "ETH/USD",
                    "address": ETH_FEED,
                    "base_asset": "eth",
                    "quote_asset": "usd",
                    "chain": "arbitrum",
                },
                {
                    "name": "USDC/USD",
                    "address": USDC_FEED,
                    "base_asset": "USDC",
                    "quote_asset": "USD",
                    "chain": "arbitrum",
                },
            ]
        }
    )


def test_feed_configuration_loads_multiple_validated_feeds(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "feeds.json"
    config_path.write_text(_config(), encoding="utf-8")

    feeds = load_chainlink_feeds(config_path, expected_chain="arbitrum")

    assert len(feeds) == 2
    assert feeds[0].name == "ETH/USD"
    assert feeds[0].feed_address == ETH_FEED
    assert feeds[0].base_asset == "ETH"
    assert feeds[1].base_asset == "USDC"


@pytest.mark.parametrize(
    ("raw_config", "message"),
    [
        ("not-json", "valid JSON"),
        ("[]", "JSON object"),
        (
            json.dumps(
                {
                    "feeds": [
                        {
                            "name": "ETH/USD",
                            "address": "",
                            "base_asset": "ETH",
                            "quote_asset": "USD",
                            "chain": "arbitrum",
                        }
                    ]
                }
            ),
            "non-empty 'address'",
        ),
        (
            json.dumps(
                {
                    "feeds": [
                        {
                            "name": "ETH/USD",
                            "address": "invalid",
                            "base_asset": "ETH",
                            "quote_asset": "USD",
                            "chain": "arbitrum",
                        }
                    ]
                }
            ),
            "invalid address",
        ),
    ],
)
def test_invalid_feed_configuration_fails_clearly(
    raw_config: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        parse_chainlink_feeds(raw_config)


def test_checked_in_catalog_contains_supported_official_feeds() -> None:
    from config.settings import CHAINLINK_FEEDS_CONFIG

    feeds = load_chainlink_feeds(
        CHAINLINK_FEEDS_CONFIG,
        expected_chain="arbitrum",
    )

    assert tuple(feed.name for feed in feeds) == (
        "ETH/USD",
        "USDC/USD",
        "USDT/USD",
        "WBTC/USD",
    )


def test_chain_mismatch_and_missing_file_fail_clearly(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="file not found"):
        load_chainlink_feeds(tmp_path / "missing.json")
    with pytest.raises(ValueError, match="configured for 'ethereum'"):
        parse_chainlink_feeds(_config(), expected_chain="ethereum")


def test_rpc_url_and_poll_interval_are_validated_lazily(monkeypatch) -> None:
    monkeypatch.delenv("ALCHEMY_RPC_URL", raising=False)
    with pytest.raises(
        ValueError,
        match="Missing required environment variable: ALCHEMY_RPC_URL",
    ):
        get_alchemy_rpc_url()

    monkeypatch.setenv("CHAINLINK_POLL_INTERVAL_SECONDS", "2.5")
    assert get_chainlink_poll_interval_seconds() == 2.5
    monkeypatch.setenv("CHAINLINK_POLL_INTERVAL_SECONDS", "0")
    with pytest.raises(ValueError, match="greater than zero"):
        get_chainlink_poll_interval_seconds()
