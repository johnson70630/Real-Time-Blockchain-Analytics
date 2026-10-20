"""Tests for resumable, marker-backed Chainlink bootstrap batches."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from backfill.chainlink import encode_proxy_round_id
from backfill.chainlink_storage import ChainlinkBootstrapBatchStore
from market_data.feeds import ChainlinkFeed
from market_data.models import MarketDataObservation
from orchestration.chainlink_bootstrap import (
    calculate_feed_coverage,
    frozen_feed_ranges,
    plan_missing_batches,
)

NOW = datetime(2026, 10, 19, tzinfo=UTC)
FEEDS = (
    ChainlinkFeed(
        "ETH/USD",
        "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
        "ETH",
        "USD",
        "arbitrum",
    ),
    ChainlinkFeed(
        "USDC/USD",
        "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
        "USDC",
        "USD",
        "arbitrum",
    ),
    ChainlinkFeed(
        "USDT/USD",
        "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",
        "USDT",
        "USD",
        "arbitrum",
    ),
    ChainlinkFeed(
        "WBTC/USD",
        "0xd0C7101eACbB49F3deCcCc166d238410D6D46d57",
        "WBTC",
        "USD",
        "arbitrum",
    ),
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TestChainlinkBootstrap")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _observation(feed: ChainlinkFeed, round_id: int) -> MarketDataObservation:
    return MarketDataObservation.create(
        chain=feed.chain,
        feed_address=feed.feed_address,
        base_asset=feed.base_asset,
        quote_asset=feed.quote_asset,
        round_id=round_id,
        answer_raw=123_456_789,
        feed_decimals=8,
        feed_updated_at="2026-10-19T12:00:00+00:00",
        block_number=515_520_400,
        block_timestamp="2026-10-16T16:38:00+00:00",
        observed_at=NOW.isoformat(),
    )


def test_missing_planner_uses_persisted_identities_and_feed_phase_boundaries() -> None:
    ranges = frozen_feed_ranges(FEEDS)
    first = ranges[0]
    persisted = (_observation(first.feed, first.start_round_id),)
    coverage = calculate_feed_coverage(ranges, persisted)
    batches = plan_missing_batches(coverage, batch_size=500)

    eth = next(item for item in coverage if item.feed.name == "ETH/USD")
    assert len(eth.persisted_ids) == 1
    assert len(eth.missing_ids) == len(eth.required_ids) - 1
    assert all(
        batch.round_range.start_round_id >> 64
        == batch.round_range.end_round_id >> 64
        for batch in batches
    )
    assert all(batch.size <= 500 for batch in batches)


def test_successful_batches_survive_later_failure_and_retry(
    spark: SparkSession,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ChainlinkBootstrapBatchStore(
        spark,
        bronze_root=str(tmp_path / "bronze"),
        staging_root=str(tmp_path / "staging"),
        quarantine_root=str(tmp_path / "quarantine"),
    )
    feed = FEEDS[0]
    first_round = encode_proxy_round_id(2, 10)
    second_round = encode_proxy_round_id(2, 11)
    first = store.write_and_validate(
        feed_address=feed.feed_address,
        phase_id=2,
        start_round_id=first_round,
        end_round_id=first_round,
        observations=(_observation(feed, first_round),),
        processed_at=NOW,
    )
    assert not first.replayed
    loaded_first = store.load_committed_batch(
        feed_address=feed.feed_address,
        phase_id=2,
        start_round_id=first_round,
        end_round_id=first_round,
    )
    assert loaded_first is not None
    assert loaded_first.replayed
    assert loaded_first.observation_count == 1

    original_promote = store._promote
    monkeypatch.setattr(
        store,
        "_promote",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("S3 unavailable")),
    )
    with pytest.raises(RuntimeError, match="S3 unavailable"):
        store.write_and_validate(
            feed_address=feed.feed_address,
            phase_id=2,
            start_round_id=second_round,
            end_round_id=second_round,
            observations=(_observation(feed, second_round),),
            processed_at=NOW,
        )
    assert [item.round_id for item in store.load_observations()] == [
        str(first_round)
    ]

    monkeypatch.setattr(store, "_promote", original_promote)
    second = store.write_and_validate(
        feed_address=feed.feed_address,
        phase_id=2,
        start_round_id=second_round,
        end_round_id=second_round,
        observations=(_observation(feed, second_round),),
        processed_at=NOW,
    )
    replay = store.write_and_validate(
        feed_address=feed.feed_address,
        phase_id=2,
        start_round_id=second_round,
        end_round_id=second_round,
        observations=(_observation(feed, second_round),),
        processed_at=NOW,
    )
    observations = store.load_observations()
    assert not second.replayed
    assert replay.replayed
    assert len(observations) == 2
    assert len({item.observation_id for item in observations}) == 2
