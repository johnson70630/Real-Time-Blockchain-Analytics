"""Complete the immutable Chainlink bootstrap in durable feed/phase batches."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime
from time import monotonic

from backfill.chainlink import ChainlinkHistoricalRpcClient, extract_historical_rounds
from backfill.chainlink_storage import (
    ChainlinkBackfillStore,
    ChainlinkBootstrapBatchStore,
)
from config.logging import configure_logging
from config.settings import (
    CHAIN,
    CHAINLINK_FEEDS_CONFIG,
    DATA_LAKE,
    STORAGE,
    get_alchemy_rpc_url,
)
from market_data.feeds import load_chainlink_feeds
from orchestration.chainlink_bootstrap import (
    CHAINLINK_BOOTSTRAP_ROUNDS,
    CHAINLINK_BOOTSTRAP_SAFE_BLOCK,
    calculate_feed_coverage,
    frozen_feed_ranges,
    plan_missing_batches,
)
from spark.session import create_spark_session

LOGGER = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-batches", type=int)
    parser.add_argument("--rpc-batch-size", type=int, default=10)
    parser.add_argument("--min-request-interval-seconds", type=float, default=1.0)
    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> None:
    if args.batch_size <= 0 or args.rpc_batch_size <= 0:
        raise ValueError("Batch sizes must be greater than zero")
    if args.max_batches is not None and args.max_batches <= 0:
        raise ValueError("max_batches must be greater than zero")
    if args.min_request_interval_seconds < 0:
        raise ValueError("RPC request interval must not be negative")


def _log_coverage(coverage) -> None:
    for item in coverage:
        LOGGER.info(
            "Chainlink bootstrap coverage feed=%s phases=%s required=%s "
            "persisted=%s missing=%s duplicates=%s",
            item.feed.name,
            ",".join(str(phase) for phase in item.phases),
            len(item.required_ids),
            len(item.persisted_ids),
            len(item.missing_ids),
            item.duplicate_count,
        )


def main() -> None:
    configure_logging()
    args = _parse_args()
    _validate_args(args)
    if not STORAGE.is_s3:
        raise ValueError("Chainlink bootstrap batches require STORAGE_MODE=s3")
    feeds = load_chainlink_feeds(CHAINLINK_FEEDS_CONFIG, expected_chain=CHAIN)
    ranges = frozen_feed_ranges(feeds)
    rpc = ChainlinkHistoricalRpcClient(
        get_alchemy_rpc_url(),
        min_request_interval_seconds=args.min_request_interval_seconds,
    )
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the configured Chainlink RPC")
    snapshot = rpc.get_block_snapshot(CHAINLINK_BOOTSTRAP_SAFE_BLOCK)
    decimals = {}
    for feed in feeds:
        address = feed.feed_address.lower()
        target = rpc.get_round_at_block(
            feed.feed_address,
            CHAINLINK_BOOTSTRAP_SAFE_BLOCK,
        )
        if target.round_id != CHAINLINK_BOOTSTRAP_ROUNDS[address][1]:
            raise RuntimeError(f"Frozen target changed for {feed.name}")
        decimals[address] = rpc.get_feed_decimals(feed.feed_address)
        LOGGER.info(
            "Frozen Chainlink target feed=%s address=%s block=%s round=%s "
            "answer_raw=%s decimals=%s updated_at=%s",
            feed.name,
            address,
            snapshot.number,
            target.round_id,
            target.answer_raw,
            decimals[address],
            target.updated_at,
        )

    spark = create_spark_session("BootstrapChainlinkDurableBatches")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    try:
        historical = ChainlinkBackfillStore(
            spark,
            bronze_path=DATA_LAKE.get("bronze_chainlink_backfill"),
            state_path=DATA_LAKE.get("state_chainlink_backfill"),
            quarantine_path=DATA_LAKE.get("quarantine_chainlink_backfill"),
        )
        batches = ChainlinkBootstrapBatchStore(
            spark,
            bronze_root=DATA_LAKE.get("bronze_chainlink_bootstrap"),
            staging_root=DATA_LAKE.get("state_chainlink_bootstrap_staging"),
            quarantine_root=DATA_LAKE.get("quarantine_chainlink_bootstrap"),
        )
        existing = (*historical.load_observations(), *batches.load_observations())
        coverage = calculate_feed_coverage(ranges, existing)
        _log_coverage(coverage)
        planned = plan_missing_batches(coverage, batch_size=args.batch_size)
        if args.max_batches is not None:
            planned = planned[: args.max_batches]
        LOGGER.info(
            "Chainlink bootstrap plan batch_size=%s batches=%s rounds=%s",
            args.batch_size,
            len(planned),
            sum(item.size for item in planned),
        )

        for index, batch in enumerate(planned, start=1):
            round_range = batch.round_range
            started = monotonic()
            result = extract_historical_rounds(
                rpc,
                ranges=(round_range,),
                feed_decimals=decimals,
                snapshot=snapshot,
                max_rpc_requests=batch.size,
                rpc_batch_size=args.rpc_batch_size,
                max_workers=1,
            )
            if result.issues or not result.complete:
                batches.write_issues(
                    feed_address=round_range.feed.feed_address,
                    phase_id=round_range.phase_id,
                    start_round_id=round_range.start_round_id,
                    end_round_id=round_range.end_round_id,
                    issues=result.issues,
                )
                raise RuntimeError(
                    "Chainlink bootstrap batch is incomplete and remains retryable"
                )
            persisted = batches.write_and_validate(
                feed_address=round_range.feed.feed_address,
                phase_id=round_range.phase_id,
                start_round_id=round_range.start_round_id,
                end_round_id=round_range.end_round_id,
                observations=result.observations,
                processed_at=datetime.now(UTC),
            )
            LOGGER.info(
                "Chainlink bootstrap batch=%s/%s feed=%s phase=%s rounds=%s "
                "duration_seconds=%.3f replayed=%s path=%s",
                index,
                len(planned),
                round_range.feed.name,
                round_range.phase_id,
                persisted.observation_count,
                monotonic() - started,
                persisted.replayed,
                persisted.bronze_path,
            )

        final_observations = (
            *historical.load_observations(),
            *batches.load_observations(),
        )
        final_coverage = calculate_feed_coverage(ranges, final_observations)
        _log_coverage(final_coverage)
        final_missing = set().union(
            *(item.missing_ids for item in final_coverage),
        )
        if not final_missing:
            legacy_issues = historical.load_issues()
            if legacy_issues:
                historical.write_issues(())
            LOGGER.info("Reconciled legacy Chainlink quarantine unresolved=0")
        LOGGER.info(
            "Chainlink bootstrap durable run complete missing=%s batches=%s",
            len(final_missing),
            len(batches.committed_batch_paths()),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
