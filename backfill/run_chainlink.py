"""CLI for bounded, resumable historical Chainlink price extraction."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime

from backfill.chainlink import (
    ChainlinkHistoricalRpcClient,
    discover_round_ranges,
    extract_historical_rounds,
)
from backfill.chainlink_storage import (
    ChainlinkBackfillState,
    ChainlinkBackfillStore,
)
from config.logging import configure_logging
from config.settings import (
    CHAIN,
    CHAINLINK_FEEDS_CONFIG,
    DATA_LAKE,
    get_alchemy_rpc_url,
)
from market_data.feeds import load_chainlink_feeds
from spark.session import create_spark_session

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse bounded extraction controls without reading secrets."""
    parser = argparse.ArgumentParser(
        description="Backfill phase-aware Chainlink proxy rounds"
    )
    parser.add_argument("--from-block", type=int, required=True)
    parser.add_argument("--to-block", type=int, required=True)
    parser.add_argument("--max-rpc-requests", type=int, default=1_000)
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--rpc-batch-size", type=int, default=10)
    parser.add_argument(
        "--min-request-interval-seconds",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--overlap-rounds",
        type=int,
        default=0,
        help="Safely re-read this many completed rounds per feed phase",
    )
    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> None:
    if args.from_block < 0 or args.to_block < args.from_block:
        raise ValueError("Historical block bounds are invalid")
    if (
        args.max_rpc_requests <= 0
        or args.max_workers <= 0
        or args.rpc_batch_size <= 0
        or args.min_request_interval_seconds < 0
    ):
        raise ValueError("RPC and worker limits must be greater than zero")
    if args.overlap_rounds < 0:
        raise ValueError("Overlap rounds must not be negative")


def main() -> None:
    """Extract one bounded slice and durably advance its checkpoints."""
    configure_logging()
    args = parse_args()
    _validate_args(args)
    feeds = load_chainlink_feeds(CHAINLINK_FEEDS_CONFIG, expected_chain=CHAIN)
    rpc = ChainlinkHistoricalRpcClient(
        get_alchemy_rpc_url(),
        min_request_interval_seconds=args.min_request_interval_seconds,
    )
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the configured Alchemy RPC")

    spark = create_spark_session("BackfillChainlinkPrices")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    store = ChainlinkBackfillStore(
        spark,
        bronze_path=DATA_LAKE.get("bronze_chainlink_backfill"),
        state_path=DATA_LAKE.get("state_chainlink_backfill"),
        quarantine_path=DATA_LAKE.get("quarantine_chainlink_backfill"),
    )
    try:
        existing = store.load_observations()
        prior_states = store.load_state()
        prior_issues = store.load_issues()
        for state in prior_states:
            if (state.from_block, state.to_block) != (
                args.from_block,
                args.to_block,
            ):
                raise ValueError(
                    "Configured block window does not match persisted "
                    "Chainlink backfill state"
                )

        ranges = tuple(
            round_range
            for feed in feeds
            for round_range in discover_round_ranges(
                rpc,
                feed,
                start_block=args.from_block,
                end_block=args.to_block,
            )
        )
        range_by_key = {round_range.key: round_range for round_range in ranges}
        if len(range_by_key) != len(ranges):
            raise ValueError("Discovered duplicate Chainlink feed-phase ranges")
        for state in prior_states:
            current = range_by_key.get(state.key)
            if current is None or (
                current.start_round_id,
                current.end_round_id,
            ) != (state.start_round_id, state.end_round_id):
                raise ValueError(
                    f"Chainlink round boundaries changed for {state.key}"
                )

        prior_next = {state.key: state.next_round_id for state in prior_states}
        retryable_rounds: dict[tuple[str, int], list[int]] = {}
        for issue in prior_issues:
            if issue.retryable:
                phase_id = int(issue.round_id) >> 64
                retryable_rounds.setdefault(
                    (issue.feed_address.lower(), phase_id), []
                ).append(int(issue.round_id))
        resume_next = {
            key: max(
                range_by_key[key].start_round_id,
                min(
                    next_round_id - args.overlap_rounds,
                    min(retryable_rounds.get(key, [next_round_id])),
                ),
            )
            for key, next_round_id in prior_next.items()
        }
        decimals = {
            feed.feed_address.lower(): rpc.get_feed_decimals(feed.feed_address)
            for feed in feeds
        }
        if any(value < 0 or value > 18 for value in decimals.values()):
            raise ValueError("Configured Chainlink feed decimals must be between 0 and 18")
        snapshot = rpc.get_block_snapshot(args.to_block)
        request_count_before = rpc.request_count
        result = extract_historical_rounds(
            rpc,
            ranges=ranges,
            feed_decimals=decimals,
            snapshot=snapshot,
            max_rpc_requests=args.max_rpc_requests,
            next_round_ids=resume_next,
            existing=existing,
            max_workers=args.max_workers,
            rpc_batch_size=args.rpc_batch_size,
        )
        processed_at = datetime.now(UTC)
        if result.observations != existing:
            store.write_observations(result.observations, processed_at)

        states = tuple(
            ChainlinkBackfillState(
                feed_address=round_range.feed.feed_address.lower(),
                phase_id=round_range.phase_id,
                start_round_id=round_range.start_round_id,
                end_round_id=round_range.end_round_id,
                next_round_id=result.next_round_ids.get(
                    round_range.key,
                    prior_next.get(
                        round_range.key,
                        round_range.start_round_id,
                    ),
                ),
                from_block=args.from_block,
                to_block=args.to_block,
                updated_at=processed_at,
            )
            for round_range in ranges
        )
        store.write_state(states)
        issues_by_round = {
            (issue.feed_address.lower(), issue.round_id): issue
            for issue in (*prior_issues, *result.issues)
        }
        successful_rounds = {
            (observation.feed_address.lower(), observation.round_id)
            for observation in result.observations
        }
        for key in successful_rounds:
            issues_by_round.pop(key, None)
        canonical_issues = tuple(
            issues_by_round[key] for key in sorted(issues_by_round)
        )
        store.write_issues(canonical_issues)
        logger.info(
            "Chainlink backfill window=%s-%s attempted=%s new=%s total=%s "
            "quarantined=%s extraction_requests=%s total_requests=%s complete=%s",
            args.from_block,
            args.to_block,
            result.rounds_attempted,
            len(result.observations) - len(existing),
            len(result.observations),
            len(canonical_issues),
            result.rpc_requests,
            rpc.request_count,
            all(
                state.next_round_id > state.end_round_id
                for state in states
            ),
        )
        logger.info(
            "Chainlink historical Bronze=%s state=%s quarantine=%s setup_requests=%s",
            DATA_LAKE.get("bronze_chainlink_backfill"),
            DATA_LAKE.get("state_chainlink_backfill"),
            DATA_LAKE.get("quarantine_chainlink_backfill"),
            request_count_before,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
