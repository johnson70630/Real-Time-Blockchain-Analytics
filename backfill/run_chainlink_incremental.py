"""Capture one finite Chainlink forward increment without committing state."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from backfill.chainlink import (
    ChainlinkHistoricalRpcClient,
    extract_historical_rounds,
    split_proxy_round_id,
)
from backfill.chainlink_storage import (
    ChainlinkBootstrapBatchResult,
    ChainlinkBootstrapBatchStore,
)
from config.logging import configure_logging
from config.settings import (
    CHAIN,
    CHAINLINK_FEEDS_CONFIG,
    DATA_LAKE,
    STORAGE,
    get_alchemy_rpc_url,
    get_chainlink_confirmation_depth,
    get_chainlink_forward_max_rounds_per_run,
)
from market_data.feeds import load_chainlink_feeds
from orchestration.chainlink_forward import (
    ChainlinkForwardPlan,
    freeze_chainlink_snapshot,
    plan_chainlink_forward,
)
from orchestration.chainlink_bootstrap import (
    calculate_feed_coverage,
    plan_missing_batches,
)
from orchestration.watermark import (
    CHAINLINK_FORWARD_WATERMARK,
    WatermarkIdentity,
    WatermarkState,
    chainlink_feed_watermark,
)
from spark.session import create_spark_session
from warehouse.watermarks import SnowflakeWatermarkStore, connect_watermark_store

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ChainlinkForwardCapture:
    """Non-secret summary for source data persisted before state commit."""

    run_id: str
    plan: ChainlinkForwardPlan
    current_states: dict[WatermarkIdentity, WatermarkState]
    observations_by_feed: dict[str, int]
    rpc_requests: int
    batches: tuple[ChainlinkBootstrapBatchResult, ...]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id")
    return parser.parse_args()


def _load_states(
    store: SnowflakeWatermarkStore,
    feed_addresses: tuple[str, ...],
) -> dict[WatermarkIdentity, WatermarkState]:
    identities = (
        CHAINLINK_FORWARD_WATERMARK,
        *(chainlink_feed_watermark(address) for address in feed_addresses),
    )
    states = {identity: store.get(identity) for identity in identities}
    missing = [identity.source_name for identity, state in states.items() if state is None]
    if missing:
        raise RuntimeError(
            "Chainlink production watermarks must be initialized explicitly: "
            + ", ".join(missing)
        )
    return {identity: state for identity, state in states.items() if state is not None}


def run_forward_capture(run_id: str | None = None) -> ChainlinkForwardCapture:
    """Persist only rounds after durable cursors; never commit those cursors."""
    if not STORAGE.is_s3:
        raise ValueError("Chainlink forward processing requires STORAGE_MODE=s3")
    feeds = load_chainlink_feeds(CHAINLINK_FEEDS_CONFIG, expected_chain=CHAIN)
    rpc = ChainlinkHistoricalRpcClient(get_alchemy_rpc_url())
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the configured Chainlink RPC")

    connection = connect_watermark_store()
    spark = None
    try:
        states = _load_states(
            SnowflakeWatermarkStore(connection),
            tuple(feed.feed_address for feed in feeds),
        )
        snapshot = freeze_chainlink_snapshot(
            rpc,
            feeds,
            get_chainlink_confirmation_depth(),
        )
        plan = plan_chainlink_forward(
            rpc,
            snapshot,
            states,
            max_rounds=get_chainlink_forward_max_rounds_per_run(),
        )
        capture_run_id = run_id or (
            f"chainlink-forward-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-"
            f"{uuid4().hex[:8]}"
        )
        if plan.is_noop:
            return ChainlinkForwardCapture(
                capture_run_id,
                plan,
                states,
                {},
                0,
                (),
            )

        decimals = {
            feed.feed_address.lower(): rpc.get_feed_decimals(feed.feed_address)
            for feed in feeds
        }
        if any(value < 0 or value > 18 for value in decimals.values()):
            raise ValueError("Configured Chainlink feed decimals must be 0 through 18")
        persisted_batches: list[ChainlinkBootstrapBatchResult] = []
        rpc_requests = 0
        counts = {feed.feed_address.lower(): 0 for feed in feeds}
        if plan.ranges:
            spark = create_spark_session("CaptureChainlinkForwardIncrement")
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            spark.sparkContext.setLogLevel("WARN")
            store = ChainlinkBootstrapBatchStore(
                spark,
                bronze_root=DATA_LAKE.get("bronze_chainlink_forward"),
                staging_root=DATA_LAKE.get("state_chainlink_forward_staging"),
                quarantine_root=DATA_LAKE.get("quarantine_chainlink_forward"),
                source_topic="chainlink_forward_rpc",
            )
            existing = store.load_observations()
            coverage = calculate_feed_coverage(plan.ranges, existing)
            round_batches = tuple(
                item.round_range
                for item in plan_missing_batches(coverage, batch_size=500)
            )
            for item in coverage:
                counts[item.feed.feed_address.lower()] = len(item.persisted_ids)
            for round_batch in round_batches:
                result = extract_historical_rounds(
                    rpc,
                    ranges=(round_batch,),
                    feed_decimals=decimals,
                    snapshot=plan.snapshot.block,
                    max_rpc_requests=(
                        split_proxy_round_id(round_batch.end_round_id)[1]
                        - split_proxy_round_id(round_batch.start_round_id)[1]
                        + 1
                    ),
                    rpc_batch_size=10,
                    max_workers=1,
                )
                rpc_requests += result.rpc_requests
                if not result.complete or result.issues:
                    store.write_issues(
                        feed_address=round_batch.feed.feed_address,
                        phase_id=round_batch.phase_id,
                        start_round_id=round_batch.start_round_id,
                        end_round_id=round_batch.end_round_id,
                        issues=result.issues,
                    )
                    raise RuntimeError(
                        "Chainlink round retrieval was incomplete; production state "
                        "is unchanged"
                    )
                persisted = store.write_and_validate(
                    feed_address=round_batch.feed.feed_address,
                    phase_id=round_batch.phase_id,
                    start_round_id=round_batch.start_round_id,
                    end_round_id=round_batch.end_round_id,
                    observations=result.observations,
                    processed_at=datetime.now(UTC),
                )
                persisted_batches.append(persisted)
                counts[round_batch.feed.feed_address.lower()] += len(
                    result.observations
                )
            if sum(counts.values()) != plan.round_count:
                raise RuntimeError(
                    "Chainlink forward persisted coverage does not match its plan"
                )
        return ChainlinkForwardCapture(
            capture_run_id,
            plan,
            states,
            counts,
            rpc_requests,
            tuple(persisted_batches),
        )
    finally:
        if spark is not None:
            spark.stop()
        connection.close()


def main() -> None:
    configure_logging()
    summary = run_forward_capture(_parse_args().run_id)
    LOGGER.info(
        "Chainlink forward capture run_id=%s head=%s lag=%s safe=%s "
        "committed=%s rounds=%s batches=%s replayed_batches=%s "
        "rpc_requests=%s observations=%s quarantine=%s",
        summary.run_id,
        summary.plan.snapshot.observed_head,
        summary.plan.snapshot.confirmation_depth,
        summary.plan.snapshot.block.number,
        summary.plan.committed_block,
        summary.plan.round_count,
        len(summary.batches),
        sum(batch.replayed for batch in summary.batches),
        summary.rpc_requests,
        sum(summary.observations_by_feed.values()),
        0,
    )
    for target in summary.plan.snapshot.targets:
        LOGGER.info(
            "Chainlink feed=%s address=%s target_round=%s updated_at=%s "
            "observations=%s",
            target.feed.name,
            target.feed.feed_address.lower(),
            target.round_id,
            target.round.updated_at,
            summary.observations_by_feed.get(target.feed.feed_address.lower(), 0),
        )


if __name__ == "__main__":
    main()
