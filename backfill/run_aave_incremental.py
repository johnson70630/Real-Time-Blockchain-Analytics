"""Capture one finite Aave forward increment without committing its watermark."""

from __future__ import annotations

import argparse
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from web3 import Web3

from backfill.aave_v3 import (
    AdaptiveRangePolicy,
    AaveV3RpcClient,
    BackfillIssue,
    RpcBlockRange,
    decode_fetched_logs,
    fetch_event_logs_adaptive,
    merge_events,
)
from backfill.storage import AaveForwardBatchStore
from config.logging import configure_logging
from config.settings import (
    CHAIN,
    DATA_LAKE,
    STORAGE,
    get_aave_backfill_rpc_url,
    get_aave_bootstrap_chunk_size,
    get_aave_bootstrap_max_range_attempts,
    get_aave_bootstrap_max_range_size,
    get_aave_bootstrap_min_range_size,
    get_aave_confirmation_depth,
    get_aave_forward_max_blocks_per_run,
    get_aave_v3_pool_address,
)
from orchestration.aave_forward import (
    CompletedBlockRange,
    calculate_safe_head,
    plan_forward_increment,
)
from orchestration.watermark import AAVE_FORWARD_WATERMARK
from producer.models import EventEnvelope
from spark.session import create_spark_session
from warehouse.completed_ranges import SnowflakeCompletedRangeStore
from warehouse.watermarks import SnowflakeWatermarkStore, connect_watermark_store

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ForwardCaptureSummary:
    """Non-secret metrics for a source capture whose watermark is uncommitted."""

    run_id: str
    observed_head: int
    safe_head: int
    committed_block: int
    start_block: int | None
    end_block: int | None
    logical_ranges: int
    completed_ranges: int
    range_attempts: int
    adaptive_splits: int
    event_counts: Counter[str]
    quarantine_count: int

    @property
    def is_noop(self) -> bool:
        return self.start_block is None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--safe-end",
        type=int,
        help="optional frozen safe head; must not exceed the current safe head",
    )
    parser.add_argument(
        "--run-id",
        help="non-secret operational run identifier",
    )
    return parser.parse_args()


def _fetch_increment(
    rpc: AaveV3RpcClient,
    *,
    pool_address: str,
    start_block: int,
    end_block: int,
    processed_at: datetime,
) -> tuple[
    tuple[RpcBlockRange, ...],
    tuple[EventEnvelope, ...],
    tuple[BackfillIssue, ...],
    int,
    int,
    int,
]:
    chunk_size = get_aave_bootstrap_chunk_size()
    policy = AdaptiveRangePolicy(
        get_aave_bootstrap_min_range_size(),
        get_aave_bootstrap_max_range_size(),
        get_aave_bootstrap_max_range_attempts(),
    )
    if chunk_size > policy.maximum_range_size:
        raise ValueError("Forward chunk size exceeds adaptive maximum")

    logical_ranges = 0
    attempts = 0
    splits = 0
    leaves: list[RpcBlockRange] = []
    events: tuple[EventEnvelope, ...] = ()
    issues: list[BackfillIssue] = []
    next_block = start_block
    while next_block <= end_block:
        parent_end = min(end_block, next_block + chunk_size - 1)
        fetched = fetch_event_logs_adaptive(
            rpc,
            pool_address=pool_address,
            start_block=next_block,
            end_block=parent_end,
            policy=policy,
        )
        decoded = decode_fetched_logs(
            rpc,
            logs=fetched.logs,
            pool_address=pool_address,
            chain=CHAIN,
            start_block=next_block,
            end_block=parent_end,
            processed_at=processed_at,
        )
        leaves.extend(fetched.completed_ranges)
        events = merge_events(events, decoded.events)
        issues.extend(decoded.issues)
        logical_ranges += 1
        attempts += fetched.range_attempts
        splits += fetched.split_count
        next_block = parent_end + 1
    return tuple(leaves), events, tuple(issues), logical_ranges, attempts, splits


def run_forward_capture(
    *,
    frozen_safe_end: int | None = None,
    run_id: str | None = None,
) -> ForwardCaptureSummary:
    """Persist exactly the next forward interval; never advance the watermark."""
    if not STORAGE.is_s3:
        raise ValueError("Aave forward processing requires STORAGE_MODE=s3")
    pool_address = get_aave_v3_pool_address()
    if not Web3.is_address(pool_address):
        raise ValueError("AAVE_V3_POOL_ADDRESS must be a valid Ethereum address")

    rpc = AaveV3RpcClient(get_aave_backfill_rpc_url())
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the Aave backfill RPC")
    observed_head = rpc.get_latest_block_number()
    current_safe_head = calculate_safe_head(
        observed_head,
        get_aave_confirmation_depth(),
    )
    safe_head = current_safe_head if frozen_safe_end is None else frozen_safe_end
    if safe_head > current_safe_head:
        raise ValueError("safe_end exceeds the current confirmed source head")

    connection = connect_watermark_store()
    spark = None
    try:
        watermark = SnowflakeWatermarkStore(connection).get(
            AAVE_FORWARD_WATERMARK
        )
        if watermark is None:
            raise RuntimeError("aave_forward must be initialized explicitly")
        plan = plan_forward_increment(
            committed_block=watermark.committed_value,
            safe_head=safe_head,
            max_blocks=get_aave_forward_max_blocks_per_run(),
        )
        capture_run_id = run_id or (
            f"aave-forward-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-"
            f"{uuid4().hex[:8]}"
        )
        if plan.is_noop:
            return ForwardCaptureSummary(
                capture_run_id,
                observed_head,
                safe_head,
                watermark.committed_value,
                None,
                None,
                0,
                0,
                0,
                0,
                Counter(),
                0,
            )

        assert plan.start_block is not None and plan.end_block is not None
        processed_at = datetime.now(UTC)
        leaves, events, issues, logical_ranges, attempts, splits = _fetch_increment(
            rpc,
            pool_address=pool_address,
            start_block=plan.start_block,
            end_block=plan.end_block,
            processed_at=processed_at,
        )
        spark = create_spark_session("CaptureAaveForwardIncrement")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.sparkContext.setLogLevel("WARN")
        AaveForwardBatchStore(
            spark,
            bronze_path=DATA_LAKE.get("bronze_aave_forward"),
            quarantine_path=DATA_LAKE.get("quarantine_aave_forward"),
        ).write_and_validate(
            start_block=plan.start_block,
            end_block=plan.end_block,
            events=events,
            issues=issues,
            processed_at=processed_at,
        )
        SnowflakeCompletedRangeStore(connection).record_ranges(
            AAVE_FORWARD_WATERMARK,
            tuple(
                CompletedBlockRange(
                    leaf.start_block,
                    leaf.end_block,
                    capture_run_id,
                    processed_at,
                )
                for leaf in leaves
            ),
        )
        return ForwardCaptureSummary(
            capture_run_id,
            observed_head,
            safe_head,
            watermark.committed_value,
            plan.start_block,
            plan.end_block,
            logical_ranges,
            len(leaves),
            attempts,
            splits,
            Counter(event.event_type for event in events),
            len(issues),
        )
    finally:
        if spark is not None:
            spark.stop()
        connection.close()


def main() -> None:
    configure_logging()
    args = _parse_args()
    summary = run_forward_capture(
        frozen_safe_end=args.safe_end,
        run_id=args.run_id,
    )
    LOGGER.info(
        "Aave forward source complete run_id=%s head=%s safe_head=%s "
        "committed=%s range=%s-%s logical=%s completed=%s attempts=%s "
        "splits=%s borrow=%s repay=%s liquidation=%s quarantine=%s",
        summary.run_id,
        summary.observed_head,
        summary.safe_head,
        summary.committed_block,
        summary.start_block,
        summary.end_block,
        summary.logical_ranges,
        summary.completed_ranges,
        summary.range_attempts,
        summary.adaptive_splits,
        summary.event_counts["borrow"],
        summary.event_counts["repay"],
        summary.event_counts["liquidation"],
        summary.quarantine_count,
    )


if __name__ == "__main__":
    main()
