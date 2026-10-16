"""Run the resumable Aave project-boundary source bootstrap."""

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
    get_aave_project_collection_start_block,
    get_aave_v3_pool_address,
)
from orchestration.aave_forward import (
    CompletedBlockRange,
    calculate_safe_head,
    highest_contiguous_block,
)
from orchestration.watermark import (
    AAVE_FORWARD_WATERMARK,
    AAVE_LEGACY_BOOTSTRAP_RANGES,
)
from producer.models import EventEnvelope
from spark.session import create_spark_session
from warehouse.completed_ranges import SnowflakeCompletedRangeStore
from warehouse.watermarks import SnowflakeWatermarkStore, connect_watermark_store

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BootstrapSummary:
    """Non-secret source-bootstrap metrics."""

    run_id: str
    observed_head: int
    safe_end_block: int
    logical_ranges: int
    completed_ranges: int
    range_attempts: int
    provider_failures: int
    adaptive_splits: int
    event_counts: Counter[str]
    quarantine_count: int
    highest_contiguous_block: int


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-ranges",
        type=int,
        default=20,
        help="logical parents persisted together before ledger commits",
    )
    parser.add_argument(
        "--safe-end",
        type=int,
        help="freeze an explicitly preflighted safe end",
    )
    return parser.parse_args()


def _persist_completed_batch(
    *,
    storage: AaveForwardBatchStore,
    ledger: SnowflakeCompletedRangeStore,
    run_id: str,
    ranges: tuple[RpcBlockRange, ...],
    events: tuple[EventEnvelope, ...],
    issues: tuple[BackfillIssue, ...],
    processed_at: datetime,
) -> None:
    """Persist and validate source output before appending ledger evidence."""
    if not ranges:
        return
    canonical = merge_events((), events)
    storage.write_and_validate(
        start_block=ranges[0].start_block,
        end_block=ranges[-1].end_block,
        events=canonical,
        issues=issues,
        processed_at=processed_at,
    )
    ledger.record_ranges(
        AAVE_LEGACY_BOOTSTRAP_RANGES,
        tuple(
            CompletedBlockRange(
                block_range.start_block,
                block_range.end_block,
                run_id,
                processed_at,
            )
            for block_range in ranges
        ),
    )


def run_bootstrap(*, batch_ranges: int, safe_end: int | None) -> BootstrapSummary:
    """Process the frozen project interval and return operational metrics."""
    if batch_ranges <= 0:
        raise ValueError("batch_ranges must be greater than zero")
    if not STORAGE.is_s3:
        raise ValueError("Aave forward bootstrap requires STORAGE_MODE=s3")
    pool_address = get_aave_v3_pool_address()
    if not Web3.is_address(pool_address):
        raise ValueError("AAVE_V3_POOL_ADDRESS must be a valid Ethereum address")

    rpc = AaveV3RpcClient(get_aave_backfill_rpc_url())
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the Aave backfill RPC")
    observed_head = rpc.get_latest_block_number()
    confirmation_depth = get_aave_confirmation_depth()
    calculated_safe_end = calculate_safe_head(
        observed_head,
        confirmation_depth,
    )
    safe_end_block = calculated_safe_end if safe_end is None else safe_end
    if safe_end_block > calculated_safe_end:
        raise ValueError("safe_end exceeds the current confirmed source head")

    project_start = get_aave_project_collection_start_block()
    chunk_size = get_aave_bootstrap_chunk_size()
    policy = AdaptiveRangePolicy(
        get_aave_bootstrap_min_range_size(),
        get_aave_bootstrap_max_range_size(),
        get_aave_bootstrap_max_range_attempts(),
    )
    if chunk_size > policy.maximum_range_size:
        raise ValueError("Bootstrap chunk size exceeds adaptive maximum")

    run_id = f"aave-bootstrap-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    snowflake = connect_watermark_store()
    spark = create_spark_session("BootstrapAaveForward")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    try:
        ledger = SnowflakeCompletedRangeStore(snowflake)
        if SnowflakeWatermarkStore(snowflake).get(AAVE_FORWARD_WATERMARK):
            raise RuntimeError("aave_forward must remain uninitialized")
        persisted_ranges = ledger.list_ranges(AAVE_LEGACY_BOOTSTRAP_RANGES)
        contiguous = highest_contiguous_block(project_start, persisted_ranges)
        next_block = contiguous + 1
        if next_block > safe_end_block:
            return BootstrapSummary(
                run_id,
                observed_head,
                safe_end_block,
                0,
                0,
                0,
                0,
                0,
                Counter(),
                0,
                contiguous,
            )

        store = AaveForwardBatchStore(
            spark,
            bronze_path=DATA_LAKE.get("bronze_aave_forward"),
            quarantine_path=DATA_LAKE.get("quarantine_aave_forward"),
        )
        logical_ranges = 0
        completed_count = 0
        range_attempts = 0
        provider_failures = 0
        adaptive_splits = 0
        event_counts: Counter[str] = Counter()
        quarantine_count = 0
        while next_block <= safe_end_block:
            batch_leaves: list[RpcBlockRange] = []
            batch_events = ()
            batch_issues: list[BackfillIssue] = []
            processed_at = datetime.now(UTC)
            for _ in range(batch_ranges):
                if next_block > safe_end_block:
                    break
                parent_end = min(
                    safe_end_block,
                    next_block + chunk_size - 1,
                )
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
                batch_leaves.extend(fetched.completed_ranges)
                batch_events = merge_events(batch_events, decoded.events)
                batch_issues.extend(decoded.issues)
                logical_ranges += 1
                range_attempts += fetched.range_attempts
                adaptive_splits += fetched.split_count
                provider_failures += (
                    fetched.range_attempts - len(fetched.completed_ranges)
                )
                event_counts.update(
                    event.event_type for event in decoded.events
                )
                quarantine_count += len(decoded.issues)
                next_block = parent_end + 1

            _persist_completed_batch(
                storage=store,
                ledger=ledger,
                run_id=run_id,
                ranges=tuple(batch_leaves),
                events=batch_events,
                issues=tuple(batch_issues),
                processed_at=processed_at,
            )
            persisted_ranges = ledger.list_ranges(AAVE_LEGACY_BOOTSTRAP_RANGES)
            contiguous = highest_contiguous_block(
                project_start,
                persisted_ranges,
            )
            completed_count += len(batch_leaves)
            LOGGER.info(
                "Aave bootstrap progress logical=%s completed=%s events=%s "
                "quarantine=%s highest_contiguous=%s remaining_blocks=%s",
                logical_ranges,
                completed_count,
                sum(event_counts.values()),
                quarantine_count,
                contiguous,
                max(0, safe_end_block - contiguous),
            )
            if contiguous < next_block - 1:
                raise RuntimeError("Completed-range ledger contains a source gap")

        return BootstrapSummary(
            run_id,
            observed_head,
            safe_end_block,
            logical_ranges,
            completed_count,
            range_attempts,
            provider_failures,
            adaptive_splits,
            event_counts,
            quarantine_count,
            contiguous,
        )
    finally:
        spark.stop()
        snowflake.close()


def main() -> None:
    configure_logging()
    args = _parse_args()
    summary = run_bootstrap(
        batch_ranges=args.batch_ranges,
        safe_end=args.safe_end,
    )
    LOGGER.info(
        "Aave bootstrap complete run_id=%s head=%s safe_end=%s logical=%s "
        "completed=%s attempts=%s failures=%s splits=%s borrow=%s repay=%s "
        "liquidation=%s quarantine=%s highest_contiguous=%s",
        summary.run_id,
        summary.observed_head,
        summary.safe_end_block,
        summary.logical_ranges,
        summary.completed_ranges,
        summary.range_attempts,
        summary.provider_failures,
        summary.adaptive_splits,
        summary.event_counts["borrow"],
        summary.event_counts["repay"],
        summary.event_counts["liquidation"],
        summary.quarantine_count,
        summary.highest_contiguous_block,
    )


if __name__ == "__main__":
    main()
