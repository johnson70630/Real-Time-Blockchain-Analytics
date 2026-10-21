"""Bootstrap gap-free Uniswap pool-event coverage without initializing state."""

from __future__ import annotations

import argparse
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from backfill.aave_v3 import BackfillIssue, RpcBlockRange
from backfill.storage import UniswapRecoveryBatchStore
from backfill.uniswap_v3 import (
    UniswapSourceContract,
    UniswapV3RecoveryRpcClient,
    canonical_pool_contract,
    decode_recovery_logs,
    fetch_complete_range_adaptive,
    merge_events,
)
from config.logging import configure_logging
from config.settings import (
    CHAIN,
    DATA_LAKE,
    STORAGE,
    get_blockchain_recovery_rpc_url,
    get_uniswap_confirmation_depth,
    get_uniswap_project_collection_start_block,
    get_uniswap_recovery_address_batch_size,
    get_uniswap_recovery_chunk_size,
    get_uniswap_recovery_max_range_attempts,
    get_uniswap_recovery_min_range_size,
)
from orchestration.aave_forward import (
    CompletedBlockRange,
    calculate_safe_head,
    highest_contiguous_block,
)
from orchestration.watermark import UNISWAP_FORWARD_WATERMARK
from producer.models import EventEnvelope
from reference_data.storage import PoolMetadataStore
from spark.session import create_spark_session
from warehouse.completed_ranges import SnowflakeCompletedRangeStore
from warehouse.watermarks import SnowflakeWatermarkStore, connect_watermark_store

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BootstrapSummary:
    """Non-secret metrics for one resumable Uniswap bootstrap execution."""

    run_id: str
    observed_head: int
    safe_end_block: int
    pool_count: int
    contract_fingerprint: str
    logical_ranges: int
    completed_ranges: int
    query_requests: int
    adaptive_splits: int
    event_counts: Counter[str]
    quarantine_count: int
    highest_contiguous_block: int


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--safe-end", type=int)
    parser.add_argument("--batch-ranges", type=int, default=10)
    return parser.parse_args()


def _load_contract(spark) -> UniswapSourceContract:
    store = PoolMetadataStore(
        spark,
        pool_path=DATA_LAKE.get("reference_uniswap_v3_pools"),
        watermark_path=DATA_LAKE.get("state_uniswap_v3_pools"),
    )
    return canonical_pool_contract(store.load_records(), chain=CHAIN)


def _persist_batch(
    *,
    store: UniswapRecoveryBatchStore,
    ledger: SnowflakeCompletedRangeStore,
    run_id: str,
    ranges: tuple[RpcBlockRange, ...],
    events: tuple[EventEnvelope, ...],
    issues: tuple[BackfillIssue, ...],
    processed_at: datetime,
    contract: UniswapSourceContract,
) -> None:
    if not ranges:
        return
    canonical = merge_events((), events)
    store.write_and_validate(
        start_block=ranges[0].start_block,
        end_block=ranges[-1].end_block,
        events=canonical,
        issues=issues,
        processed_at=processed_at,
        contract_fingerprint=contract.fingerprint,
        pool_count=len(contract.pool_addresses),
        event_topic_count=len(contract.event_topics),
        deduplication_paths=(
            (DATA_LAKE.get("bronze_events"), True),
        ),
    )
    ledger.record_ranges(
        UNISWAP_FORWARD_WATERMARK,
        tuple(
            CompletedBlockRange(
                item.start_block,
                item.end_block,
                run_id,
                processed_at,
            )
            for item in ranges
        ),
    )


def run_bootstrap(
    *,
    safe_end: int | None,
    batch_ranges: int,
) -> BootstrapSummary:
    """Recover the frozen project interval while leaving the watermark absent."""
    if batch_ranges <= 0:
        raise ValueError("batch_ranges must be greater than zero")
    if not STORAGE.is_s3:
        raise ValueError("Uniswap recovery bootstrap requires STORAGE_MODE=s3")
    rpc = UniswapV3RecoveryRpcClient(get_blockchain_recovery_rpc_url())
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the recovery RPC")
    observed_head = rpc.get_latest_block_number()
    current_safe_end = calculate_safe_head(
        observed_head,
        get_uniswap_confirmation_depth(),
    )
    safe_end_block = current_safe_end if safe_end is None else safe_end
    if safe_end_block > current_safe_end:
        raise ValueError("safe_end exceeds the current confirmed source head")

    connection = connect_watermark_store()
    spark = create_spark_session("BootstrapUniswapRecovery")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    run_id = f"uniswap-bootstrap-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    try:
        watermark_store = SnowflakeWatermarkStore(connection)
        if watermark_store.get(UNISWAP_FORWARD_WATERMARK) is not None:
            raise RuntimeError("uniswap_forward must remain uninitialized")
        contract = _load_contract(spark)
        ledger = SnowflakeCompletedRangeStore(connection)
        completed = ledger.list_ranges(UNISWAP_FORWARD_WATERMARK)
        project_start = get_uniswap_project_collection_start_block()
        contiguous = highest_contiguous_block(project_start, completed)
        next_block = contiguous + 1
        store = UniswapRecoveryBatchStore(
            spark,
            bronze_path=DATA_LAKE.get("bronze_uniswap_recovery"),
            quarantine_path=DATA_LAKE.get("quarantine_uniswap_recovery"),
        )
        logical_ranges = 0
        completed_count = 0
        query_requests = 0
        split_count = 0
        event_counts: Counter[str] = Counter()
        quarantine_count = 0
        chunk_size = get_uniswap_recovery_chunk_size()
        while next_block <= safe_end_block:
            leaves: list[RpcBlockRange] = []
            events: tuple[EventEnvelope, ...] = ()
            issues: list[BackfillIssue] = []
            processed_at = datetime.now(UTC)
            for _ in range(batch_ranges):
                if next_block > safe_end_block:
                    break
                parent_end = min(safe_end_block, next_block + chunk_size - 1)
                fetched = fetch_complete_range_adaptive(
                    rpc,
                    contract=contract,
                    start_block=next_block,
                    end_block=parent_end,
                    address_batch_size=get_uniswap_recovery_address_batch_size(),
                    minimum_range_size=get_uniswap_recovery_min_range_size(),
                    max_range_attempts=get_uniswap_recovery_max_range_attempts(),
                )
                decoded, rejected = decode_recovery_logs(
                    rpc,
                    logs=fetched.logs,
                    contract=contract,
                    chain=CHAIN,
                    start_block=next_block,
                    end_block=parent_end,
                    processed_at=processed_at,
                )
                leaves.extend(fetched.completed_ranges)
                events = merge_events(events, decoded)
                issues.extend(rejected)
                logical_ranges += 1
                query_requests += fetched.query_requests
                split_count += fetched.split_count
                event_counts.update(event.event_type for event in decoded)
                quarantine_count += len(rejected)
                next_block = parent_end + 1
            _persist_batch(
                store=store,
                ledger=ledger,
                run_id=run_id,
                ranges=tuple(leaves),
                events=events,
                issues=tuple(issues),
                processed_at=processed_at,
                contract=contract,
            )
            completed_count += len(leaves)
            contiguous = highest_contiguous_block(
                project_start,
                ledger.list_ranges(UNISWAP_FORWARD_WATERMARK),
            )
            if contiguous < next_block - 1:
                raise RuntimeError("Uniswap completed-range ledger contains a gap")
            LOGGER.info(
                "Uniswap bootstrap progress ranges=%s queries=%s events=%s "
                "quarantine=%s highest_contiguous=%s remaining_blocks=%s",
                logical_ranges,
                query_requests,
                sum(event_counts.values()),
                quarantine_count,
                contiguous,
                max(0, safe_end_block - contiguous),
            )
        return BootstrapSummary(
            run_id,
            observed_head,
            safe_end_block,
            len(contract.pool_addresses),
            contract.fingerprint,
            logical_ranges,
            completed_count,
            query_requests,
            split_count,
            event_counts,
            quarantine_count,
            contiguous,
        )
    finally:
        spark.stop()
        connection.close()


def main() -> None:
    configure_logging()
    args = _parse_args()
    summary = run_bootstrap(
        safe_end=args.safe_end,
        batch_ranges=args.batch_ranges,
    )
    LOGGER.info(
        "Uniswap bootstrap complete run_id=%s head=%s safe_end=%s pools=%s "
        "contract=%s logical=%s completed=%s queries=%s splits=%s swap=%s "
        "mint=%s burn=%s quarantine=%s highest_contiguous=%s",
        summary.run_id,
        summary.observed_head,
        summary.safe_end_block,
        summary.pool_count,
        summary.contract_fingerprint,
        summary.logical_ranges,
        summary.completed_ranges,
        summary.query_requests,
        summary.adaptive_splits,
        summary.event_counts["swap"],
        summary.event_counts["mint"],
        summary.event_counts["burn"],
        summary.quarantine_count,
        summary.highest_contiguous_block,
    )


if __name__ == "__main__":
    main()
