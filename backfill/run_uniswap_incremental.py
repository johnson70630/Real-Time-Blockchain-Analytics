"""Capture one finite Uniswap recovery increment without committing state."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from backfill.storage import UniswapRecoveryBatchStore
from backfill.uniswap_v3 import (
    UniswapV3RecoveryRpcClient,
    canonical_pool_contract,
    decode_recovery_logs,
    fetch_complete_range_adaptive,
    merge_events,
)
from config.settings import (
    CHAIN,
    DATA_LAKE,
    STORAGE,
    get_blockchain_recovery_rpc_url,
    get_uniswap_confirmation_depth,
    get_uniswap_forward_max_blocks_per_run,
    get_uniswap_recovery_address_batch_size,
    get_uniswap_recovery_chunk_size,
    get_uniswap_recovery_max_range_attempts,
    get_uniswap_recovery_min_range_size,
)
from orchestration.aave_forward import CompletedBlockRange, calculate_safe_head
from orchestration.uniswap_recovery import (
    UniswapRecoveryPlan,
    assert_source_contract_matches,
    plan_recovery_increment,
)
from orchestration.watermark import UNISWAP_FORWARD_WATERMARK
from reference_data.storage import PoolMetadataStore
from spark.session import create_spark_session
from warehouse.completed_ranges import SnowflakeCompletedRangeStore
from warehouse.watermarks import SnowflakeWatermarkStore, connect_watermark_store


@dataclass(frozen=True, slots=True)
class ForwardCaptureSummary:
    """Non-secret metrics for one persisted, uncommitted recovery increment."""

    run_id: str
    plan: UniswapRecoveryPlan
    observed_head: int
    pool_count: int
    contract_fingerprint: str
    logical_ranges: int
    completed_ranges: int
    query_requests: int
    adaptive_splits: int
    event_counts: Counter[str]
    quarantine_count: int


def run_forward_capture(
    run_id: str | None = None,
    *,
    safe_head_override: int | None = None,
) -> ForwardCaptureSummary:
    """Persist exactly the next confirmed interval and leave commit to orchestration."""
    if not STORAGE.is_s3:
        raise ValueError("Uniswap recovery requires STORAGE_MODE=s3")
    rpc = UniswapV3RecoveryRpcClient(get_blockchain_recovery_rpc_url())
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the recovery RPC")
    observed_head = rpc.get_latest_block_number()
    calculated_safe_head = calculate_safe_head(
        observed_head,
        get_uniswap_confirmation_depth(),
    )
    if safe_head_override is not None and safe_head_override > calculated_safe_head:
        raise ValueError("Safe-head override exceeds the currently confirmed head")
    safe_head = (
        calculated_safe_head if safe_head_override is None else safe_head_override
    )
    connection = connect_watermark_store()
    spark = None
    capture_run_id = run_id or (
        f"uniswap-forward-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    )
    try:
        state = SnowflakeWatermarkStore(connection).get(UNISWAP_FORWARD_WATERMARK)
        if state is None:
            raise RuntimeError("uniswap_forward must be initialized explicitly")
        spark = create_spark_session("CaptureUniswapRecoveryIncrement")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.sparkContext.setLogLevel("WARN")
        records = PoolMetadataStore(
            spark,
            pool_path=DATA_LAKE.get("reference_uniswap_v3_pools"),
            watermark_path=DATA_LAKE.get("state_uniswap_v3_pools"),
        ).load_records()
        contract = canonical_pool_contract(records, chain=CHAIN)
        assert_source_contract_matches(state, contract.fingerprint)
        plan = plan_recovery_increment(
            committed_block=state.committed_value,
            safe_head=safe_head,
            max_blocks=get_uniswap_forward_max_blocks_per_run(),
        )
        if plan.is_noop:
            return ForwardCaptureSummary(
                capture_run_id,
                plan,
                observed_head,
                len(contract.pool_addresses),
                contract.fingerprint,
                0,
                0,
                0,
                0,
                Counter(),
                0,
            )
        assert plan.start_block is not None and plan.end_block is not None
        processed_at = datetime.now(UTC)
        leaves = []
        events = ()
        issues = []
        logical_ranges = 0
        query_requests = 0
        splits = 0
        next_block = plan.start_block
        while next_block <= plan.end_block:
            parent_end = min(
                plan.end_block,
                next_block + get_uniswap_recovery_chunk_size() - 1,
            )
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
            splits += fetched.split_count
            next_block = parent_end + 1
        UniswapRecoveryBatchStore(
            spark,
            bronze_path=DATA_LAKE.get("bronze_uniswap_recovery"),
            quarantine_path=DATA_LAKE.get("quarantine_uniswap_recovery"),
        ).write_and_validate(
            start_block=plan.start_block,
            end_block=plan.end_block,
            events=events,
            issues=tuple(issues),
            processed_at=processed_at,
            contract_fingerprint=contract.fingerprint,
            pool_count=len(contract.pool_addresses),
            event_topic_count=len(contract.event_topics),
            deduplication_paths=(
                (DATA_LAKE.get("bronze_events"), True),
            ),
        )
        SnowflakeCompletedRangeStore(connection).record_ranges(
            UNISWAP_FORWARD_WATERMARK,
            tuple(
                CompletedBlockRange(
                    item.start_block,
                    item.end_block,
                    capture_run_id,
                    processed_at,
                )
                for item in leaves
            ),
        )
        return ForwardCaptureSummary(
            capture_run_id,
            plan,
            observed_head,
            len(contract.pool_addresses),
            contract.fingerprint,
            logical_ranges,
            len(leaves),
            query_requests,
            splits,
            Counter(event.event_type for event in events),
            len(issues),
        )
    finally:
        if spark is not None:
            spark.stop()
        connection.close()
