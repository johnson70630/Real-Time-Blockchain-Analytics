"""Translate existing pipeline summaries into the shared observability model."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from orchestration.observability import RunMetrics


def aave_forward_metrics(
    capture: Any,
    *,
    watermark_after: int,
    records_inserted: int = 0,
    records_updated: int = 0,
    retry_count: int = 0,
) -> RunMetrics:
    """Build run metrics from the existing Aave forward capture summary."""
    counts = capture.event_counts
    total = sum(int(value) for value in counts.values())
    return RunMetrics(
        source_start_position=capture.start_block,
        source_end_position=capture.end_block,
        source_head_position=capture.observed_head,
        safe_head_position=capture.safe_head,
        watermark_before=capture.committed_block,
        watermark_after=watermark_after,
        records_read=total,
        records_written=total,
        records_inserted=records_inserted,
        records_updated=records_updated,
        records_quarantined=capture.quarantine_count,
        retry_count=retry_count,
        details={
            "rpc_logical_ranges": capture.logical_ranges,
            "rpc_requests": capture.range_attempts,
            "adaptive_splits": capture.adaptive_splits,
            "completed_ranges": capture.completed_ranges,
            "borrow_records": counts["borrow"],
            "repay_records": counts["repay"],
            "liquidation_records": counts["liquidation"],
        },
    )


def chainlink_forward_metrics(
    capture: Any,
    *,
    watermark_after: int,
    records_inserted: int = 0,
    records_updated: int = 0,
    retry_count: int = 0,
) -> RunMetrics:
    """Build run metrics from one atomic Chainlink forward capture."""
    counts: Mapping[str, int] = capture.observations_by_feed
    observations = sum(int(value) for value in counts.values())
    details = {
        "rounds_discovered": capture.plan.round_count,
        "rpc_requests": capture.rpc_requests,
        "batches": len(capture.batches),
        "replayed_batches": sum(batch.replayed for batch in capture.batches),
    }
    details.update(
        {
            f"feed_{index}_observations": count
            for index, (_address, count) in enumerate(sorted(counts.items()), start=1)
        }
    )
    return RunMetrics(
        source_start_position=(
            capture.plan.committed_block + 1
            if capture.plan.snapshot.block.number > capture.plan.committed_block
            else None
        ),
        source_end_position=(
            capture.plan.snapshot.block.number
            if capture.plan.snapshot.block.number > capture.plan.committed_block
            else None
        ),
        source_head_position=capture.plan.snapshot.observed_head,
        safe_head_position=capture.plan.snapshot.block.number,
        watermark_before=capture.plan.committed_block,
        watermark_after=watermark_after,
        records_read=observations,
        records_written=observations,
        records_inserted=records_inserted,
        records_updated=records_updated,
        retry_count=retry_count,
        details=details,
    )


def uniswap_forward_metrics(
    capture: Any,
    *,
    watermark_after: int,
    records_inserted: int = 0,
    records_updated: int = 0,
    silver_records: int,
    live_overlap_records: int,
    retry_count: int = 0,
) -> RunMetrics:
    """Build run metrics from one leased Uniswap recovery capture."""
    counts = capture.event_counts
    total = sum(int(value) for value in counts.values())
    return RunMetrics(
        source_start_position=capture.plan.start_block,
        source_end_position=capture.plan.end_block,
        source_head_position=capture.observed_head,
        safe_head_position=capture.plan.safe_head,
        watermark_before=capture.plan.committed_block,
        watermark_after=watermark_after,
        records_read=total + live_overlap_records,
        records_written=total,
        records_inserted=records_inserted,
        records_updated=records_updated,
        records_quarantined=capture.quarantine_count,
        retry_count=retry_count,
        details={
            "logical_ranges": capture.logical_ranges,
            "rpc_requests": capture.query_requests,
            "adaptive_splits": capture.adaptive_splits,
            "completed_ranges": capture.completed_ranges,
            "pool_count": capture.pool_count,
            "swap_records": counts["swap"],
            "mint_records": counts["mint"],
            "burn_records": counts["burn"],
            "live_overlap_records": live_overlap_records,
            "net_new_canonical_records": silver_records,
            "silver_records": silver_records,
        },
    )


def warehouse_metrics(
    *,
    copy_files: int,
    records_read: int,
    records_inserted: int,
    records_updated: int,
    dbt_models: int,
    dbt_tests: int,
    retry_count: int = 0,
) -> RunMetrics:
    """Build pipeline-level warehouse metrics without duplicating task history."""
    return RunMetrics(
        records_read=records_read,
        records_written=records_inserted + records_updated,
        records_inserted=records_inserted,
        records_updated=records_updated,
        retry_count=retry_count,
        details={
            "copy_files": copy_files,
            "dbt_models": dbt_models,
            "dbt_tests": dbt_tests,
        },
    )
