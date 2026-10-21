"""Audit a bounded source replay without mutating forward production state."""

from __future__ import annotations

import argparse
import logging
from contextlib import closing, nullcontext
from time import monotonic

from config.settings import (
    CHAIN,
    CHAINLINK_FEEDS_CONFIG,
    get_aave_backfill_rpc_url,
    get_aave_v3_pool_address,
    get_alchemy_rpc_url,
    get_blockchain_recovery_rpc_url,
)
from market_data.feeds import load_chainlink_feeds
from orchestration.correction_replay import (
    ProcessingMode,
    ReplayRequest,
    execute_replay,
)
from orchestration.observability import (
    ErrorStage,
    PipelineOutcome,
    RunMetrics,
    RunStatus,
    RunType,
)
from orchestration.run_observer import PipelineRunObserver
from orchestration.replay_sources import (
    fetch_fresh_records,
    load_stored_records,
    read_production_watermarks,
)
from orchestration.uniswap_recovery import UNISWAP_FORWARD_LEASE
from warehouse.leases import SnowflakePipelineLeaseStore
from warehouse.watermarks import connect_watermark_store

LOGGER = logging.getLogger(__name__)


def validate_environment(request: ReplayRequest) -> None:
    """Resolve only the credentials/configuration required by the source."""
    if request.source == "aave":
        get_aave_backfill_rpc_url()
        get_aave_v3_pool_address()
    elif request.source == "uniswap":
        get_blockchain_recovery_rpc_url()
    else:
        get_alchemy_rpc_url()
        load_chainlink_feeds(CHAINLINK_FEEDS_CONFIG, expected_chain=CHAIN)


def run_audit(
    request: ReplayRequest,
    run_id: str,
    observer: PipelineRunObserver | None = None,
) -> PipelineOutcome:
    """Reconcile one range, sharing the Uniswap writer lease when applicable."""
    started = monotonic()
    if observer is not None:
        observer.set_stage(ErrorStage.SOURCE_FETCH)
    with closing(connect_watermark_store()) as connection:
        lease_store = SnowflakePipelineLeaseStore(connection)
        lease_context = (
            UniswapReplayLease(lease_store, run_id)
            if request.source == "uniswap"
            else nullcontext()
        )
        with lease_context:
            execution = execute_replay(
                request,
                read_watermarks=lambda: read_production_watermarks(connection),
                load_stored=lambda replay, fresh: load_stored_records(
                    connection,
                    replay,
                    fresh,
                ),
                fetch_fresh=lambda replay: fetch_fresh_records(
                    connection,
                    replay,
                ),
            )
    result = execution.reconciliation
    LOGGER.info(
        "Correction replay audit source=%s mode=%s range=%s-%s "
        "stored=%s fresh=%s unchanged=%s added=%s missing=%s changed=%s "
        "applied=%s rpc_requests=%s duration_seconds=%.3f",
        request.source,
        request.mode.value,
        request.start_block,
        request.end_block,
        len(result.unchanged) + len(result.missing_from_source) + len(result.changed),
        len(result.unchanged) + len(result.added) + len(result.changed),
        len(result.unchanged),
        len(result.added),
        len(result.missing_from_source),
        len(result.changed),
        execution.applied,
        execution.source_snapshot.request_count,
        monotonic() - started,
    )
    for label, identities in (
        ("added", (item.identity for item in result.added)),
        (
            "missing_from_source",
            (item.identity for item in result.missing_from_source),
        ),
        ("changed", (fresh.identity for _stored, fresh in result.changed)),
    ):
        values = tuple(identities)
        if values:
            LOGGER.warning("Replay %s identities=%s", label, ",".join(values))
    watermark_keys = {
        "aave": "aave_forward/aave_v3_pool",
        "chainlink": "chainlink_forward/chainlink_feeds",
        "uniswap": "uniswap_forward/uniswap_v3_pools",
    }
    watermark = execution.watermark_state[watermark_keys[request.source]]
    metrics = RunMetrics(
        source_start_position=request.start_block,
        source_end_position=request.end_block,
        watermark_before=watermark,
        watermark_after=watermark,
        records_read=len(execution.source_snapshot.records),
        records_written=0,
        details={
            "rpc_requests": execution.source_snapshot.request_count,
            "stored_records": (
                len(result.unchanged)
                + len(result.missing_from_source)
                + len(result.changed)
            ),
            "fresh_records": (
                len(result.unchanged) + len(result.added) + len(result.changed)
            ),
            "unchanged_records": len(result.unchanged),
            "added_candidates": len(result.added),
            "missing_candidates": len(result.missing_from_source),
            "changed_candidates": len(result.changed),
        },
    )
    status = RunStatus.SUCCEEDED if result.has_changes else RunStatus.NO_OP
    return PipelineOutcome(status, metrics)


class UniswapReplayLease:
    """Context manager for the existing Uniswap source-level lease."""

    def __init__(self, store: SnowflakePipelineLeaseStore, run_id: str) -> None:
        self.store = store
        self.run_id = run_id
        self.handle = None

    def __enter__(self):
        self.handle = self.store.acquire(UNISWAP_FORWARD_LEASE, self.run_id)
        return self.handle

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        assert self.handle is not None
        self.store.release(self.handle)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "validate-environment", "audit"))
    parser.add_argument("--source", required=True, choices=("aave", "chainlink", "uniswap"))
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    parser.add_argument(
        "--mode",
        choices=(
            ProcessingMode.HISTORICAL_REPLAY.value,
            ProcessingMode.CORRECTION_REPLAY.value,
        ),
        default=ProcessingMode.CORRECTION_REPLAY.value,
    )
    parser.add_argument("--run-id", default="manual-correction-replay")
    return parser.parse_args()


def main() -> None:
    """Validate or audit one bounded operator request."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()
    request = ReplayRequest(
        args.source,
        args.start_block,
        args.end_block,
        ProcessingMode(args.mode),
    )
    if args.action == "validate":
        return
    if args.action == "audit":
        with PipelineRunObserver(
            f"{request.source}_correction_replay",
            args.run_id,
            RunType.CORRECTION,
        ) as observer:
            observer.set_stage(ErrorStage.VALIDATION)
            validate_environment(request)
            observer.finish(run_audit(request, args.run_id, observer))
    else:
        validate_environment(request)


if __name__ == "__main__":
    main()
