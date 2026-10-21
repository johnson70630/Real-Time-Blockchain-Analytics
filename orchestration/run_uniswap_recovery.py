"""Run one finite, commit-last Uniswap source-gap recovery cycle."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from time import monotonic

from backfill.run_uniswap_incremental import run_forward_capture
from config.settings import (
    STORAGE,
    get_blockchain_recovery_rpc_url,
    get_uniswap_confirmation_depth,
    get_uniswap_forward_max_blocks_per_run,
)
from orchestration.watermark import (
    UNISWAP_FORWARD_WATERMARK,
    commit_watermark_from_expected,
)
from orchestration.uniswap_recovery import (
    UNISWAP_FORWARD_LEASE,
    assert_source_contract_matches,
    source_contract_run_id,
)
from warehouse.leases import (
    PipelineLeaseHandle,
    SnowflakePipelineLeaseStore,
)
from warehouse.watermarks import SnowflakeWatermarkStore, connect_watermark_store

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PYTHON = os.getenv("PROJECT_PYTHON", sys.executable)
WAREHOUSE_PYTHON = os.getenv("WAREHOUSE_PYTHON", sys.executable)
DBT = os.getenv("DBT", str(Path(sys.executable).with_name("dbt")))


def validate_environment() -> None:
    """Validate required non-secret recovery settings without changing state."""
    if not STORAGE.is_s3:
        raise ValueError("Uniswap recovery requires STORAGE_MODE=s3")
    get_blockchain_recovery_rpc_url()
    get_uniswap_confirmation_depth()
    get_uniswap_forward_max_blocks_per_run()


def _run(command: tuple[str, ...]) -> None:
    started = monotonic()
    LOGGER.info("Starting command=%s", " ".join(command[1:3]))
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
    LOGGER.info(
        "Completed command=%s duration_seconds=%.3f",
        " ".join(command[1:3]),
        monotonic() - started,
    )


def _run_pipeline_under_lease(
    run_id: str,
    *,
    safe_head: int | None = None,
) -> None:
    """Run one recovery after the caller has acquired single-writer ownership."""
    capture = run_forward_capture(run_id, safe_head_override=safe_head)
    plan = capture.plan
    if plan.is_noop:
        LOGGER.info(
            "Uniswap recovery is a clean no-op at block=%s",
            plan.committed_block,
        )
        return
    assert plan.start_block is not None and plan.end_block is not None
    _run(
        (
            PROJECT_PYTHON,
            "-m",
            "spark.build_uniswap_silver_increment",
            "--start-block",
            str(plan.start_block),
            "--end-block",
            str(plan.end_block),
        )
    )
    _run(
        (
            WAREHOUSE_PYTHON,
            "-m",
            "warehouse.load_raw",
            "--dataset",
            "silver_uniswap_swaps",
        )
    )
    _run((DBT, "run", "--project-dir", str(PROJECT_ROOT / "dbt")))
    _run((DBT, "test", "--project-dir", str(PROJECT_ROOT / "dbt")))
    with closing(connect_watermark_store()) as connection:
        store = SnowflakeWatermarkStore(connection)
        current = store.get(UNISWAP_FORWARD_WATERMARK)
        if current is None:
            raise RuntimeError("uniswap_forward disappeared before commit")
        assert_source_contract_matches(current, capture.contract_fingerprint)
        committed = commit_watermark_from_expected(
            store,
            UNISWAP_FORWARD_WATERMARK,
            expected_value=plan.committed_block,
            candidate_value=plan.end_block,
            successful_run_id=source_contract_run_id(
                run_id,
                capture.contract_fingerprint,
            ),
            processing_succeeded=True,
        )
    LOGGER.info(
        "Uniswap recovery committed block=%s pools=%s logical_ranges=%s "
        "queries=%s swap=%s mint=%s burn=%s quarantine=%s",
        committed.committed_value,
        capture.pool_count,
        capture.logical_ranges,
        capture.query_requests,
        capture.event_counts["swap"],
        capture.event_counts["mint"],
        capture.event_counts["burn"],
        capture.quarantine_count,
    )


def _acquire_lease(run_id: str) -> PipelineLeaseHandle:
    with closing(connect_watermark_store()) as connection:
        return SnowflakePipelineLeaseStore(connection).acquire(
            UNISWAP_FORWARD_LEASE,
            run_id,
        )


def _release_lease(handle: PipelineLeaseHandle) -> None:
    with closing(connect_watermark_store()) as connection:
        SnowflakePipelineLeaseStore(connection).release(handle)


def run_pipeline(run_id: str, *, safe_head: int | None = None) -> None:
    """Run one commit-last recovery while holding the shared two-hour lease."""
    handle = _acquire_lease(run_id)
    LOGGER.info(
        "Acquired Uniswap recovery lease owner=%s version=%s expires_at=%s",
        handle.owner_run_id,
        handle.lease_version,
        handle.expires_at,
    )
    failed = False
    try:
        _run_pipeline_under_lease(run_id, safe_head=safe_head)
    except BaseException:
        failed = True
        raise
    finally:
        try:
            _release_lease(handle)
            LOGGER.info(
                "Released Uniswap recovery lease owner=%s version=%s",
                handle.owner_run_id,
                handle.lease_version,
            )
        except Exception:
            if not failed:
                raise
            LOGGER.exception(
                "Recovery failed and owner/version-checked lease release also failed"
            )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate-environment", "run"))
    parser.add_argument("--run-id", default="manual-uniswap-recovery")
    parser.add_argument("--safe-head", type=int)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()
    validate_environment()
    if args.action == "run":
        run_pipeline(args.run_id, safe_head=args.safe_head)


if __name__ == "__main__":
    main()
