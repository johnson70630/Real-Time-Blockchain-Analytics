"""Run one finite, commit-last Aave forward increment."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from time import monotonic

from backfill.run_aave_incremental import run_forward_capture
from config.settings import (
    STORAGE,
    get_aave_backfill_rpc_url,
    get_aave_confirmation_depth,
    get_aave_forward_max_blocks_per_run,
    get_aave_v3_pool_address,
)
from orchestration.observability import ErrorStage, PipelineOutcome, RunStatus, RunType
from orchestration.run_metrics import aave_forward_metrics
from orchestration.run_observer import PipelineRunObserver
from orchestration.watermark import (
    AAVE_FORWARD_WATERMARK,
    commit_watermark_from_expected,
)
from warehouse.watermarks import SnowflakeWatermarkStore, connect_watermark_store

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PYTHON = os.getenv("PROJECT_PYTHON", sys.executable)
WAREHOUSE_PYTHON = os.getenv("WAREHOUSE_PYTHON", sys.executable)
DBT = os.getenv("DBT", str(Path(sys.executable).with_name("dbt")))


def validate_environment() -> None:
    """Validate the existing Aave forward configuration without changing state."""
    if not STORAGE.is_s3:
        raise ValueError("Aave forward processing requires STORAGE_MODE=s3")
    get_aave_backfill_rpc_url()
    get_aave_v3_pool_address()
    get_aave_confirmation_depth()
    get_aave_forward_max_blocks_per_run()


def _run(command: tuple[str, ...]) -> None:
    started = monotonic()
    LOGGER.info("Starting command=%s", " ".join(command[1:3]))
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
    LOGGER.info(
        "Completed command=%s duration_seconds=%.3f",
        " ".join(command[1:3]),
        monotonic() - started,
    )


def run_pipeline(
    run_id: str,
    *,
    safe_head: int | None = None,
    observer: PipelineRunObserver | None = None,
) -> PipelineOutcome:
    """Run existing source, Silver, RAW, dbt, and commit boundaries in order."""
    if observer is not None:
        observer.set_stage(ErrorStage.SOURCE_FETCH)
    capture = run_forward_capture(frozen_safe_end=safe_head, run_id=run_id)
    if capture.is_noop:
        LOGGER.info("Aave forward is a clean no-op at block=%s", capture.committed_block)
        return PipelineOutcome(
            RunStatus.NO_OP,
            aave_forward_metrics(
                capture,
                watermark_after=capture.committed_block,
            ),
        )

    assert capture.start_block is not None and capture.end_block is not None
    if observer is not None:
        observer.set_stage(ErrorStage.SILVER)
    _run(
        (
            PROJECT_PYTHON,
            "-m",
            "spark.build_aave_silver_increment",
            "--start-block",
            str(capture.start_block),
            "--end-block",
            str(capture.end_block),
        )
    )
    if observer is not None:
        observer.set_stage(ErrorStage.SNOWFLAKE)
    _run(
        (
            WAREHOUSE_PYTHON,
            "-m",
            "warehouse.load_raw",
            "--dataset",
            "silver_aave_borrows",
            "--dataset",
            "silver_aave_repays",
            "--dataset",
            "silver_aave_liquidations",
        )
    )
    if observer is not None:
        observer.set_stage(ErrorStage.DBT)
    _run((DBT, "run", "--project-dir", str(PROJECT_ROOT / "dbt")))
    _run((DBT, "test", "--project-dir", str(PROJECT_ROOT / "dbt")))

    if observer is not None:
        observer.set_stage(ErrorStage.WATERMARK_COMMIT)
    with closing(connect_watermark_store()) as connection:
        committed = commit_watermark_from_expected(
            SnowflakeWatermarkStore(connection),
            AAVE_FORWARD_WATERMARK,
            expected_value=capture.committed_block,
            candidate_value=capture.end_block,
            successful_run_id=run_id,
            processing_succeeded=True,
        )
    LOGGER.info(
        "Aave forward committed block=%s logical_ranges=%s rpc_requests=%s "
        "borrow=%s repay=%s liquidation=%s quarantine=%s",
        committed.committed_value,
        capture.logical_ranges,
        capture.range_attempts,
        capture.event_counts["borrow"],
        capture.event_counts["repay"],
        capture.event_counts["liquidation"],
        capture.quarantine_count,
    )
    return PipelineOutcome(
        RunStatus.SUCCEEDED,
        aave_forward_metrics(
            capture,
            watermark_after=committed.committed_value,
        ),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate-environment", "run"))
    parser.add_argument("--run-id", default="manual-aave-forward")
    parser.add_argument("--safe-head", type=int)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()
    if args.action == "run":
        with PipelineRunObserver(
            "aave_forward",
            args.run_id,
            RunType.FORWARD,
        ) as observer:
            observer.set_stage(ErrorStage.VALIDATION)
            validate_environment()
            observer.finish(
                run_pipeline(
                    args.run_id,
                    safe_head=args.safe_head,
                    observer=observer,
                )
            )
    else:
        validate_environment()


if __name__ == "__main__":
    main()
