"""Validate and invoke existing finite DeFi backfill entrypoints."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from config.settings import (
    CHAINLINK_FEEDS_CONFIG,
    STORAGE,
    get_aave_backfill_rpc_url,
    get_aave_v3_pool_address,
    get_alchemy_rpc_url,
)

LOGGER = logging.getLogger(__name__)
SUPPORTED_BACKFILLS = frozenset({"aave", "chainlink"})
PROJECT_PYTHON = os.getenv("PROJECT_PYTHON", "/opt/project-venv/bin/python")
WAREHOUSE_PYTHON = os.getenv(
    "WAREHOUSE_PYTHON", "/opt/warehouse-venv/bin/python"
)

WAREHOUSE_DATASETS = {
    "aave": (
        "silver_aave_borrows",
        "silver_aave_repays",
        "silver_aave_liquidations",
    ),
    "chainlink": ("silver_chainlink_market_prices",),
}


@dataclass(frozen=True, slots=True)
class BackfillRequest:
    """One validated, inclusive blockchain block window."""

    backfill_type: str
    start_block: int
    end_block: int

    @property
    def block_count(self) -> int:
        """Return the finite number of blocks in the inclusive window."""
        return self.end_block - self.start_block + 1


def validate_request(
    backfill_type: str,
    start_block: int | None,
    end_block: int | None,
) -> BackfillRequest:
    """Validate supported type and finite, increasing block bounds."""
    normalized_type = backfill_type.strip().lower()
    if normalized_type not in SUPPORTED_BACKFILLS:
        supported = ", ".join(sorted(SUPPORTED_BACKFILLS))
        raise ValueError(f"backfill_type must be one of: {supported}")
    if start_block is None or end_block is None:
        raise ValueError("start_block and end_block are required")
    if start_block < 0 or end_block < 0:
        raise ValueError("block bounds must not be negative")
    if start_block >= end_block:
        raise ValueError("start_block must be less than end_block")
    return BackfillRequest(normalized_type, start_block, end_block)


def source_commands(request: BackfillRequest) -> tuple[tuple[str, ...], ...]:
    """Build argument-safe commands for existing extraction and Silver jobs."""
    if request.backfill_type == "aave":
        # Targets larger than any bounded operational run force the existing
        # target-aware extractor to traverse the complete requested window.
        force_full_window = str(2**31 - 1)
        return (
            (
                PROJECT_PYTHON,
                "-m",
                "backfill.run_aave_v3",
                "--to-block",
                str(request.end_block),
                "--max-blocks",
                str(request.block_count),
                "--target-borrow",
                force_full_window,
                "--target-repay",
                force_full_window,
                "--target-liquidation",
                force_full_window,
            ),
            (PROJECT_PYTHON, "-m", "spark.build_aave_silver"),
        )
    return (
        (
            PROJECT_PYTHON,
            "-m",
            "backfill.run_chainlink",
            "--from-block",
            str(request.start_block),
            "--to-block",
            str(request.end_block),
            "--overlap-rounds",
            "1",
        ),
        (PROJECT_PYTHON, "-m", "spark.build_market_data_silver"),
    )


def warehouse_command(
    request: BackfillRequest, *, validate_only: bool
) -> tuple[str, ...]:
    """Build the existing RAW loader command for the selected source."""
    command = [WAREHOUSE_PYTHON, "-m", "warehouse.load_raw"]
    if validate_only:
        command.append("--validate-inputs")
    for dataset in WAREHOUSE_DATASETS[request.backfill_type]:
        command.extend(("--dataset", dataset))
    return tuple(command)


def validate_environment(request: BackfillRequest) -> None:
    """Fail before extraction when RPC, S3, or source config is unavailable."""
    if not STORAGE.is_s3:
        raise ValueError("Operational backfills require STORAGE_MODE=s3")
    if request.backfill_type == "aave":
        get_aave_backfill_rpc_url()
        get_aave_v3_pool_address()
    else:
        get_alchemy_rpc_url()
        if not Path(CHAINLINK_FEEDS_CONFIG).is_file():
            raise FileNotFoundError(
                f"Chainlink feeds config not found: {CHAINLINK_FEEDS_CONFIG}"
            )


def _run(command: Sequence[str]) -> None:
    """Run one command without a shell and surface its non-zero status."""
    LOGGER.info("Starting command=%s", " ".join(command[1:3]))
    started = time.monotonic()
    subprocess.run(tuple(command), check=True)
    LOGGER.info(
        "Completed command=%s duration_seconds=%.3f",
        " ".join(command[1:3]),
        time.monotonic() - started,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=("validate", "validate-environment", "run", "validate-output", "load"),
    )
    parser.add_argument("--backfill-type", required=True)
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    parser.add_argument("--dag-run-id", default="unknown")
    parser.add_argument("--logical-date", default="unknown")
    return parser.parse_args()


def main() -> None:
    """Execute one thin orchestration action for a bounded request."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()
    request = validate_request(
        args.backfill_type,
        args.start_block,
        args.end_block,
    )
    LOGGER.info(
        "Backfill context run_id=%s logical_date=%s type=%s range=%s-%s blocks=%s action=%s",
        args.dag_run_id,
        args.logical_date,
        request.backfill_type,
        request.start_block,
        request.end_block,
        request.block_count,
        args.action,
    )
    if args.action == "validate":
        return
    if args.action == "validate-environment":
        validate_environment(request)
        return
    if args.action == "run":
        for command in source_commands(request):
            _run(command)
        return
    _run(
        warehouse_command(
            request,
            validate_only=args.action == "validate-output",
        )
    )


if __name__ == "__main__":
    main()
