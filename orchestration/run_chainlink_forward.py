"""Run the finite commit-last Chainlink forward pipeline."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from time import monotonic

from backfill.run_chainlink_incremental import run_forward_capture
from config.settings import (
    CHAIN,
    CHAINLINK_FEEDS_CONFIG,
    STORAGE,
    get_alchemy_rpc_url,
    get_chainlink_confirmation_depth,
    get_chainlink_forward_max_rounds_per_run,
)
from market_data.feeds import load_chainlink_feeds
from orchestration.chainlink_bootstrap import (
    CHAINLINK_BOOTSTRAP_ROUNDS,
    CHAINLINK_BOOTSTRAP_SAFE_BLOCK,
)
from orchestration.watermark import (
    CHAINLINK_FORWARD_WATERMARK,
    chainlink_feed_watermark,
    commit_watermarks_atomically,
    initialize_watermarks_atomically,
)
from warehouse.watermarks import SnowflakeWatermarkStore, connect_watermark_store

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PYTHON = os.getenv("PROJECT_PYTHON", sys.executable)
WAREHOUSE_PYTHON = os.getenv("WAREHOUSE_PYTHON", sys.executable)
DBT = os.getenv("DBT", str(Path(sys.executable).with_name("dbt")))


def validate_environment() -> None:
    """Validate non-mutating prerequisites before source or warehouse work."""
    if not STORAGE.is_s3:
        raise ValueError("Chainlink forward processing requires STORAGE_MODE=s3")
    get_alchemy_rpc_url()
    get_chainlink_confirmation_depth()
    get_chainlink_forward_max_rounds_per_run()
    load_chainlink_feeds(CHAINLINK_FEEDS_CONFIG, expected_chain=CHAIN)


def _run(command: tuple[str, ...]) -> None:
    started = monotonic()
    LOGGER.info("Starting command=%s", " ".join(command[1:3]))
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
    LOGGER.info(
        "Completed command=%s duration_seconds=%.3f",
        " ".join(command[1:3]),
        monotonic() - started,
    )


def run_pipeline(run_id: str) -> None:
    """Process one frozen snapshot and commit all five cursors last."""
    capture = run_forward_capture(run_id)
    plan = capture.plan
    if plan.is_noop:
        LOGGER.info("Chainlink forward is a clean no-op at block=%s", plan.committed_block)
        return

    LOGGER.info(
        "Chainlink snapshot old_block=%s head=%s safe_block=%s block_delta=%s "
        "new_rounds=%s batches=%s replayed_batches=%s rpc_requests=%s",
        plan.committed_block,
        plan.snapshot.observed_head,
        plan.snapshot.block.number,
        plan.snapshot.block.number - plan.committed_block,
        plan.round_count,
        len(capture.batches),
        sum(batch.replayed for batch in capture.batches),
        capture.rpc_requests,
    )
    for target in plan.snapshot.targets:
        current = capture.current_states[target.identity].committed_value
        LOGGER.info(
            "Chainlink target feed=%s address=%s old_round=%s target_round=%s "
            "changed=%s updated_at=%s persisted_observations=%s",
            target.feed.name,
            target.feed.feed_address.lower(),
            current,
            target.round_id,
            target.round_id != current,
            target.round.updated_at,
            capture.observations_by_feed.get(target.feed.feed_address.lower(), 0),
        )

    if plan.round_count:
        _run(
            (
                PROJECT_PYTHON,
                "-m",
                "spark.build_chainlink_silver_increment",
                "--start-block",
                str(plan.committed_block + 1),
                "--end-block",
                str(plan.snapshot.block.number),
            )
        )
        _run(
            (
                WAREHOUSE_PYTHON,
                "-m",
                "warehouse.load_raw",
                "--dataset",
                "silver_chainlink_market_prices",
            )
        )

        _run((DBT, "run", "--project-dir", str(PROJECT_ROOT / "dbt")))
        _run((DBT, "test", "--project-dir", str(PROJECT_ROOT / "dbt")))
    else:
        LOGGER.info("No new rounds; skipping Silver, Snowflake, and dbt mutations")
    with closing(connect_watermark_store()) as connection:
        committed = commit_watermarks_atomically(
            SnowflakeWatermarkStore(connection),
            capture.current_states,
            plan.candidates,
            run_id,
            processing_succeeded=True,
        )
    LOGGER.info(
        "Chainlink state committed identities=%s global_block=%s",
        len(committed),
        plan.snapshot.block.number,
    )


def initialize_bootstrap_state(run_id: str) -> tuple:
    """Atomically initialize the accepted global block and four feed cursors."""
    values = {
        CHAINLINK_FORWARD_WATERMARK: CHAINLINK_BOOTSTRAP_SAFE_BLOCK,
        **{
            chainlink_feed_watermark(address): terminal_round
            for address, (_start_round, terminal_round) in (
                CHAINLINK_BOOTSTRAP_ROUNDS.items()
            )
        },
    }
    with closing(connect_watermark_store()) as connection:
        store = SnowflakeWatermarkStore(connection)
        initialized = initialize_watermarks_atomically(store, values, run_id)
        verified = tuple(store.get(identity) for identity in values)
    actual = {
        state.identity: state.committed_value
        for state in verified
        if state is not None
    }
    if actual != values:
        raise RuntimeError("Chainlink bootstrap state read-back failed")
    LOGGER.info(
        "Chainlink bootstrap state initialized atomically rows=%s run_id=%s",
        len(initialized),
        run_id,
    )
    return initialized


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=("validate-environment", "initialize-bootstrap", "run"),
    )
    parser.add_argument("--run-id", default="manual-chainlink-forward")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()
    validate_environment()
    if args.action == "initialize-bootstrap":
        initialize_bootstrap_state(args.run_id)
    elif args.action == "run":
        run_pipeline(args.run_id)


if __name__ == "__main__":
    main()
