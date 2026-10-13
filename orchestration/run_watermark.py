"""Inspect, initialize, or safely advance one durable watermark."""

from __future__ import annotations

import argparse
import logging
import os
from contextlib import closing

from orchestration.watermark import (
    ProcessingMode,
    WatermarkIdentity,
    commit_watermark,
    get_watermark,
    initialize_watermark,
)
from warehouse.load_raw import DEFAULT_CONNECTION_NAME, DEFAULT_WAREHOUSE
from warehouse.watermarks import SnowflakeWatermarkStore, connect_watermark_store

LOGGER = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("inspect", "initialize", "advance"))
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--source-name", required=True)
    parser.add_argument("--chain-id", required=True, type=int)
    parser.add_argument("--watermark-type", default="block_number")
    parser.add_argument("--value", type=int)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--processing-mode",
        choices=tuple(mode.value for mode in ProcessingMode),
        default=ProcessingMode.FORWARD.value,
    )
    parser.add_argument(
        "--confirm-success",
        action="store_true",
        help="confirm every required persistence and validation step succeeded",
    )
    parser.add_argument(
        "--connection-name",
        default=os.getenv("SNOWFLAKE_CONNECTION_NAME", DEFAULT_CONNECTION_NAME),
    )
    parser.add_argument(
        "--warehouse",
        default=os.getenv("SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE),
    )
    return parser.parse_args()


def _require_mutation_args(args: argparse.Namespace) -> tuple[int, str]:
    if args.value is None or not args.run_id:
        raise ValueError("--value and --run-id are required for this action")
    return args.value, args.run_id


def main() -> None:
    """Execute one explicit control-plane operation without logging secrets."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()
    identity = WatermarkIdentity(
        pipeline_name=args.pipeline_name,
        source_name=args.source_name,
        chain_id=args.chain_id,
        watermark_type=args.watermark_type,
    )
    with closing(
        connect_watermark_store(args.connection_name, args.warehouse)
    ) as connection:
        store = SnowflakeWatermarkStore(connection)
        if args.action == "inspect":
            state = get_watermark(store, identity)
            if state is None:
                LOGGER.info("Watermark is not initialized")
                return
        else:
            value, run_id = _require_mutation_args(args)
            if args.action == "initialize":
                state = initialize_watermark(store, identity, value, run_id)
            else:
                state = commit_watermark(
                    store,
                    identity,
                    value,
                    run_id,
                    processing_succeeded=args.confirm_success,
                    processing_mode=ProcessingMode(args.processing_mode),
                )
        LOGGER.info(
            "Watermark pipeline=%s source=%s chain_id=%s type=%s value=%s "
            "run_id=%s updated_at=%s",
            state.identity.pipeline_name,
            state.identity.source_name,
            state.identity.chain_id,
            state.identity.watermark_type,
            state.committed_value,
            state.successful_run_id,
            state.updated_at.isoformat(),
        )


if __name__ == "__main__":
    main()
