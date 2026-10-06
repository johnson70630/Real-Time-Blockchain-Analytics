"""CLI for the bounded historical Aave V3 backfill."""

from __future__ import annotations

import argparse
import logging
from collections import Counter
from datetime import UTC, datetime

from web3 import Web3

from backfill.aave_v3 import (
    AaveV3RpcClient,
    SearchLimits,
    preserve_backward_checkpoint,
    search_historical_events,
)
from backfill.storage import AaveBackfillStore, BackfillState
from config.logging import configure_logging
from config.settings import CHAIN, DATA_LAKE, get_aave_v3_pool_address, get_alchemy_rpc_url
from spark.session import create_spark_session

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a bounded historical Aave V3 event backfill")
    parser.add_argument("--to-block", type=int)
    parser.add_argument("--chunk-size", type=int, default=10)
    parser.add_argument("--max-blocks", type=int, default=2_000)
    parser.add_argument("--max-rpc-requests", type=int, default=500)
    parser.add_argument("--target-borrow", type=int, default=20)
    parser.add_argument("--target-repay", type=int, default=20)
    parser.add_argument("--target-liquidation", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    pool_address = get_aave_v3_pool_address()
    if not Web3.is_address(pool_address):
        raise ValueError("AAVE_V3_POOL_ADDRESS must be a valid Ethereum address")

    rpc = AaveV3RpcClient(get_alchemy_rpc_url())
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the configured Alchemy RPC")

    spark = create_spark_session("BackfillAaveV3")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    store = AaveBackfillStore(
        spark,
        bronze_path=DATA_LAKE.get("bronze_aave_backfill"),
        state_path=DATA_LAKE.get("state_aave_backfill"),
        quarantine_path=DATA_LAKE.get("quarantine_aave_backfill"),
    )
    try:
        existing = store.load_events()
        state = store.load_state()
        head = rpc.get_latest_block_number()
        end_block = args.to_block if args.to_block is not None else (
            state.next_end_block if state is not None else head
        )
        if end_block > head:
            raise ValueError("Backfill end block cannot exceed the confirmed chain head")
        limits = SearchLimits(
            chunk_size=args.chunk_size,
            max_blocks=args.max_blocks,
            max_rpc_requests=args.max_rpc_requests,
            target_borrow=args.target_borrow,
            target_repay=args.target_repay,
            target_liquidation=args.target_liquidation,
        )
        result = search_historical_events(
            rpc,
            pool_address=pool_address,
            chain=CHAIN,
            end_block=end_block,
            limits=limits,
            existing=existing,
        )
        processed_at = datetime.now(UTC)
        if result.events != existing:
            store.write_events(result.events, processed_at)
        next_end_block = preserve_backward_checkpoint(
            state.next_end_block if state is not None else None,
            result.next_end_block,
        )
        store.write_state(BackfillState(next_end_block, head, processed_at))
        store.write_issues(result.issues)
        counts = Counter(event.event_type for event in result.events)
        logger.info(
            "Aave backfill range=%s-%s blocks=%s requests=%s targets_met=%s",
            result.start_block,
            result.end_block,
            result.blocks_scanned,
            result.rpc_requests,
            result.targets_met,
        )
        logger.info(
            "Aave canonical events borrow=%s repay=%s liquidation=%s quarantine=%s output=%s next_end=%s",
            counts["borrow"],
            counts["repay"],
            counts["liquidation"],
            len(result.issues),
            DATA_LAKE.get("bronze_aave_backfill"),
            next_end_block,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
