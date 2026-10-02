"""Run restartable Uniswap V3 Factory pool-metadata extraction."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime

from web3 import Web3

from config.logging import configure_logging
from config.settings import (
    CHAIN,
    DATA_LAKE,
    UNISWAP_V3_FACTORY_ADDRESS,
    UNISWAP_V3_FACTORY_START_BLOCK,
    get_alchemy_rpc_url,
)
from reference_data.storage import PoolMetadataStore, observed_swap_pools
from reference_data.uniswap_v3_pools import (
    UniswapV3FactoryRpcClient,
    extract_pool_metadata,
    merge_pool_records,
    next_start_block,
    reconcile_pool_coverage,
)
from spark.session import create_spark_session

logger = logging.getLogger(__name__)
# The provisioned Alchemy Free tier accepts at most ten Arbitrum blocks per
# eth_getLogs request. Operators on higher tiers may override this value.
DEFAULT_CHUNK_SIZE = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract canonical Uniswap V3 Factory pool metadata",
    )
    parser.add_argument("--from-block", type=int)
    parser.add_argument("--to-block", type=int)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    if not Web3.is_address(UNISWAP_V3_FACTORY_ADDRESS):
        raise ValueError(
            "UNISWAP_V3_FACTORY_ADDRESS must be a valid Ethereum address"
        )

    rpc = UniswapV3FactoryRpcClient(get_alchemy_rpc_url())
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the configured Alchemy RPC")

    spark = create_spark_session("BuildUniswapV3PoolReference")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    pool_path = DATA_LAKE.get("reference_uniswap_v3_pools")
    watermark_path = DATA_LAKE.get("state_uniswap_v3_pools")
    store = PoolMetadataStore(
        spark,
        pool_path=pool_path,
        watermark_path=watermark_path,
    )
    try:
        existing = store.load_records()
        watermark = store.load_watermark()
        start_block = next_start_block(
            UNISWAP_V3_FACTORY_START_BLOCK,
            watermark,
            args.from_block,
        )
        end_block = (
            rpc.get_latest_block_number()
            if args.to_block is None
            else args.to_block
        )
        if end_block < start_block:
            logger.info(
                "No new blocks to process: start=%s end=%s watermark=%s",
                start_block,
                end_block,
                watermark,
            )
            merged = existing
            events_retrieved = 0
            duplicate_events = 0
            chunks_processed = 0
        else:
            result = extract_pool_metadata(
                rpc,
                chain=CHAIN,
                factory_address=UNISWAP_V3_FACTORY_ADDRESS,
                start_block=start_block,
                end_block=end_block,
                chunk_size=args.chunk_size,
            )
            merged = merge_pool_records(existing, result.records)
            newly_added = len(merged) - len(existing)
            overlap_duplicates = len(result.records) - newly_added
            if merged != existing:
                store.write_records(merged)
            processed_at = datetime.now(UTC)
            new_watermark = max(watermark or 0, end_block)
            store.write_watermark(new_watermark, processed_at)
            events_retrieved = result.events_retrieved
            duplicate_events = (
                result.duplicate_events + overlap_duplicates
            )
            chunks_processed = result.chunks_processed

        observed = observed_swap_pools(
            spark,
            DATA_LAKE.get("silver_uniswap_swaps"),
        )
        coverage = reconcile_pool_coverage(observed, merged)
        logger.info(
            "Pool metadata range=%s-%s chunks=%s retrieved=%s unique_total=%s "
            "duplicates=%s output=%s",
            start_block,
            end_block,
            chunks_processed,
            events_retrieved,
            len(merged),
            duplicate_events,
            pool_path,
        )
        logger.info(
            "Swap-pool coverage observed=%s mapped=%s missing=%s coverage=%.2f%%",
            coverage.observed_pools,
            coverage.mapped_pools,
            len(coverage.missing_pools),
            coverage.coverage_percentage,
        )
        for address in coverage.missing_pools:
            logger.warning("Missing Swap pool metadata: %s", address)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
