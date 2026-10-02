"""Bootstrap Uniswap V3 metadata for pools observed in Swap Silver."""

from __future__ import annotations

import logging

from web3 import Web3

from config.logging import configure_logging
from config.settings import (
    CHAIN,
    DATA_LAKE,
    UNISWAP_V3_FACTORY_ADDRESS,
    get_alchemy_rpc_url,
)
from reference_data.storage import PoolMetadataStore, observed_swap_pools
from reference_data.uniswap_v3_pools import (
    UniswapV3FactoryRpcClient,
    bootstrap_observed_pools,
    merge_pool_records,
    reconcile_pool_coverage,
)
from spark.session import create_spark_session

logger = logging.getLogger(__name__)


def main() -> None:
    """Verify observed pools at one block and update the canonical registry."""
    configure_logging()
    if not Web3.is_address(UNISWAP_V3_FACTORY_ADDRESS):
        raise ValueError(
            "UNISWAP_V3_FACTORY_ADDRESS must be a valid Ethereum address"
        )

    rpc = UniswapV3FactoryRpcClient(get_alchemy_rpc_url())
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the configured Alchemy RPC")

    spark = create_spark_session("BootstrapObservedUniswapV3Pools")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    pool_path = DATA_LAKE.get("reference_uniswap_v3_pools")
    quarantine_path = DATA_LAKE.get("quarantine_uniswap_v3_pools")
    store = PoolMetadataStore(
        spark,
        pool_path=pool_path,
        watermark_path=DATA_LAKE.get("state_uniswap_v3_pools"),
        quarantine_path=quarantine_path,
    )
    try:
        observed = observed_swap_pools(
            spark,
            DATA_LAKE.get("silver_uniswap_swaps"),
        )
        result = bootstrap_observed_pools(
            rpc,
            observed,
            chain=CHAIN,
            expected_factory=UNISWAP_V3_FACTORY_ADDRESS,
        )
        existing = store.load_records()
        merged = merge_pool_records(existing, result.records)
        registry_changed = merged != existing
        if registry_changed:
            store.write_records(merged)
        store.write_issues(result.issues)

        coverage = reconcile_pool_coverage(observed, merged)
        official_addresses = {record.pool_address for record in result.records}
        mapped_addresses = {record.pool_address for record in merged}
        official_mapped = len(official_addresses & mapped_addresses)
        official_missing = tuple(sorted(official_addresses - mapped_addresses))
        official_coverage = (
            100.0
            if not official_addresses
            else official_mapped * 100.0 / len(official_addresses)
        )
        logger.info(
            "Observed-pool bootstrap block=%s observed=%s official_verified=%s "
            "incompatible=%s successfully_mapped=%s missing=%s "
            "coverage=%.2f%% registry_total=%s registry_changed=%s "
            "output=%s quarantine=%s",
            result.checked_block,
            len(observed),
            len(result.records),
            len(result.issues),
            official_mapped,
            len(official_missing),
            official_coverage,
            len(merged),
            registry_changed,
            pool_path,
            quarantine_path,
        )
        logger.info(
            "All-observed coverage mapped=%s missing=%s coverage=%.2f%%",
            coverage.mapped_pools,
            len(coverage.missing_pools),
            coverage.coverage_percentage,
        )
        for issue in result.issues:
            logger.warning(
                "Rejected observed pool %s: %s",
                issue.pool_address,
                issue.reason,
            )
        for address in official_missing:
            logger.warning("Verified pool missing from registry: %s", address)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
