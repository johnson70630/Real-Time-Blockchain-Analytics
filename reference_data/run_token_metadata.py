"""Build the shared ERC-20 token metadata registry from observed datasets."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from config.logging import configure_logging
from config.settings import CHAIN, DATA_LAKE, get_alchemy_rpc_url
from reference_data.storage import (
    TokenMetadataStore,
    observed_token_source_values,
)
from reference_data.token_metadata import (
    Erc20MetadataRpcClient,
    TokenMetadataIssue,
    bootstrap_token_metadata,
    discover_token_addresses,
    merge_token_records,
)
from spark.session import create_spark_session

logger = logging.getLogger(__name__)

AAVE_TOKEN_SOURCES = (
    ("silver_aave_borrows", ("reserve",)),
    ("silver_aave_repays", ("reserve",)),
    (
        "silver_aave_liquidations",
        ("collateral_asset", "debt_asset"),
    ),
    ("enriched_aave_borrows", ("reserve",)),
    ("enriched_aave_repays", ("reserve",)),
    (
        "enriched_aave_liquidations",
        ("collateral_asset", "debt_asset"),
    ),
)


def _issue_signature(issue: TokenMetadataIssue) -> tuple[str, str]:
    return issue.token_address, issue.reason


def main() -> None:
    """Discover observed tokens, query metadata, and reconcile the registry."""
    configure_logging()
    rpc = Erc20MetadataRpcClient(get_alchemy_rpc_url())
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the configured Alchemy RPC")

    spark = create_spark_session("BuildTokenReference")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    registry_path = DATA_LAKE.get("reference_tokens")
    quarantine_path = DATA_LAKE.get("quarantine_tokens")
    store = TokenMetadataStore(
        spark,
        registry_path=registry_path,
        quarantine_path=quarantine_path,
    )
    try:
        uniswap_values, aave_values = observed_token_source_values(
            spark,
            pool_registry_path=DATA_LAKE.get("reference_uniswap_v3_pools"),
            aave_sources=tuple(
                (DATA_LAKE.get(dataset), columns)
                for dataset, columns in AAVE_TOKEN_SOURCES
            ),
        )
        discovery = discover_token_addresses(uniswap_values, aave_values)
        result = bootstrap_token_metadata(
            rpc,
            discovery.all_addresses,
            chain=CHAIN,
        )
        discovery_issues = tuple(
            TokenMetadataIssue(
                token_address=address,
                reason=reason,
                metadata_block_number=result.metadata_block_number,
                processed_at=datetime.now(UTC),
            )
            for address, reason in discovery.invalid_addresses
        )
        issues = tuple(
            sorted(
                (*result.issues, *discovery_issues),
                key=lambda issue: issue.token_address,
            )
        )

        existing = store.load_records()
        merged = merge_token_records(existing, result.records)
        registry_changed = merged != existing
        if registry_changed:
            store.write_records(merged)

        previous_issues = store.load_issues()
        quarantine_changed = {
            _issue_signature(issue) for issue in previous_issues
        } != {_issue_signature(issue) for issue in issues}
        if quarantine_changed:
            store.write_issues(issues)

        required = set(discovery.all_addresses)
        mapped = {
            record.token_address
            for record in merged
            if record.chain == CHAIN.lower()
        }
        successfully_mapped = required & mapped
        unresolved = tuple(sorted(required - mapped))
        coverage = (
            100.0
            if not required
            else len(successfully_mapped) * 100.0 / len(required)
        )
        logger.info(
            "Token metadata block=%s uniswap=%s aave=%s overlap=%s total=%s "
            "mapped=%s unresolved=%s coverage=%.2f%% registry_changed=%s "
            "quarantine_changed=%s output=%s quarantine=%s",
            result.metadata_block_number,
            len(discovery.uniswap_addresses),
            len(discovery.aave_addresses),
            len(discovery.overlap),
            len(required),
            len(successfully_mapped),
            len(unresolved),
            coverage,
            registry_changed,
            quarantine_changed,
            registry_path,
            quarantine_path,
        )
        issue_by_address = {
            issue.token_address: issue.reason for issue in issues
        }
        for address in unresolved:
            logger.warning(
                "Unresolved token %s: %s",
                address,
                issue_by_address.get(address, "metadata unavailable"),
            )
        for asset in discovery.native_assets:
            logger.warning("Native-asset semantic excluded: %s", asset)
        for record in sorted(
            (record for record in merged if record.token_address in required),
            key=lambda record: record.token_address,
        )[:5]:
            logger.info(
                "Token example address=%s symbol=%s name=%s decimals=%s",
                record.token_address,
                record.symbol,
                record.name,
                record.decimals,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
