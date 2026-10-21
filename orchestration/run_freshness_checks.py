"""Compute production freshness without mutating data or control state."""

from __future__ import annotations

import argparse
import json
from contextlib import closing
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from backfill.aave_v3 import AaveV3RpcClient
from backfill.chainlink import ChainlinkHistoricalRpcClient
from config.settings import (
    CHAIN,
    CHAINLINK_FEEDS_CONFIG,
    get_aave_backfill_rpc_url,
    get_aave_confirmation_depth,
    get_alchemy_rpc_url,
    get_chainlink_confirmation_depth,
    get_uniswap_confirmation_depth,
)
from market_data.feeds import load_chainlink_feeds
from orchestration.aave_forward import calculate_safe_head
from orchestration.freshness import (
    FeedFreshnessSnapshot,
    FreshnessReport,
    FreshnessState,
    SourceFreshnessSnapshot,
    build_freshness_report,
)
from orchestration.watermark import (
    AAVE_FORWARD_WATERMARK,
    CHAINLINK_FORWARD_WATERMARK,
    UNISWAP_FORWARD_WATERMARK,
    chainlink_feed_watermark,
)
from warehouse.freshness import (
    DBT_ROLE,
    PIPELINE_ROLE,
    FreshnessWarehouseReader,
    connect_read_only,
)
from warehouse.watermarks import SnowflakeWatermarkStore


def _required_state(store: SnowflakeWatermarkStore, identity: Any):
    state = store.get(identity)
    if state is None:
        raise RuntimeError(f"Required watermark is missing: {identity.pipeline_name}")
    return state


def collect_freshness(*, checked_at: datetime | None = None) -> FreshnessReport:
    """Read live RPC/Snowflake state and evaluate it without writes."""
    now = (checked_at or datetime.now(UTC)).astimezone(UTC)
    feeds = load_chainlink_feeds(CHAINLINK_FEEDS_CONFIG, expected_chain=CHAIN)

    aave_rpc = AaveV3RpcClient(get_aave_backfill_rpc_url())
    chainlink_rpc = ChainlinkHistoricalRpcClient(
        get_alchemy_rpc_url(),
        min_request_interval_seconds=0,
    )
    # Both event pipelines use the same Arbitrum block-height domain.  One
    # bounded-history RPC read provides a consistent head without requiring an
    # optional recovery-provider setting or altering either runtime path.
    event_source_head = aave_rpc.get_latest_block_number()
    chainlink_head = chainlink_rpc.get_latest_block()
    chainlink_safe = calculate_safe_head(
        chainlink_head.number,
        get_chainlink_confirmation_depth(),
    )

    with closing(connect_read_only(PIPELINE_ROLE)) as pipeline_connection, closing(
        connect_read_only(DBT_ROLE)
    ) as analyst_connection:
        watermarks = SnowflakeWatermarkStore(pipeline_connection)
        event_times = FreshnessWarehouseReader(
            pipeline_connection,
            analyst_connection,
        )
        aave_state = _required_state(watermarks, AAVE_FORWARD_WATERMARK)
        uniswap_state = _required_state(watermarks, UNISWAP_FORWARD_WATERMARK)
        chainlink_state = _required_state(watermarks, CHAINLINK_FORWARD_WATERMARK)
        retained_times = event_times.latest_event_timestamps()
        feed_times = event_times.chainlink_feed_times()
        warehouse_refreshed_at = event_times.latest_warehouse_refresh()
        mart_times = event_times.mart_represented_times()

        if warehouse_refreshed_at is None:
            raise RuntimeError("No successful daily warehouse run is available")
        if any(value is None for value in (*retained_times.values(), *mart_times.values())):
            raise RuntimeError("Freshness checks require non-empty canonical datasets")

        feed_snapshots = []
        for feed in feeds:
            name = feed.name.upper()
            updated_at = feed_times.get(name)
            if updated_at is None:
                raise RuntimeError(f"No canonical observation exists for {name}")
            source_round = chainlink_rpc.get_round_at_block(
                feed.feed_address,
                chainlink_safe,
            )
            cursor = _required_state(
                watermarks,
                chainlink_feed_watermark(feed.feed_address),
            )
            feed_snapshots.append(
                FeedFreshnessSnapshot(
                    feed_name=name,
                    feed_address=feed.feed_address.lower(),
                    committed_round_id=cursor.committed_value,
                    source_round_id=source_round.round_id,
                    latest_feed_updated_at=updated_at,
                )
            )

        return build_freshness_report(
            checked_at=now,
            aave=SourceFreshnessSnapshot(
                "aave_forward",
                event_source_head,
                calculate_safe_head(
                    event_source_head,
                    get_aave_confirmation_depth(),
                ),
                aave_state.committed_value,
                retained_times["aave_forward"],
            ),
            uniswap=SourceFreshnessSnapshot(
                "uniswap_forward",
                event_source_head,
                calculate_safe_head(
                    event_source_head,
                    get_uniswap_confirmation_depth(),
                ),
                uniswap_state.committed_value,
                retained_times["uniswap_forward"],
            ),
            chainlink_source_head=chainlink_head.number,
            chainlink_safe_head=chainlink_safe,
            chainlink_committed_position=chainlink_state.committed_value,
            chainlink_feeds=tuple(feed_snapshots),
            warehouse_refreshed_at=warehouse_refreshed_at,
            mart_represented_times=mart_times,
        )


def _json_report(report: FreshnessReport) -> str:
    payload = {
        "checked_at": report.checked_at.isoformat(),
        "overall_state": report.overall_state.value,
        "results": [asdict(result) for result in report.results],
    }
    return json.dumps(payload, default=str, sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fail-on-stale",
        action="store_true",
        help="return a failing exit status when any independent check is stale",
    )
    args = parser.parse_args()
    report = collect_freshness()
    print(_json_report(report))
    if args.fail_on_stale and report.overall_state is FreshnessState.STALE:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
