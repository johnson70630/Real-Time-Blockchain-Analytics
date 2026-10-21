"""Read-only adapters for bounded source replay reconciliation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from contextlib import closing
from datetime import UTC, datetime
from typing import Any

from backfill.aave_v3 import AaveV3RpcClient
from backfill.chainlink import (
    ChainlinkHistoricalRpcClient,
    discover_round_ranges,
    extract_historical_rounds,
)
from backfill.run_aave_incremental import _fetch_increment
from backfill.uniswap_v3 import (
    UniswapSourceContract,
    UniswapV3RecoveryRpcClient,
    decode_recovery_logs,
    fetch_complete_range_adaptive,
    merge_events,
)
from config.settings import (
    CHAIN,
    CHAINLINK_FEEDS_CONFIG,
    DATA_LAKE,
    get_aave_backfill_rpc_url,
    get_aave_v3_pool_address,
    get_alchemy_rpc_url,
    get_blockchain_recovery_rpc_url,
    get_uniswap_recovery_address_batch_size,
    get_uniswap_recovery_chunk_size,
    get_uniswap_recovery_max_range_attempts,
    get_uniswap_recovery_min_range_size,
)
from market_data.feeds import load_chainlink_feeds
from market_data.models import MarketDataObservation
from orchestration.correction_replay import (
    ReplayRecord,
    ReplayRequest,
    SourceSnapshot,
)
from orchestration.watermark import (
    AAVE_FORWARD_WATERMARK,
    CHAINLINK_FORWARD_WATERMARK,
    UNISWAP_FORWARD_WATERMARK,
    chainlink_feed_watermark,
)
from producer.models import EventEnvelope
from reference_data.storage import path_exists
from spark.session import create_spark_session
from warehouse.watermarks import SnowflakeWatermarkStore

MAX_AUDIT_BLOCKS = 5_000


def _fingerprint(values: Mapping[str, Any]) -> str:
    payload = json.dumps(values, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def _normalized_topics(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [value]
    if value is None:
        return ()
    return tuple(str(item).lower() for item in value)


def _canonical_timestamp(value: Any) -> str:
    timestamp = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value))
    )
    if timestamp.tzinfo is None:
        # Parquet source timestamps are UTC wall times but Snowflake VARIANT
        # exposes them without an offset. Restore the source convention rather
        # than applying the client session timezone.
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC).isoformat()


def event_replay_record(event: EventEnvelope) -> ReplayRecord:
    """Project one decoded event onto stable source business content."""
    address = event.payload.get("pool_address") or event.payload.get(
        "contract_address"
    )
    comparable = {
        "protocol": event.protocol.lower(),
        "chain": event.chain.lower(),
        "event_type": event.event_type.lower(),
        "block_number": event.block_number,
        "transaction_hash": event.transaction_hash.lower(),
        "log_index": event.log_index,
        "address": str(address or "").lower(),
        "raw_data": str(event.payload.get("raw_data") or "").lower(),
        "raw_topics": _normalized_topics(event.payload.get("raw_topics")),
    }
    return ReplayRecord(
        identity=(
            f"{event.chain.lower()}:{event.transaction_hash.lower()}:"
            f"{event.log_index}"
        ),
        block_number=event.block_number,
        content_fingerprint=_fingerprint(comparable),
    )


def observation_replay_record(
    observation: MarketDataObservation,
    *,
    replay_block: int,
) -> ReplayRecord:
    """Project one proxy round without mutable polling provenance."""
    comparable = {
        "protocol": observation.protocol.lower(),
        "chain": observation.chain.lower(),
        "feed_address": observation.feed_address.lower(),
        "base_asset": observation.base_asset.upper(),
        "quote_asset": observation.quote_asset.upper(),
        "round_id": str(observation.round_id),
        "answer_raw": str(observation.answer_raw),
        "feed_decimals": observation.feed_decimals,
        "feed_updated_at": _canonical_timestamp(observation.feed_updated_at),
    }
    return ReplayRecord(
        identity=observation.observation_id.lower(),
        block_number=replay_block,
        content_fingerprint=_fingerprint(comparable),
    )


def _stored_event_record(row: tuple[Any, ...]) -> ReplayRecord:
    (
        chain,
        event_type,
        block_number,
        transaction_hash,
        log_index,
        address,
        raw_data,
        raw_topics,
        protocol,
    ) = row
    comparable = {
        "protocol": str(protocol).lower(),
        "chain": str(chain).lower(),
        "event_type": str(event_type).lower(),
        "block_number": int(block_number),
        "transaction_hash": str(transaction_hash).lower(),
        "log_index": int(log_index),
        "address": str(address or "").lower(),
        "raw_data": str(raw_data or "").lower(),
        "raw_topics": _normalized_topics(raw_topics),
    }
    return ReplayRecord(
        identity=(
            f"{str(chain).lower()}:{str(transaction_hash).lower()}:"
            f"{int(log_index)}"
        ),
        block_number=int(block_number),
        content_fingerprint=_fingerprint(comparable),
    )


def read_production_watermarks(connection: Any) -> dict[str, int]:
    """Read every production cursor used to prove replay isolation."""
    store = SnowflakeWatermarkStore(connection)
    feeds = load_chainlink_feeds(CHAINLINK_FEEDS_CONFIG, expected_chain=CHAIN)
    identities = (
        AAVE_FORWARD_WATERMARK,
        UNISWAP_FORWARD_WATERMARK,
        CHAINLINK_FORWARD_WATERMARK,
        *(chainlink_feed_watermark(feed.feed_address) for feed in feeds),
    )
    result: dict[str, int] = {}
    for identity in identities:
        state = store.get(identity)
        if state is None:
            raise RuntimeError(
                f"Production watermark is missing: {identity.pipeline_name}/"
                f"{identity.source_name}"
            )
        key = f"{identity.pipeline_name}/{identity.source_name}"
        result[key] = state.committed_value
    return result


def load_stored_records(
    connection: Any,
    request: ReplayRequest,
    fresh: SourceSnapshot,
) -> tuple[ReplayRecord, ...]:
    """Load retained RAW identities for exactly the audited source scope."""
    if request.source == "aave":
        query = """
            SELECT chain, event_type, block_number, transaction_hash, log_index,
                   contract_address, raw_data, TO_JSON(raw_topics), protocol
            FROM BLOCKCHAIN_ANALYTICS.RAW.AAVE_BORROWS
            WHERE block_number BETWEEN %s AND %s
            UNION ALL
            SELECT chain, event_type, block_number, transaction_hash, log_index,
                   contract_address, raw_data, TO_JSON(raw_topics), protocol
            FROM BLOCKCHAIN_ANALYTICS.RAW.AAVE_REPAYS
            WHERE block_number BETWEEN %s AND %s
            UNION ALL
            SELECT chain, event_type, block_number, transaction_hash, log_index,
                   contract_address, raw_data, TO_JSON(raw_topics), protocol
            FROM BLOCKCHAIN_ANALYTICS.RAW.AAVE_LIQUIDATIONS
            WHERE block_number BETWEEN %s AND %s
        """
        parameters = (request.start_block, request.end_block) * 3
        with closing(connection.cursor()) as cursor:
            cursor.execute(query, parameters)
            return tuple(_stored_event_record(row) for row in cursor.fetchall())
    if request.source == "uniswap":
        return _load_uniswap_bronze_records(request)

    identities = tuple(record.identity for record in fresh.records)
    if not identities:
        return ()
    rows: list[tuple[Any, ...]] = []
    with closing(connection.cursor()) as cursor:
        for offset in range(0, len(identities), 500):
            batch = identities[offset : offset + 500]
            placeholders = ", ".join("%s" for _ in batch)
            cursor.execute(
                f"""
                SELECT observation_id, protocol, chain, feed_address,
                       base_asset, quote_asset, round_id, answer_raw,
                       feed_decimals, record:feed_updated_at::VARCHAR
                FROM BLOCKCHAIN_ANALYTICS.RAW.CHAINLINK_PRICES
                WHERE LOWER(observation_id) IN ({placeholders})
                """,
                batch,
            )
            rows.extend(cursor.fetchall())
    return tuple(
        ReplayRecord(
            identity=str(row[0]).lower(),
            block_number=request.end_block,
            content_fingerprint=_fingerprint(
                {
                    "protocol": str(row[1]).lower(),
                    "chain": str(row[2]).lower(),
                    "feed_address": str(row[3]).lower(),
                    "base_asset": str(row[4]).upper(),
                    "quote_asset": str(row[5]).upper(),
                    "round_id": str(row[6]),
                    "answer_raw": str(row[7]),
                    "feed_decimals": int(row[8]),
                    "feed_updated_at": _canonical_timestamp(row[9]),
                }
            ),
        )
        for row in rows
    )


def _load_uniswap_bronze_records(
    request: ReplayRequest,
) -> tuple[ReplayRecord, ...]:
    """Read Swap, Mint, and Burn identities retained by live/recovery Bronze."""
    spark = create_spark_session("AuditUniswapCorrectionReplay")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    try:
        frames = []
        live_path = DATA_LAKE.get("bronze_events")
        if path_exists(spark, live_path):
            frames.append(
                spark.read.option("mergeSchema", "true")
                .option("basePath", live_path)
                .parquet(live_path)
            )
        recovery_path = DATA_LAKE.get("bronze_uniswap_recovery")
        if path_exists(spark, recovery_path):
            frames.append(
                spark.read.option("mergeSchema", "true")
                .option("recursiveFileLookup", "true")
                .parquet(recovery_path)
            )
        if not frames:
            return ()
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame, allowMissingColumns=True)
        rows = (
            combined.filter(
                (combined.protocol == "uniswap_v3")
                & (combined.block_number >= request.start_block)
                & (combined.block_number <= request.end_block)
            )
            .select("json_value")
            .collect()
        )
        records = tuple(
            event_replay_record(EventEnvelope(**json.loads(row.json_value)))
            for row in rows
        )
        # Identical copies may legitimately exist across live and recovery
        # storage. Conflicting copies remain visible to reconciliation.
        return tuple(sorted(set(records), key=lambda item: item.identity))
    finally:
        spark.stop()


def _validate_audit_size(request: ReplayRequest) -> None:
    if request.block_count > MAX_AUDIT_BLOCKS:
        raise ValueError(
            f"correction audit is limited to {MAX_AUDIT_BLOCKS} blocks per run"
        )


def fetch_fresh_records(
    connection: Any,
    request: ReplayRequest,
) -> SourceSnapshot:
    """Fetch a complete bounded snapshot through existing protocol decoders."""
    _validate_audit_size(request)
    if request.source == "aave":
        rpc = AaveV3RpcClient(get_aave_backfill_rpc_url())
        if not rpc.is_connected():
            raise ConnectionError("Unable to connect to the Aave replay RPC")
        processed_at = datetime.now(UTC)
        _leaves, events, issues, _logical, _attempts, _splits = _fetch_increment(
            rpc,
            pool_address=get_aave_v3_pool_address(),
            start_block=request.start_block,
            end_block=request.end_block,
            processed_at=processed_at,
        )
        return SourceSnapshot(
            tuple(event_replay_record(event) for event in events),
            complete=not issues,
            request_count=rpc.request_count,
        )
    if request.source == "uniswap":
        with closing(connection.cursor()) as cursor:
            cursor.execute(
                """
                SELECT LOWER(pool_address)
                FROM BLOCKCHAIN_ANALYTICS.RAW.UNISWAP_V3_POOLS
                WHERE factory_verified = TRUE
                ORDER BY LOWER(pool_address)
                """
            )
            pools = tuple(str(row[0]) for row in cursor.fetchall())
        contract = UniswapSourceContract(pools)
        rpc = UniswapV3RecoveryRpcClient(get_blockchain_recovery_rpc_url())
        if not rpc.is_connected():
            raise ConnectionError("Unable to connect to the Uniswap replay RPC")
        processed_at = datetime.now(UTC)
        next_block = request.start_block
        events: tuple[EventEnvelope, ...] = ()
        issue_count = 0
        while next_block <= request.end_block:
            parent_end = min(
                request.end_block,
                next_block + get_uniswap_recovery_chunk_size() - 1,
            )
            fetched = fetch_complete_range_adaptive(
                rpc,
                contract=contract,
                start_block=next_block,
                end_block=parent_end,
                address_batch_size=get_uniswap_recovery_address_batch_size(),
                minimum_range_size=get_uniswap_recovery_min_range_size(),
                max_range_attempts=get_uniswap_recovery_max_range_attempts(),
            )
            decoded, issues = decode_recovery_logs(
                rpc,
                logs=fetched.logs,
                contract=contract,
                chain=CHAIN,
                start_block=next_block,
                end_block=parent_end,
                processed_at=processed_at,
            )
            events = merge_events(events, decoded)
            issue_count += len(issues)
            next_block = parent_end + 1
        return SourceSnapshot(
            tuple(event_replay_record(event) for event in events),
            complete=issue_count == 0,
            request_count=rpc.request_count,
        )

    feeds = load_chainlink_feeds(CHAINLINK_FEEDS_CONFIG, expected_chain=CHAIN)
    rpc = ChainlinkHistoricalRpcClient(
        get_alchemy_rpc_url(),
        min_request_interval_seconds=0,
    )
    if not rpc.is_connected():
        raise ConnectionError("Unable to connect to the Chainlink replay RPC")
    ranges = tuple(
        item
        for feed in feeds
        for item in discover_round_ranges(
            rpc,
            feed,
            start_block=request.start_block,
            end_block=request.end_block,
        )
    )
    decimals = {
        feed.feed_address.lower(): rpc.get_feed_decimals(feed.feed_address)
        for feed in feeds
    }
    snapshot = rpc.get_block_snapshot(request.end_block)
    round_count = sum(
        item.end_round_id - item.start_round_id + 1 for item in ranges
    )
    result = extract_historical_rounds(
        rpc,
        ranges=ranges,
        feed_decimals=decimals,
        snapshot=snapshot,
        max_rpc_requests=max(1, round_count),
        max_workers=2,
        rpc_batch_size=20,
    )
    return SourceSnapshot(
        tuple(
            observation_replay_record(
                observation,
                replay_block=request.end_block,
            )
            for observation in result.observations
        ),
        complete=result.complete and not result.issues,
        request_count=rpc.request_count,
    )
