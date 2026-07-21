from datetime import datetime

import duckdb

from config.metadata import (
    BRONZE_METADATA_FIELDS,
    BRONZE_OUTPUT_METADATA_FIELDS,
    GOLD_METADATA_FIELDS,
    GOLD_OUTPUT_METADATA_FIELDS,
    PRODUCER_METADATA_FIELDS,
    SILVER_METADATA_FIELDS,
    SILVER_OUTPUT_METADATA_FIELDS,
)
from config.versions import (
    GOLD_JOB_VERSION,
    PRODUCER_VERSION,
    SCHEMA_VERSION,
    SILVER_JOB_VERSION,
)
from producer.event_handlers import SwapEventHandler
from spark import build_swaps_silver
from spark.build_swaps_gold import (
    build_pipeline_summary,
    build_recent_swaps,
    build_swaps_per_minute,
    build_top_pools,
)


def _write_bronze_fixture(output_path) -> None:
    connection = duckdb.connect()
    try:
        connection.sql(
            f"""
            COPY (
                SELECT
                    'uniswap_v3'::VARCHAR AS protocol,
                    'arbitrum'::VARCHAR AS chain,
                    DATE '2026-07-20' AS event_date,
                    'swap'::VARCHAR AS event_type,
                    123::INTEGER AS block_number,
                    '0xabc'::VARCHAR AS transaction_hash,
                    '0xpool'::VARCHAR AS pool_address,
                    2::INTEGER AS log_index,
                    '0xdata'::VARCHAR AS raw_data,
                    ['0xtopic']::VARCHAR[] AS raw_topics,
                    TIMESTAMP '2026-07-20 00:00:02' AS kafka_timestamp,
                    '{PRODUCER_VERSION}'::VARCHAR AS producer_version,
                    '{SCHEMA_VERSION}'::VARCHAR AS schema_version,
                    TIMESTAMP '2026-07-20 00:00:01' AS ingested_at,
                    TIMESTAMP '2026-07-20 00:00:03' AS bronze_processed_at,
                    'data/bronze/swaps/protocol=uniswap_v3/'
                        'chain=arbitrum/event_type=swap/'
                        'event_date=2026-07-20'::VARCHAR AS bronze_file
            ) TO '{output_path}' (FORMAT PARQUET)
            """
        )
    finally:
        connection.close()


def _read_one(path) -> dict:
    connection = duckdb.connect()
    try:
        relation = connection.read_parquet(str(path))
        columns = relation.columns
        projections = [
            f'CAST("{column}" AS VARCHAR) AS "{column}"'
            if "TIME ZONE" in str(column_type)
            else f'"{column}"'
            for column, column_type in zip(
                columns,
                relation.types,
                strict=True,
            )
        ]
        values = relation.project(", ".join(projections)).fetchone()
        return dict(zip(columns, values, strict=True))
    finally:
        connection.close()


def test_producer_generates_versions_and_utc_ingestion_timestamp(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        SwapEventHandler,
        "decode_payload",
        lambda _handler, _log: {"pool_address": "0xpool"},
    )
    event = SwapEventHandler().build_event(
        {
            "blockNumber": "0x1",
            "transactionHash": "0xabc",
            "logIndex": "0x0",
        },
        "2026-07-20T00:00:00+00:00",
    ).to_dict()

    assert event["producer_version"] == PRODUCER_VERSION
    assert event["schema_version"] == SCHEMA_VERSION
    assert datetime.fromisoformat(event["ingested_at"]).utcoffset().total_seconds() == 0


def test_metadata_contract_lists_each_pipeline_stage() -> None:
    assert PRODUCER_METADATA_FIELDS == (
        "producer_version",
        "schema_version",
        "ingested_at",
    )
    assert BRONZE_METADATA_FIELDS == ("bronze_processed_at", "bronze_file")
    assert SILVER_METADATA_FIELDS == (
        "silver_processed_at",
        "silver_job_version",
    )
    assert GOLD_METADATA_FIELDS == ("gold_processed_at", "aggregation_version")
    assert set(PRODUCER_METADATA_FIELDS) <= set(BRONZE_OUTPUT_METADATA_FIELDS)
    assert set(BRONZE_OUTPUT_METADATA_FIELDS) <= set(
        SILVER_OUTPUT_METADATA_FIELDS
    )
    assert set(SILVER_OUTPUT_METADATA_FIELDS) <= set(GOLD_OUTPUT_METADATA_FIELDS)


def test_metadata_propagates_from_bronze_through_silver_and_gold(
    tmp_path,
    monkeypatch,
) -> None:
    bronze_file = tmp_path / "bronze.parquet"
    silver_dir = tmp_path / "silver"
    silver_file = silver_dir / "swaps_silver.parquet"
    _write_bronze_fixture(bronze_file)

    monkeypatch.setattr(build_swaps_silver, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(build_swaps_silver, "SILVER_OUTPUT_FILE", silver_file)
    build_swaps_silver.merge_silver_swaps([bronze_file])

    silver = _read_one(silver_file)
    assert silver["producer_version"] == PRODUCER_VERSION
    assert silver["schema_version"] == SCHEMA_VERSION
    assert silver["bronze_processed_at"] is not None
    assert "event_date=2026-07-20" in silver["bronze_file"]
    assert silver["silver_processed_at"] is not None
    assert silver["silver_job_version"] == SILVER_JOB_VERSION

    gold_outputs = {
        "per_minute": tmp_path / "gold" / "per_minute.parquet",
        "top_pools": tmp_path / "gold" / "top_pools.parquet",
        "summary": tmp_path / "gold" / "summary.parquet",
        "recent": tmp_path / "gold" / "recent.parquet",
    }
    build_swaps_per_minute(silver_file, gold_outputs["per_minute"])
    build_top_pools(silver_file, gold_outputs["top_pools"])
    build_pipeline_summary(silver_file, gold_outputs["summary"])
    build_recent_swaps(silver_file, gold_outputs["recent"])

    for output_path in gold_outputs.values():
        gold = _read_one(output_path)
        assert gold["producer_version"] == PRODUCER_VERSION
        assert gold["schema_version"] == SCHEMA_VERSION
        assert gold["bronze_processed_at"] is not None
        assert gold["silver_processed_at"] is not None
        assert gold["silver_job_version"] == SILVER_JOB_VERSION
        assert gold["gold_processed_at"] is not None
        assert gold["aggregation_version"] == GOLD_JOB_VERSION
