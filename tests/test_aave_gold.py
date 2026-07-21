from datetime import date, datetime, timedelta

import duckdb
import pytest

from config.versions import GOLD_JOB_VERSION
from spark.build_aave_gold import AaveGoldPaths, build_aave_gold
from tests import data_quality_check

COMMON_SCHEMA = """
    protocol VARCHAR,
    chain VARCHAR,
    event_date DATE,
    block_timestamp TIMESTAMP,
    transaction_hash VARCHAR,
    block_number INTEGER,
    log_index INTEGER,
    kafka_timestamp TIMESTAMP,
    ingested_at TIMESTAMP,
    producer_version VARCHAR,
    schema_version VARCHAR,
    bronze_processed_at TIMESTAMP,
    bronze_file VARCHAR,
    silver_processed_at TIMESTAMP,
    silver_job_version VARCHAR
"""
BORROW_SCHEMA = """
    reserve VARCHAR,
    user VARCHAR,
    amount_raw VARCHAR,
    interest_rate_mode INTEGER
"""
REPAY_SCHEMA = """
    reserve VARCHAR,
    user VARCHAR,
    repayer VARCHAR,
    amount_raw VARCHAR,
    use_atokens BOOLEAN
"""
LIQUIDATION_SCHEMA = """
    collateral_asset VARCHAR,
    debt_asset VARCHAR,
    user VARCHAR,
    debt_to_cover_raw VARCHAR,
    liquidated_collateral_amount_raw VARCHAR,
    liquidator VARCHAR,
    receive_atoken BOOLEAN
"""
LARGE_AMOUNT = 2**255 + 12345


def _paths(tmp_path) -> AaveGoldPaths:
    return AaveGoldPaths(
        borrow_input=tmp_path / "silver" / "borrow.parquet",
        repay_input=tmp_path / "silver" / "repay.parquet",
        liquidation_input=tmp_path / "silver" / "liquidation.parquet",
        daily_borrow_output=tmp_path / "gold" / "borrow.parquet",
        daily_repayment_output=tmp_path / "gold" / "repay.parquet",
        daily_liquidation_output=tmp_path / "gold" / "liquidation.parquet",
        daily_summary_output=tmp_path / "gold" / "summary.parquet",
    )


def _metadata(
    transaction_hash: str,
    log_index: int,
    kafka_offset_seconds: int = 2,
) -> tuple:
    event_time = datetime(2026, 7, 21, 0, 5)
    return (
        "aave_v3",
        "arbitrum",
        date(2026, 7, 20),  # Gold must derive July 21 from block_timestamp.
        event_time,
        transaction_hash,
        100,
        log_index,
        event_time + timedelta(seconds=kafka_offset_seconds),
        event_time + timedelta(seconds=1),
        "1.0.0",
        "1.0.0",
        event_time + timedelta(seconds=3),
        "data/bronze/aave",
        event_time + timedelta(seconds=4),
        "1.0.0",
    )


def _write_parquet(path, event_schema: str, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.sql(
            f"CREATE TABLE events ({COMMON_SCHEMA}, {event_schema})"
        )
        if rows:
            column_count = len(rows[0])
            placeholders = ", ".join("?" for _ in range(column_count))
            connection.executemany(
                f"INSERT INTO events VALUES ({placeholders})",
                rows,
            )
        connection.sql(f"COPY events TO '{path}' (FORMAT PARQUET)")
    finally:
        connection.close()


def _write_silver_fixtures(paths: AaveGoldPaths) -> None:
    borrow_rows = [
        (*_metadata("0xborrow1", 1, 2), "0xreservea", "0xuser1", str(LARGE_AMOUNT), 2),
        # Older duplicate must not affect counts or amounts.
        (*_metadata("0xborrow1", 1, 0), "0xreservea", "0xuser1", "999", 1),
        (*_metadata("0xborrow2", 2), "0xreservea", "0xuser2", "7", 1),
        (*_metadata("0xborrow3", 3), "0xreserveb", "0xuser1", "5", 2),
    ]
    repay_rows = [
        (*_metadata("0xrepay1", 1), "0xreservea", "0xuser1", "0xrepayerx", "10", True),
        (*_metadata("0xrepay2", 2), "0xreservea", "0xuser2", "0xrepayerx", "20", False),
        (*_metadata("0xrepay3", 3), "0xreserveb", "0xuser1", "0xrepayery", "30", True),
    ]
    liquidation_rows = [
        (*_metadata("0xliq1", 1), "0xcolla", "0xdebta", "0xuser1", "100", "200", "0xliquidx", True),
        (*_metadata("0xliq2", 2), "0xcolla", "0xdebta", "0xuser2", "50", "70", "0xliquidy", False),
        (*_metadata("0xliq3", 3), "0xcollb", "0xdebta", "0xuser1", "9", "11", "0xliquidx", True),
    ]
    _write_parquet(paths.borrow_input, BORROW_SCHEMA, borrow_rows)
    _write_parquet(paths.repay_input, REPAY_SCHEMA, repay_rows)
    _write_parquet(
        paths.liquidation_input,
        LIQUIDATION_SCHEMA,
        liquidation_rows,
    )


def _rows(path, order_by: str) -> list[dict]:
    connection = duckdb.connect()
    try:
        relation = connection.sql(
            f"SELECT * FROM read_parquet('{path}') ORDER BY {order_by}"
        )
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
        values = relation.project(", ".join(projections)).fetchall()
        return [dict(zip(columns, row, strict=True)) for row in values]
    finally:
        connection.close()


def test_daily_borrow_aggregation_is_exact_and_deduplicated(tmp_path) -> None:
    paths = _paths(tmp_path)
    _write_silver_fixtures(paths)

    counts = build_aave_gold(paths)
    rows = _rows(paths.daily_borrow_output, "reserve_address")

    assert counts["daily_borrow_activity"] == 2
    assert [row["reserve_address"] for row in rows] == [
        "0xreservea",
        "0xreserveb",
    ]
    assert rows[0]["event_date"] == date(2026, 7, 21)
    assert rows[0]["borrow_event_count"] == 2
    assert rows[0]["unique_borrowers"] == 2
    assert rows[0]["total_borrow_amount_raw"] == str(LARGE_AMOUNT + 7)
    assert rows[0]["variable_rate_borrow_count"] == 1
    assert rows[0]["stable_rate_borrow_count"] == 1
    assert rows[1]["total_borrow_amount_raw"] == "5"


def test_repayment_and_liquidation_aggregations(tmp_path) -> None:
    paths = _paths(tmp_path)
    _write_silver_fixtures(paths)
    build_aave_gold(paths)

    repayments = _rows(paths.daily_repayment_output, "reserve_address")
    liquidations = _rows(
        paths.daily_liquidation_output,
        "collateral_asset, debt_asset",
    )

    assert repayments[0]["repay_event_count"] == 2
    assert repayments[0]["unique_borrowers_repaid"] == 2
    assert repayments[0]["unique_repayers"] == 1
    assert repayments[0]["total_repaid_amount_raw"] == "30"
    assert repayments[0]["atoken_repay_count"] == 1
    assert liquidations[0]["liquidation_count"] == 2
    assert liquidations[0]["unique_liquidated_users"] == 2
    assert liquidations[0]["unique_liquidators"] == 2
    assert liquidations[0]["total_debt_covered_raw"] == "150"
    assert liquidations[0]["total_collateral_liquidated_raw"] == "270"
    assert liquidations[0]["receive_atoken_count"] == 1
    assert len(liquidations) == 2


def test_daily_summary_and_gold_metadata(tmp_path) -> None:
    paths = _paths(tmp_path)
    _write_silver_fixtures(paths)
    build_aave_gold(paths)

    summary = _rows(paths.daily_summary_output, "event_date")[0]

    assert summary["protocol"] == "aave_v3"
    assert summary["chain"] == "arbitrum"
    assert summary["borrow_event_count"] == 3
    assert summary["repay_event_count"] == 3
    assert summary["liquidation_count"] == 3
    assert summary["unique_borrowers"] == 2
    assert summary["unique_repayers"] == 2
    assert summary["unique_liquidated_users"] == 2
    assert summary["unique_liquidators"] == 2
    assert summary["producer_version"] == "1.0.0"
    assert summary["schema_version"] == "1.0.0"
    assert summary["silver_job_version"] == "1.0.0"
    assert summary["gold_processed_at"] is not None
    assert summary["aggregation_version"] == GOLD_JOB_VERSION
    assert "total_borrow_amount_raw" not in summary


def test_empty_and_missing_input_behavior(tmp_path) -> None:
    paths = _paths(tmp_path)

    assert build_aave_gold(paths) == {}

    _write_parquet(paths.borrow_input, BORROW_SCHEMA, [])
    with pytest.raises(FileNotFoundError, match="missing inputs"):
        build_aave_gold(paths)

    _write_parquet(paths.repay_input, REPAY_SCHEMA, [])
    _write_parquet(paths.liquidation_input, LIQUIDATION_SCHEMA, [])
    counts = build_aave_gold(paths)

    assert counts == {
        "daily_borrow_activity": 0,
        "daily_repayment_activity": 0,
        "daily_liquidation_activity": 0,
        "daily_lending_summary": 0,
    }
    assert all(
        output.exists()
        for output in (
            paths.daily_borrow_output,
            paths.daily_repayment_output,
            paths.daily_liquidation_output,
            paths.daily_summary_output,
        )
    )


def test_aave_gold_quality_checks_pass_for_valid_outputs(
    tmp_path,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)
    _write_silver_fixtures(paths)
    build_aave_gold(paths)
    replacements = {
        "AAVE_DAILY_BORROW_ACTIVITY_FILE": paths.daily_borrow_output,
        "AAVE_DAILY_REPAYMENT_ACTIVITY_FILE": paths.daily_repayment_output,
        "AAVE_DAILY_LIQUIDATION_ACTIVITY_FILE": paths.daily_liquidation_output,
        "AAVE_DAILY_LENDING_SUMMARY_FILE": paths.daily_summary_output,
    }
    for name, path in replacements.items():
        monkeypatch.setattr(data_quality_check, name, path)

    data_quality_check.run_aave_gold_checks()
