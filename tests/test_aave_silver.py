from dataclasses import replace
from datetime import date, datetime

import duckdb

from config.versions import SILVER_JOB_VERSION
from spark.build_aave_silver import (
    AAVE_SILVER_MODELS,
    COMMON_COLUMNS,
    RAW_PAYLOAD_FIELDS,
    build_aave_silver,
)

PAYLOAD_TYPE = """
STRUCT(
    contract_address VARCHAR,
    raw_data VARCHAR,
    raw_topics VARCHAR[],
    reserve VARCHAR,
    user VARCHAR,
    on_behalf_of VARCHAR,
    amount_raw VARCHAR,
    interest_rate_mode INTEGER,
    borrow_rate_raw VARCHAR,
    referral_code INTEGER,
    repayer VARCHAR,
    use_atokens BOOLEAN,
    collateral_asset VARCHAR,
    debt_asset VARCHAR,
    debt_to_cover_raw VARCHAR,
    liquidated_collateral_amount_raw VARCHAR,
    liquidator VARCHAR,
    receive_atoken BOOLEAN
)
"""
LARGE_AMOUNT = str(2**255 + 12345)


def _payload(**overrides) -> dict:
    payload = {
        "contract_address": "0xpool",
        "raw_data": "0xdata",
        "raw_topics": ["0xtopic"],
        "reserve": None,
        "user": None,
        "on_behalf_of": None,
        "amount_raw": None,
        "interest_rate_mode": None,
        "borrow_rate_raw": None,
        "referral_code": None,
        "repayer": None,
        "use_atokens": None,
        "collateral_asset": None,
        "debt_asset": None,
        "debt_to_cover_raw": None,
        "liquidated_collateral_amount_raw": None,
        "liquidator": None,
        "receive_atoken": None,
    }
    payload.update(overrides)
    return payload


def _write_bronze_fixture(output_path) -> None:
    connection = duckdb.connect()
    try:
        connection.sql(
            f"""
            CREATE TABLE bronze_events (
                protocol VARCHAR,
                chain VARCHAR,
                event_type VARCHAR,
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
                payload {PAYLOAD_TYPE}
            )
            """
        )
        base = (
            "aave_v3",
            "arbitrum",
            None,
            date(2026, 7, 21),
            datetime(2026, 7, 21, 0, 0),
            None,
            100,
            1,
            datetime(2026, 7, 21, 0, 0, 2),
            datetime(2026, 7, 21, 0, 0, 1),
            "1.0.0",
            "1.0.0",
            datetime(2026, 7, 21, 0, 0, 3),
            "data/bronze/aave",
            None,
        )
        rows = [
            (
                *base[:2],
                "borrow",
                *base[3:5],
                "0xborrow",
                *base[6:14],
                _payload(
                    reserve="0xreserve",
                    user="0xuser",
                    on_behalf_of="0xbehalf",
                    amount_raw=LARGE_AMOUNT,
                    interest_rate_mode=2,
                    borrow_rate_raw=LARGE_AMOUNT,
                    referral_code=7,
                ),
            ),
            (
                *base[:2],
                "repay",
                *base[3:5],
                "0xrepay",
                *base[6:14],
                _payload(
                    reserve="0xreserve",
                    user="0xuser",
                    repayer="0xrepayer",
                    amount_raw=LARGE_AMOUNT,
                    use_atokens=True,
                ),
            ),
            (
                *base[:2],
                "liquidation",
                *base[3:5],
                "0xliquidation",
                *base[6:14],
                _payload(
                    collateral_asset="0xcollateral",
                    debt_asset="0xdebt",
                    user="0xuser",
                    debt_to_cover_raw=LARGE_AMOUNT,
                    liquidated_collateral_amount_raw=LARGE_AMOUNT,
                    liquidator="0xliquidator",
                    receive_atoken=False,
                ),
            ),
            (
                "uniswap_v3",
                "arbitrum",
                "borrow",
                *base[3:5],
                "0xwrongprotocol",
                *base[6:14],
                _payload(amount_raw="1"),
            ),
        ]
        connection.executemany(
            "INSERT INTO bronze_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?)",
            rows,
        )
        connection.sql(
            f"COPY bronze_events TO '{output_path}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()


def _output_models(tmp_path):
    return tuple(
        replace(model, output_path=tmp_path / model.output_path.name)
        for model in AAVE_SILVER_MODELS
    )


def _row_and_schema(path) -> tuple[dict, dict[str, str]]:
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
        schema = {
            column: str(column_type)
            for column, column_type in zip(
                columns,
                relation.types,
                strict=True,
            )
        }
        return dict(zip(columns, values, strict=True)), schema
    finally:
        connection.close()


def test_aave_silver_filters_events_and_flattens_payloads(tmp_path) -> None:
    bronze_root = tmp_path / "bronze"
    bronze_root.mkdir()
    _write_bronze_fixture(bronze_root / "events.parquet")
    models = _output_models(tmp_path / "silver")

    counts = build_aave_silver(bronze_root, models)

    assert counts == {"borrow": 1, "repay": 1, "liquidation": 1}
    rows = {
        model.event_type: _row_and_schema(model.output_path)[0]
        for model in models
    }
    assert rows["borrow"]["amount_raw"] == LARGE_AMOUNT
    assert rows["borrow"]["on_behalf_of"] == "0xbehalf"
    assert rows["repay"]["repayer"] == "0xrepayer"
    assert rows["repay"]["use_atokens"] is True
    assert rows["liquidation"]["debt_to_cover_raw"] == LARGE_AMOUNT
    assert rows["liquidation"]["receive_atoken"] is False


def test_aave_silver_preserves_metadata_and_consistent_schemas(tmp_path) -> None:
    bronze_root = tmp_path / "bronze"
    bronze_root.mkdir()
    _write_bronze_fixture(bronze_root / "events.parquet")
    models = _output_models(tmp_path / "silver")
    build_aave_silver(bronze_root, models)

    for model in models:
        row, schema = _row_and_schema(model.output_path)
        expected_columns = {
            "event_id",
            *COMMON_COLUMNS,
            *RAW_PAYLOAD_FIELDS,
            *model.payload_fields,
            "silver_processed_at",
            "silver_job_version",
        }
        assert set(schema) == expected_columns
        assert row["protocol"] == "aave_v3"
        assert row["chain"] == "arbitrum"
        assert row["transaction_hash"].startswith("0x")
        assert row["block_timestamp"] is not None
        assert row["ingested_at"] is not None
        assert row["producer_version"] == "1.0.0"
        assert row["schema_version"] == "1.0.0"
        assert row["silver_job_version"] == SILVER_JOB_VERSION
        assert row["silver_processed_at"] is not None

    _, borrow_schema = _row_and_schema(models[0].output_path)
    assert borrow_schema["amount_raw"] == "VARCHAR"
    assert borrow_schema["borrow_rate_raw"] == "VARCHAR"
    assert borrow_schema["interest_rate_mode"] == "INTEGER"
