from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = PROJECT_ROOT / "dbt"


def _read(relative_path: str) -> str:
    return (DBT_ROOT / relative_path).read_text()


def test_aave_staging_models_preserve_raw_amounts_and_event_grain() -> None:
    models = {
        "stg_aave_borrows.sql": ("amount_raw", "borrow_rate_raw", "reserve"),
        "stg_aave_repays.sql": ("amount_raw", "reserve", "repayer"),
        "stg_aave_liquidations.sql": (
            "debt_to_cover_raw",
            "liquidated_collateral_amount_raw",
            "collateral_asset",
            "debt_asset",
        ),
    }

    for name, required_fields in models.items():
        sql = _read(f"models/staging/aave/{name}")
        assert "event_id::varchar as event_id" in sql
        assert "block_timestamp::timestamp_tz" in sql
        assert "source_file::varchar" in sql
        for field in required_fields:
            assert f"{field}::varchar as {field}" in sql


def test_chainlink_staging_preserves_exact_observation_values() -> None:
    sql = _read("models/staging/market_data/stg_chainlink_prices.sql")

    assert "observation_id::varchar as observation_id" in sql
    assert "round_id::varchar as round_id" in sql
    assert "answer_raw::varchar as answer_raw" in sql
    assert "price::number(38, 18) as price" in sql


def test_sources_register_aave_and_chainlink_raw_tables() -> None:
    source_path = "models/staging/uniswap/_uniswap__sources.yml"
    source_config = yaml.safe_load(_read(source_path))
    tables = {
        table["name"]: table["identifier"]
        for table in source_config["sources"][0]["tables"]
    }

    assert tables["aave_borrows"] == "AAVE_BORROWS"
    assert tables["aave_repays"] == "AAVE_REPAYS"
    assert tables["aave_liquidations"] == "AAVE_LIQUIDATIONS"
    assert tables["chainlink_prices"] == "CHAINLINK_PRICES"


def test_aave_token_coverage_test_uses_all_asset_columns() -> None:
    sql = _read("tests/assert_aave_assets_resolve.sql")

    for field in ("reserve", "collateral_asset", "debt_asset"):
        assert field in sql
    assert "ref('stg_tokens')" in sql
    assert "tokens.token_address is null" in sql
