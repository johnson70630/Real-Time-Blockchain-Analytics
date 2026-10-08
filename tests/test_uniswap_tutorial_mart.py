from pathlib import Path

import yaml

DBT_ROOT = Path(__file__).resolve().parents[1] / "dbt"
MART_ROOT = DBT_ROOT / "models" / "marts" / "uniswap"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_tutorial_mart_uses_trusted_core_models() -> None:
    sql = _read(MART_ROOT / "mart_uniswap_swap_tutorial.sql")

    assert "ref('fact_uniswap_swap_valued')" in sql
    assert "ref('dim_pool')" in sql
    assert sql.count("ref('dim_token')") == 2
    assert "ref('stg_uniswap_swaps')" not in sql
    assert "ref('int_defi_event_token_prices')" not in sql


def test_tutorial_mart_preserves_pit_statuses_and_fixed_point_values() -> None:
    sql = _read(MART_ROOT / "mart_uniswap_swap_tutorial.sql").lower()

    for field in (
        "token0_price_status",
        "token1_price_status",
        "token0_price_usd",
        "token1_price_usd",
        "amount0_usd",
        "amount1_usd",
    ):
        assert field in sql
    assert "float" not in sql
    assert "double" not in sql


def test_tutorial_mart_schema_defines_canonical_event_grain() -> None:
    config = yaml.safe_load(_read(MART_ROOT / "_uniswap__marts.yml"))
    model = config["models"][0]
    columns = {column["name"]: column for column in model["columns"]}

    assert model["name"] == "mart_uniswap_swap_tutorial"
    assert "one row per event" in model["description"].lower()
    assert columns["event_id"]["data_tests"] == ["not_null", "unique"]
    assert "pool_pair" in columns
    assert "pricing_coverage_status" in columns
