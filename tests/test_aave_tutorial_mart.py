from pathlib import Path

import yaml

DBT_ROOT = Path(__file__).resolve().parents[1] / "dbt"
MART_ROOT = DBT_ROOT / "models" / "marts" / "aave"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_lending_mart_uses_only_trusted_core_models() -> None:
    sql = _read(MART_ROOT / "mart_aave_lending_tutorial.sql")

    assert "ref('fact_aave_borrow_valued')" in sql
    assert "ref('fact_aave_repay_valued')" in sql
    assert "ref('dim_token')" in sql
    assert "ref('stg_aave_borrows')" not in sql
    assert "ref('stg_aave_repays')" not in sql
    assert "ref('int_defi_event_token_prices')" not in sql


def test_lending_mart_labels_sources_and_keeps_actor_roles_separate() -> None:
    sql = _read(MART_ROOT / "mart_aave_lending_tutorial.sql").lower()

    assert "'borrow' as activity_type" in sql
    assert "'repay' as activity_type" in sql
    for role in ("user", "on_behalf_of", "repayer"):
        assert role in sql
    assert "borrow_rate_raw" in sql
    assert "float" not in sql
    assert "double" not in sql


def test_lending_mart_schema_defines_unified_event_grain() -> None:
    config = yaml.safe_load(_read(MART_ROOT / "_aave__marts.yml"))
    model = config["models"][0]
    columns = {column["name"]: column for column in model["columns"]}

    assert model["name"] == "mart_aave_lending_tutorial"
    assert "one row per borrow or repay event" in model["description"].lower()
    assert columns["event_id"]["data_tests"] == ["not_null", "unique"]
    assert "activity_type" in columns
    assert "has_valid_usd_valuation" in columns
