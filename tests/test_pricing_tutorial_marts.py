from pathlib import Path

import yaml

DBT_ROOT = Path(__file__).resolve().parents[1] / "dbt"
MART_ROOT = DBT_ROOT / "models" / "marts" / "pricing"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_oracle_mart_uses_canonical_fact_and_exact_window_ordering() -> None:
    sql = _read(MART_ROOT / "mart_chainlink_oracle_tutorial.sql").lower()

    assert "ref('fact_market_price')" in sql
    assert "partition by lower(chain), lower(feed_address)" in sql
    assert "lag(feed_updated_at)" in sql
    assert "feed_times.previous_feed_updated_at" in sql
    assert "number(38, 18)" in sql
    assert "float" not in sql
    assert "double" not in sql


def test_pit_mart_preserves_canonical_match_and_separates_next_round() -> None:
    sql = _read(MART_ROOT / "mart_point_in_time_pricing_tutorial.sql").lower()

    assert sql.count("ref('int_defi_event_token_prices')") == 2
    assert "pit.price_usd as valuation_price_usd" in sql
    assert "observations.feed_updated_at > pit.event_timestamp" in sql
    assert "next_price_occurs_after_event" in sql
    assert "ref('stg_chainlink_prices')" not in sql
    assert "float" not in sql
    assert "double" not in sql


def test_pricing_mart_schema_defines_both_tutorial_grains() -> None:
    config = yaml.safe_load(_read(MART_ROOT / "_pricing__marts.yml"))
    models = {model["name"]: model for model in config["models"]}

    assert set(models) == {
        "mart_chainlink_oracle_tutorial",
        "mart_point_in_time_pricing_tutorial",
    }
    assert "one row per observation" in models[
        "mart_chainlink_oracle_tutorial"
    ]["description"].lower()
    assert "one row per canonical event-token side" in models[
        "mart_point_in_time_pricing_tutorial"
    ]["description"].lower()
