import csv
import json
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = PROJECT_ROOT / "dbt"
PRICING_ROOT = DBT_ROOT / "models" / "intermediate" / "pricing"
CORE_ROOT = DBT_ROOT / "models" / "core"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_dbt_mapping_is_resolved_from_authoritative_catalogs() -> None:
    asset_config = json.loads(
        _read(PROJECT_ROOT / "config" / "asset_price_mapping.json")
    )
    feed_config = json.loads(
        _read(PROJECT_ROOT / "config" / "chainlink_feeds.json")
    )
    feeds = {
        (
            feed["chain"].lower(),
            feed["base_asset"].upper(),
            feed["quote_asset"].upper(),
        ): feed["address"].lower()
        for feed in feed_config["feeds"]
    }
    expected = {
        (
            mapping["chain"].lower(),
            mapping["event_asset_address"].lower(),
            mapping["event_asset"].upper(),
            feeds[
                (
                    mapping["chain"].lower(),
                    mapping["price_asset"].upper(),
                    mapping["quote_asset"].upper(),
                )
            ],
            mapping["price_asset"].upper(),
            mapping["quote_asset"].upper(),
        )
        for mapping in asset_config["mappings"]
        if mapping["event_asset_address"] is not None
    }
    with (DBT_ROOT / "seeds" / "token_price_feed_mapping.csv").open(
        newline="",
        encoding="utf-8",
    ) as seed_file:
        actual = {
            (
                row["chain"],
                row["token_address"],
                row["token_asset"],
                row["feed_address"],
                row["base_asset"],
                row["quote_asset"],
            )
            for row in csv.DictReader(seed_file)
        }

    assert actual == expected


def test_pit_match_uses_latest_non_future_feed_timestamp() -> None:
    sql = _read(PRICING_ROOT / "int_defi_event_token_prices.sql").lower()

    assert "prices.feed_updated_at <= event_tokens.event_timestamp" in sql
    assert "prices.feed_updated_at desc nulls last" in sql
    assert "prices.observed_at" not in sql
    assert "row_number() over" in sql
    assert "price_age_seconds" in sql


def test_pit_statuses_preserve_unmapped_and_stale_events() -> None:
    sql = _read(PRICING_ROOT / "int_defi_event_token_prices.sql").lower()

    assert "left join {{ ref('token_price_feed_mapping') }}" in sql
    assert "left join {{ ref('fact_market_price') }}" in sql
    for status in ("unmapped", "no_prior_price", "stale", "priced"):
        assert f"'{status}'" in sql
    assert "price_status = 'priced'" in sql


def test_valued_models_keep_base_fact_grain() -> None:
    expected_sources = {
        "fact_uniswap_swap_valued.sql": "fact_uniswap_swap",
        "fact_aave_borrow_valued.sql": "fact_aave_borrow",
        "fact_aave_repay_valued.sql": "fact_aave_repay",
        "fact_aave_liquidation_valued.sql": "fact_aave_liquidation",
    }

    for filename, source in expected_sources.items():
        sql = _read(CORE_ROOT / filename)
        assert f"from {{{{ ref('{source}') }}}}" in sql
        assert "left join {{ ref('int_defi_event_token_prices') }}" in sql


def test_usd_calculation_uses_fixed_point_types() -> None:
    sql = _read(DBT_ROOT / "macros" / "calculate_usd_value.sql").lower()

    assert "try_to_decimal" in sql
    assert "), 28, 18)" in sql
    assert "38," in sql and "8" in sql
    assert "float" not in sql
    assert "double" not in sql


def test_dbt_stale_threshold_matches_existing_default() -> None:
    project = yaml.safe_load(_read(DBT_ROOT / "dbt_project.yml"))

    configured = project["vars"]["max_price_age_seconds"]
    assert "MAX_PRICE_AGE_SECONDS" in configured
    assert "300" in configured
