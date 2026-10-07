from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = PROJECT_ROOT / "dbt"
CORE_ROOT = DBT_ROOT / "models" / "core"
INTERMEDIATE_ROOT = DBT_ROOT / "models" / "intermediate" / "uniswap"


def _read(path: Path) -> str:
    return path.read_text()


def test_fact_models_are_defined_at_canonical_grains() -> None:
    config = yaml.safe_load(_read(CORE_ROOT / "_core__facts.yml"))
    models = {model["name"]: model for model in config["models"]}

    assert set(models) == {
        "fact_uniswap_swap",
        "fact_uniswap_swap_valued",
        "fact_aave_borrow",
        "fact_aave_borrow_valued",
        "fact_aave_repay",
        "fact_aave_repay_valued",
        "fact_aave_liquidation",
        "fact_aave_liquidation_valued",
        "fact_market_price",
    }
    for model in models.values():
        assert "one row per" in model["description"].lower()


def test_normalization_macro_uses_only_fixed_point_semantics() -> None:
    sql = _read(DBT_ROOT / "macros" / "normalize_token_amount.sql").lower()

    assert "try_to_decimal" in sql
    assert "38," in sql and "18" in sql
    assert "regexp_like" in sql
    assert "float" not in sql
    assert "double" not in sql
    assert "power(" not in sql
    assert "pow(" not in sql


def test_uniswap_fact_uses_pool_dimension_token_identities() -> None:
    sql = _read(CORE_ROOT / "fact_uniswap_swap.sql")

    assert "pools.pool_key" in sql
    assert "pools.token0_key" in sql
    assert "pools.token1_key" in sql
    assert "on pools.token0_key = token0.token_key" in sql
    assert "on pools.token1_key = token1.token_key" in sql
    assert "normalize_token_amount('swaps.amount0_raw'" in sql
    assert "normalize_token_amount('swaps.amount1_raw'" in sql
    assert "ref('int_uniswap_swap_classification')" in sql
    assert "where swaps.swap_classification = 'CANONICAL_UNISWAP_V3'" in sql


def test_uniswap_classification_preserves_all_staged_identifiers() -> None:
    classification_sql = _read(
        INTERMEDIATE_ROOT / "int_uniswap_swap_classification.sql"
    )
    exclusions_sql = _read(
        INTERMEDIATE_ROOT / "int_uniswap_swap_exclusions.sql"
    )

    assert "swaps.*" in classification_sql
    assert "left join {{ ref('dim_pool') }}" in classification_sql
    assert "CANONICAL_UNISWAP_V3" in classification_sql
    assert "NONCANONICAL_OR_INCOMPATIBLE" in classification_sql
    assert "not_in_verified_pool_registry" in classification_sql
    assert "select *" in exclusions_sql
    assert "NONCANONICAL_OR_INCOMPATIBLE" in exclusions_sql


def test_aave_facts_preserve_raw_amounts_and_use_token_decimals() -> None:
    expected = {
        "fact_aave_borrow.sql": ("amount_raw", "borrow_amount"),
        "fact_aave_repay.sql": ("amount_raw", "repay_amount"),
        "fact_aave_liquidation.sql": (
            "debt_to_cover_raw",
            "liquidated_collateral_amount_raw",
        ),
    }

    for filename, fields in expected.items():
        sql = _read(CORE_ROOT / filename)
        assert "normalize_token_amount" in sql
        assert "left join {{ ref('dim_token') }}" in sql
        for field in fields:
            assert field in sql


def test_market_price_date_uses_feed_effective_timestamp() -> None:
    sql = _read(CORE_ROOT / "fact_market_price.sql")

    assert "prices.feed_updated_at::date = dates.date" in sql
    assert "prices.price::number(38, 18) as price" in sql
    assert "token_key" not in sql


def test_fact_reconciliation_tests_cover_counts_dates_and_amounts() -> None:
    count_test = _read(DBT_ROOT / "tests" / "assert_fact_row_counts_reconcile.sql")
    date_test = _read(DBT_ROOT / "tests" / "assert_fact_date_keys_match_source.sql")
    amount_test = _read(
        DBT_ROOT / "tests" / "assert_fact_normalized_amounts_populated.sql"
    )

    for fact in (
        "fact_uniswap_swap",
        "fact_aave_borrow",
        "fact_aave_repay",
        "fact_aave_liquidation",
        "fact_market_price",
    ):
        assert f"ref('{fact}')" in count_test
        assert f"ref('{fact}')" in date_test
    assert "amount0 is null" in amount_test
    assert "borrow_amount is null" in amount_test
    assert "repay_amount is null" in amount_test


def test_uniswap_accounting_tests_require_complete_disjoint_populations() -> None:
    conservation = _read(
        DBT_ROOT / "tests" / "assert_uniswap_swap_conservation.sql"
    )
    accounted_once = _read(
        DBT_ROOT / "tests" / "assert_uniswap_swap_accounted_once.sql"
    )
    disjoint = _read(
        DBT_ROOT / "tests" / "assert_uniswap_swap_populations_disjoint.sql"
    )

    assert "staged_rows != canonical_rows + excluded_rows" in conservation
    assert "population_count != 1" in accounted_once
    assert "fact_uniswap_swap" in disjoint
    assert "int_uniswap_swap_exclusions" in disjoint
