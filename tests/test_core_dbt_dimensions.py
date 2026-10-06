from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = PROJECT_ROOT / "dbt"
CORE_ROOT = DBT_ROOT / "models" / "core"


def _read(path: Path) -> str:
    return path.read_text()


def test_core_dimensions_are_defined_at_documented_grains() -> None:
    config = yaml.safe_load(_read(CORE_ROOT / "_core__models.yml"))
    models = {model["name"]: model for model in config["models"]}

    assert set(models) == {
        "dim_date",
        "dim_chain",
        "dim_protocol",
        "dim_token",
        "dim_pool",
    }
    for model in models.values():
        assert "one row per" in model["description"].lower()


def test_surrogate_keys_use_deterministic_natural_identities() -> None:
    chain_sql = _read(CORE_ROOT / "dim_chain.sql")
    protocol_sql = _read(CORE_ROOT / "dim_protocol.sql")
    token_sql = _read(CORE_ROOT / "dim_token.sql")
    pool_sql = _read(CORE_ROOT / "dim_pool.sql")

    assert "sha2(lower(trim(chain)), 256) as chain_key" in chain_sql
    assert "sha2(lower(trim(protocol)), 256) as protocol_key" in protocol_sql
    assert "lower(tokens.chain), lower(tokens.token_address)" in token_sql
    for identity in ("lower(pools.chain)", "lower(pools.protocol)", "lower(pools.pool_address)"):
        assert identity in pool_sql
    for sql in (chain_sql, protocol_sql, token_sql, pool_sql):
        assert "sequence" not in sql.lower()
        assert "autoincrement" not in sql.lower()


def test_pool_dimension_is_v4_compatible_without_fabricating_v4_rows() -> None:
    sql = _read(CORE_ROOT / "dim_pool.sql")

    assert "pools.pool_address as pool_identifier" in sql
    assert "pools.pool_address," in sql
    assert "cast(null as varchar) as pool_id" in sql
    assert "protocols.protocol_version" in sql
    assert "token0.token_key as token0_key" in sql
    assert "token1.token_key as token1_key" in sql


def test_date_dimension_covers_observed_dates_with_extensions() -> None:
    sql = _read(CORE_ROOT / "dim_date.sql")

    assert "dateadd(year, -1" in sql
    assert "dateadd(" in sql and "year," in sql and "2," in sql
    assert "to_char(calendar_date, 'YYYYMMDD')" in sql
    assert "weekiso(calendar_date)" in sql


def test_reconciliation_tests_cover_source_counts_and_pool_resolution() -> None:
    token_test = _read(DBT_ROOT / "tests" / "assert_dim_token_reconciles_to_staging.sql")
    pool_test = _read(DBT_ROOT / "tests" / "assert_dim_pool_reconciles_to_staging.sql")
    resolution_test = _read(DBT_ROOT / "tests" / "assert_dim_pool_source_resolves_once.sql")

    assert "ref('stg_tokens')" in token_test
    assert "ref('dim_token')" in token_test
    assert "ref('stg_uniswap_v3_pools')" in pool_test
    assert "ref('dim_pool')" in pool_test
    assert "having count(dimensions.pool_key) != 1" in resolution_test
