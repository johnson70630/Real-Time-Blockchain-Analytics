from pathlib import Path

DBT_ROOT = Path(__file__).resolve().parents[1] / "dbt"


def _read(relative_path: str) -> str:
    return (DBT_ROOT / relative_path).read_text()


def test_reference_sources_map_to_canonical_raw_tables() -> None:
    sources = _read("models/staging/uniswap/_uniswap__sources.yml")

    assert "identifier: UNISWAP_V3_POOLS" in sources
    assert "identifier: TOKENS" in sources


def test_pool_staging_preserves_typed_reference_fields() -> None:
    sql = _read("models/staging/reference/stg_uniswap_v3_pools.sql")

    for field in (
        "pool_address",
        "token0_address",
        "token1_address",
        "fee_tier",
        "tick_spacing",
        "factory_address",
        "metadata_source",
        "factory_verified",
        "created_block",
        "created_transaction_hash",
        "created_log_index",
        "created_block_timestamp",
        "processed_at",
    ):
        assert field in sql
    assert "partition by chain, protocol, pool_address" in sql


def test_token_staging_preserves_typed_reference_fields() -> None:
    sql = _read("models/staging/reference/stg_tokens.sql")

    for field in (
        "token_address",
        "symbol",
        "name",
        "decimals",
        "metadata_source",
        "metadata_block_number",
        "processed_at",
    ):
        assert field in sql
    assert "partition by chain, token_address" in sql


def test_cross_reference_test_reports_exact_missing_token_addresses() -> None:
    sql = _read("tests/assert_uniswap_pool_tokens_resolve.sql")

    assert "token0_address as token_address" in sql
    assert "token1_address as token_address" in sql
    assert "where tokens.token_address is null" in sql
