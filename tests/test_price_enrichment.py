import json
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.settings import get_max_price_age_seconds
from config.versions import PRICE_ENRICHMENT_JOB_VERSION
from spark import build_price_enriched_silver as enrichment_job
from spark.price_enrichment import (
    AMOUNT_SCALE,
    DECIMAL_PRECISION,
    AssetPriceConfig,
    AssetPriceMapping,
    EnrichmentLeg,
    UniswapV3PoolMapping,
    create_mapping_frames,
    enrich_events,
    invalid_enrichment_condition,
    load_asset_price_config,
    monetary_columns_are_exact,
)

WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
UNKNOWN = "0x" + "99" * 20
POOL = "0x" + "11" * 20
ETH_FEED = "0x" + "aa" * 20
USDC_FEED = "0x" + "bb" * 20

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("protocol", StringType(), False),
        StructField("chain", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("block_timestamp", TimestampType(), True),
        StructField("transaction_hash", StringType(), False),
        StructField("block_number", LongType(), False),
        StructField("log_index", IntegerType(), False),
        StructField("reserve", StringType(), True),
        StructField("amount_raw", StringType(), True),
        StructField("debt_asset", StringType(), True),
        StructField("collateral_asset", StringType(), True),
        StructField("debt_to_cover_raw", StringType(), True),
        StructField("liquidated_collateral_amount_raw", StringType(), True),
        StructField("pool_address", StringType(), True),
        StructField("amount0_raw", StringType(), True),
        StructField("amount1_raw", StringType(), True),
        StructField("producer_version", StringType(), False),
    ]
)

PRICE_SCHEMA = StructType(
    [
        StructField("observation_id", StringType(), False),
        StructField("chain", StringType(), False),
        StructField("base_asset", StringType(), False),
        StructField("quote_asset", StringType(), False),
        StructField("feed_address", StringType(), False),
        StructField("round_id", StringType(), False),
        StructField("answer_raw", StringType(), False),
        StructField("feed_decimals", IntegerType(), False),
        StructField(
            "price",
            DecimalType(DECIMAL_PRECISION, AMOUNT_SCALE),
            False,
        ),
        StructField("feed_updated_at", TimestampType(), False),
        StructField("block_number", LongType(), False),
        StructField("silver_processed_at", TimestampType(), False),
    ]
)

SINGLE_LEG = (
    EnrichmentLeg(
        name="",
        raw_amount_column="amount_raw",
        source_asset_column="reserve",
    ),
)
SWAP_LEGS = (
    EnrichmentLeg(
        name="token0",
        raw_amount_column="amount0_raw",
        uniswap_pool_side="token0",
        absolute_usd_value=True,
    ),
    EnrichmentLeg(
        name="token1",
        raw_amount_column="amount1_raw",
        uniswap_pool_side="token1",
        absolute_usd_value=True,
    ),
)
LIQUIDATION_LEGS = (
    EnrichmentLeg(
        name="debt",
        raw_amount_column="debt_to_cover_raw",
        source_asset_column="debt_asset",
    ),
    EnrichmentLeg(
        name="collateral",
        raw_amount_column="liquidated_collateral_amount_raw",
        source_asset_column="collateral_asset",
    ),
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TestPriceEnrichment")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _config(*, include_pool: bool = False) -> AssetPriceConfig:
    return AssetPriceConfig(
        mappings=(
            AssetPriceMapping(
                "arbitrum",
                "WETH",
                WETH,
                "ETH",
                "USD",
                18,
            ),
            AssetPriceMapping(
                "arbitrum",
                "USDC",
                USDC,
                "USDC",
                "USD",
                6,
            ),
        ),
        uniswap_v3_pools=(
            (
                UniswapV3PoolMapping(
                    "arbitrum",
                    POOL,
                    "WETH",
                    "USDC",
                ),
            )
            if include_pool
            else ()
        ),
    )


def _event(event_id: str, timestamp: datetime | None, **overrides) -> dict:
    row = {
        "event_id": event_id,
        "protocol": "aave_v3",
        "chain": "arbitrum",
        "event_type": "borrow",
        "block_timestamp": timestamp,
        "transaction_hash": f"0x{event_id}",
        "block_number": 100,
        "log_index": 1,
        "reserve": WETH,
        "amount_raw": "1500000000000000000",
        "debt_asset": None,
        "collateral_asset": None,
        "debt_to_cover_raw": None,
        "liquidated_collateral_amount_raw": None,
        "pool_address": None,
        "amount0_raw": None,
        "amount1_raw": None,
        "producer_version": "1.0.0",
    }
    row.update(overrides)
    return row


def _price(
    observation_id: str,
    asset: str,
    timestamp: datetime,
    value: str,
    **overrides,
) -> dict:
    row = {
        "observation_id": observation_id,
        "chain": "arbitrum",
        "base_asset": asset,
        "quote_asset": "USD",
        "feed_address": ETH_FEED,
        "round_id": "1",
        "answer_raw": str(int(Decimal(value) * Decimal(10**8))),
        "feed_decimals": 8,
        "price": Decimal(value).quantize(Decimal("0.000000000000000001")),
        "feed_updated_at": timestamp,
        "block_number": 1,
        "silver_processed_at": timestamp,
    }
    row.update(overrides)
    return row


def _transform(
    spark: SparkSession,
    event_rows: list[dict],
    price_rows: list[dict],
    legs: tuple[EnrichmentLeg, ...] = SINGLE_LEG,
    *,
    include_pool: bool = False,
    max_age: int = 300,
):
    events = spark.createDataFrame(event_rows, EVENT_SCHEMA)
    prices = spark.createDataFrame(price_rows, PRICE_SCHEMA)
    mappings, pools = create_mapping_frames(
        spark,
        _config(include_pool=include_pool),
    )
    return enrich_events(
        events,
        prices,
        mappings,
        pools,
        legs,
        max_age,
        PRICE_ENRICHMENT_JOB_VERSION,
    )


def test_latest_prior_exact_match_future_exclusion_lineage_and_decimal_types(
    spark: SparkSession,
) -> None:
    event_time = datetime(2026, 7, 24, 12, 5)
    result = _transform(
        spark,
        [_event("event-1", event_time)],
        [
            _price("older", "ETH", datetime(2026, 7, 24, 12, 3), "3000"),
            _price(
                "exact",
                "ETH",
                event_time,
                "3100",
                round_id="2",
                block_number=2,
            ),
            _price("future", "ETH", datetime(2026, 7, 24, 12, 6), "9999"),
        ],
    )
    row = result.first().asDict()

    assert row["matched_observation_id"] == "exact"
    assert row["matched_feed_address"] == ETH_FEED
    assert row["matched_price_timestamp"] == event_time
    assert row["matched_price_block_number"] == 2
    assert row["price_age_seconds"] == 0
    assert row["price_enrichment_status"] == "enriched"
    assert row["normalized_amount"] == Decimal("1.500000000000000000")
    assert row["price_usd"] == Decimal("3100.000000000000000000")
    assert row["amount_usd"] == Decimal("4650.00000000")
    assert row["producer_version"] == "1.0.0"
    assert result.schema["normalized_amount"].dataType == DecimalType(38, 18)
    assert result.schema["price_usd"].dataType == DecimalType(38, 18)
    assert result.schema["amount_usd"].dataType == DecimalType(38, 8)
    assert monetary_columns_are_exact(result)


def test_unmatched_stale_chain_and_quote_cases_preserve_event_count(
    spark: SparkSession,
) -> None:
    result = _transform(
        spark,
        [
            _event("stale", datetime(2026, 7, 24, 12, 0)),
            _event("no-prior", datetime(2026, 7, 24, 10, 0)),
            _event(
                "no-mapping",
                datetime(2026, 7, 24, 12, 0),
                reserve=UNKNOWN,
            ),
            _event(
                "wrong-quote",
                datetime(2026, 7, 24, 12, 0),
                reserve=USDC,
                amount_raw="1000000",
            ),
            _event(
                "wrong-chain",
                datetime(2026, 7, 24, 12, 0),
                chain="optimism",
            ),
            _event("bad-time", None),
        ],
        [
            _price("stale-price", "ETH", datetime(2026, 7, 24, 11, 0), "3000"),
            _price(
                "eur-price",
                "USDC",
                datetime(2026, 7, 24, 11, 59),
                "1",
                quote_asset="EUR",
                feed_address=USDC_FEED,
            ),
            _price(
                "optimism-price",
                "ETH",
                datetime(2026, 7, 24, 11, 59),
                "3000",
                chain="optimism",
            ),
        ],
    )
    rows = {row["event_id"]: row.asDict() for row in result.collect()}

    assert len(rows) == 6
    assert rows["stale"]["price_enrichment_status"] == "stale_price"
    assert rows["stale"]["price_age_seconds"] == 3600
    assert rows["no-prior"]["price_enrichment_status"] == "no_prior_price"
    assert rows["no-mapping"]["price_enrichment_status"] == "no_asset_mapping"
    assert rows["wrong-quote"]["price_enrichment_status"] == "no_prior_price"
    assert rows["wrong-chain"]["price_enrichment_status"] == "no_asset_mapping"
    assert rows["bad-time"]["price_enrichment_status"] == (
        "invalid_event_timestamp"
    )
    assert all(row["amount_usd"] is None for row in rows.values())
    assert all(row["matched_observation_id"] is None for row in rows.values())


def test_duplicate_candidates_resolve_by_numeric_round_id_deterministically(
    spark: SparkSession,
) -> None:
    timestamp = datetime(2026, 7, 24, 12, 0)
    events = [_event("event-1", timestamp)]
    prices = [
        _price(
            "round-9",
            "ETH",
            timestamp,
            "3000",
            round_id="9",
            block_number=50,
        ),
        _price(
            "round-10",
            "ETH",
            timestamp,
            "3100",
            round_id="10",
            block_number=50,
        ),
    ]

    first = _transform(spark, events, prices).first().asDict()
    second = _transform(spark, events, list(reversed(prices))).first().asDict()

    assert first["matched_observation_id"] == "round-10"
    assert second["matched_observation_id"] == "round-10"
    assert first["price_usd"] == Decimal("3100.000000000000000000")


def test_decimal_precision_exposes_float_rounding_errors(
    spark: SparkSession,
) -> None:
    timestamp = datetime(2026, 7, 24, 12, 0)
    result = _transform(
        spark,
        [
            _event(
                "event-1",
                timestamp,
                reserve=USDC,
                amount_raw="1",
            )
        ],
        [
            _price(
                "usdc-price",
                "USDC",
                timestamp,
                "1.005",
                feed_address=USDC_FEED,
            )
        ],
    )
    row = result.first().asDict()

    assert row["normalized_amount"] == Decimal("0.000001000000000000")
    assert row["amount_usd"] == Decimal("0.00000101")


def test_uniswap_token_legs_enrich_independently_and_weth_maps_to_eth(
    spark: SparkSession,
) -> None:
    timestamp = datetime(2026, 7, 24, 12, 0)
    result = _transform(
        spark,
        [
            _event(
                "swap-1",
                timestamp,
                protocol="uniswap_v3",
                event_type="swap",
                reserve=None,
                amount_raw=None,
                pool_address=POOL,
                amount0_raw="-2000000000000000000",
                amount1_raw="6000000000",
            )
        ],
        [_price("eth-price", "ETH", datetime(2026, 7, 24, 11, 59), "3000")],
        SWAP_LEGS,
        include_pool=True,
    )
    row = result.first().asDict()

    assert row["token0_asset_symbol"] == "WETH"
    assert row["token0_price_asset"] == "ETH"
    assert row["token0_price_enrichment_status"] == "enriched"
    assert row["token0_normalized_amount"] == Decimal("-2.000000000000000000")
    assert row["token0_amount_usd_abs"] == Decimal("6000.00000000")
    assert row["token1_asset_symbol"] == "USDC"
    assert row["token1_price_enrichment_status"] == "no_prior_price"
    assert row["token1_amount_usd_abs"] is None


def test_aave_liquidation_legs_enrich_independently(
    spark: SparkSession,
) -> None:
    timestamp = datetime(2026, 7, 24, 12, 0)
    result = _transform(
        spark,
        [
            _event(
                "liquidation-1",
                timestamp,
                event_type="liquidation",
                reserve=None,
                amount_raw=None,
                debt_asset=USDC,
                collateral_asset=WETH,
                debt_to_cover_raw="2500000",
                liquidated_collateral_amount_raw="1000000000000000",
            )
        ],
        [
            _price(
                "usdc-price",
                "USDC",
                datetime(2026, 7, 24, 11, 59),
                "1",
                feed_address=USDC_FEED,
            )
        ],
        LIQUIDATION_LEGS,
    )
    row = result.first().asDict()

    assert row["debt_price_enrichment_status"] == "enriched"
    assert row["debt_amount_usd"] == Decimal("2.50000000")
    assert row["debt_matched_observation_id"] == "usdc-price"
    assert row["collateral_price_enrichment_status"] == "no_prior_price"
    assert row["collateral_amount_usd"] is None


def test_data_quality_accepts_unmatched_reference_data(
    spark: SparkSession,
) -> None:
    timestamp = datetime(2026, 7, 24, 12, 0)
    result = _transform(
        spark,
        [_event("event-1", timestamp, reserve=UNKNOWN)],
        [_price("eth-price", "ETH", timestamp, "3000")],
    )

    rejected = result.filter(invalid_enrichment_condition(SINGLE_LEG, 300))
    assert rejected.count() == 0
    assert result.count() == 1


def test_batch_build_is_idempotent_and_atomically_replaces_output(
    spark: SparkSession,
    tmp_path,
    monkeypatch,
) -> None:
    timestamp = datetime(2026, 7, 24, 12, 0)
    source_path = tmp_path / "silver" / "borrow_events.parquet"
    price_root = tmp_path / "silver" / "market_prices"
    output_path = tmp_path / "silver" / "enriched" / "borrow.parquet"
    mapping_path = tmp_path / "asset_price_mapping.json"
    spark.createDataFrame(
        [_event("event-1", timestamp)],
        EVENT_SCHEMA,
    ).write.mode("overwrite").parquet(str(source_path))
    spark.createDataFrame(
        [_price("eth-price", "ETH", timestamp, "3000")],
        PRICE_SCHEMA,
    ).write.mode("overwrite").parquet(str(price_root))
    mapping_path.write_text(
        json.dumps(
            {
                "mappings": [
                    {
                        "chain": "arbitrum",
                        "event_asset": "WETH",
                        "event_asset_address": WETH,
                        "price_asset": "ETH",
                        "quote_asset": "USD",
                        "token_decimals": 18,
                    }
                ],
                "uniswap_v3_pools": [],
            }
        ),
        encoding="utf-8",
    )
    model = enrichment_job.PriceEnrichmentModel(
        name="aave_v3_borrow_test",
        source_path=source_path,
        output_path=output_path,
        legs=SINGLE_LEG,
    )
    monkeypatch.setattr(
        enrichment_job,
        "PRICE_ENRICHMENT_QUARANTINE_DIR",
        tmp_path / "quarantine",
    )

    first = enrichment_job.build_price_enriched_silver(
        spark,
        models=(model,),
        market_price_root=price_root,
        mapping_path=mapping_path,
        max_price_age_seconds=300,
    )
    second = enrichment_job.build_price_enriched_silver(
        spark,
        models=(model,),
        market_price_root=price_root,
        mapping_path=mapping_path,
        max_price_age_seconds=300,
    )
    output = spark.read.parquet(str(output_path))

    assert first[model.name].source_records == 1
    assert first[model.name].records_written == 1
    assert first[model.name].rejected_records == 0
    assert second[model.name] == first[model.name]
    assert output.count() == 1
    assert output.select("event_id").first()[0] == "event-1"


def test_mapping_loader_and_max_age_validation(tmp_path, monkeypatch) -> None:
    path = tmp_path / "mapping.json"
    path.write_text(
        json.dumps(
            {
                "mappings": [
                    {
                        "chain": "Arbitrum",
                        "event_asset": "weth",
                        "event_asset_address": WETH.upper().replace("0X", "0x"),
                        "price_asset": "eth",
                        "quote_asset": "usd",
                        "token_decimals": 18,
                    }
                ],
                "uniswap_v3_pools": [],
            }
        ),
        encoding="utf-8",
    )
    config = load_asset_price_config(path)

    assert config.mappings[0].chain == "arbitrum"
    assert config.mappings[0].event_asset == "WETH"
    assert config.mappings[0].price_asset == "ETH"
    monkeypatch.setenv("MAX_PRICE_AGE_SECONDS", "60")
    assert get_max_price_age_seconds() == 60
    monkeypatch.setenv("MAX_PRICE_AGE_SECONDS", "0")
    with pytest.raises(ValueError, match="greater than zero"):
        get_max_price_age_seconds()
