from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config.versions import SILVER_JOB_VERSION
from spark.build_market_data_silver import (
    PRICE_PRECISION,
    PRICE_SCALE,
    build_market_data_silver,
    get_market_price_schema,
    transform_market_data,
)

FEED_ADDRESS = "0x" + "AB" * 20
NORMALIZED_FEED_ADDRESS = FEED_ADDRESS.lower()
BRONZE_SCHEMA = StructType(
    [
        StructField("observation_id", StringType()),
        StructField("protocol", StringType()),
        StructField("event_type", StringType()),
        StructField("chain", StringType()),
        StructField("feed_address", StringType()),
        StructField("base_asset", StringType()),
        StructField("quote_asset", StringType()),
        StructField("round_id", StringType()),
        StructField("answer_raw", StringType()),
        StructField("feed_decimals", IntegerType()),
        StructField("feed_updated_at", StringType()),
        StructField("observed_at", StringType()),
        StructField("block_number", LongType()),
        StructField("block_timestamp", StringType()),
        StructField("ingested_at", StringType()),
        StructField("producer_version", StringType()),
        StructField("schema_version", StringType()),
        StructField("source_topic", StringType()),
        StructField("kafka_timestamp", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_key", StringType()),
        StructField("json_value", StringType()),
        StructField("bronze_processed_at", StringType()),
        StructField("bronze_file", StringType()),
    ]
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TestMarketDataSilver")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _row(**overrides) -> dict:
    round_id = overrides.get("round_id", "42")
    row = {
        "observation_id": (
            f"arbitrum:{NORMALIZED_FEED_ADDRESS}:{round_id}"
        ),
        "protocol": "chainlink",
        "event_type": "price_update",
        "chain": "arbitrum",
        "feed_address": FEED_ADDRESS,
        "base_asset": "eth",
        "quote_asset": "usd",
        "round_id": round_id,
        "answer_raw": "123456789123456789",
        "feed_decimals": 8,
        "feed_updated_at": "2026-07-23T12:00:00Z",
        "observed_at": "2026-07-23T12:00:01Z",
        "block_number": 123456,
        "block_timestamp": "2026-07-23T12:00:00Z",
        "ingested_at": "2026-07-23T12:00:01Z",
        "producer_version": "1.0.0",
        "schema_version": "1.0.0",
        "source_topic": "market_data_observations",
        "kafka_timestamp": "2026-07-23T12:00:02Z",
        "kafka_partition": 0,
        "kafka_offset": 1,
        "kafka_key": "observation-key",
        "json_value": "{}",
        "bronze_processed_at": "2026-07-23T12:00:03Z",
        "bronze_file": "data/bronze/market_data/chain=arbitrum",
    }
    row.update(overrides)
    return row


def _transform(spark: SparkSession, rows: list[dict]):
    bronze = spark.createDataFrame(rows, BRONZE_SCHEMA)
    return transform_market_data(bronze)


def test_exact_decimal_price_normalization_and_lineage(
    spark: SparkSession,
) -> None:
    silver, quarantine = _transform(spark, [_row()])

    result = silver.collect()[0].asDict()
    price_field = silver.schema["price"]

    assert quarantine.count() == 0
    assert result["price"] == Decimal("1234567891.234567890000000000")
    assert price_field.dataType.typeName() == "decimal"
    assert price_field.dataType.precision == PRICE_PRECISION
    assert price_field.dataType.scale == PRICE_SCALE
    assert result["feed_address"] == NORMALIZED_FEED_ADDRESS
    assert result["base_asset"] == "ETH"
    assert result["quote_asset"] == "USD"
    assert isinstance(result["feed_updated_at"], datetime)
    assert result["answer_raw"] == "123456789123456789"
    assert result["source_topic"] == "market_data_observations"
    assert result["bronze_processed_at"] is not None
    assert result["producer_version"] == "1.0.0"
    assert result["schema_version"] == "1.0.0"
    assert result["silver_job_version"] == SILVER_JOB_VERSION
    assert result["snapshot_id"] is None


def test_snapshot_id_is_recovered_from_raw_json(spark: SparkSession) -> None:
    silver, _ = _transform(
        spark,
        [_row(json_value='{"snapshot_id":"arbitrum:123456"}')],
    )

    assert silver.collect()[0]["snapshot_id"] == "arbitrum:123456"


def test_deduplication_prefers_latest_bronze_record_deterministically(
    spark: SparkSession,
) -> None:
    older = _row(
        bronze_processed_at="2026-07-23T12:00:03Z",
        bronze_file="older.parquet",
        kafka_offset=1,
    )
    newer = _row(
        bronze_processed_at="2026-07-23T12:00:04Z",
        bronze_file="newer.parquet",
        kafka_offset=2,
    )
    repeated = [older, newer, older, newer]

    first, _ = _transform(spark, repeated)
    second, _ = _transform(spark, repeated)

    assert first.count() == 1
    assert second.count() == 1
    assert first.collect()[0]["bronze_file"] == "newer.parquet"
    assert second.collect()[0]["bronze_file"] == "newer.parquet"


def test_invalid_prices_and_decimals_are_quarantined(
    spark: SparkSession,
) -> None:
    rows = [
        _row(round_id="1", answer_raw="not-an-integer"),
        _row(round_id="2", answer_raw="0"),
        _row(round_id="3", answer_raw="-1"),
        _row(round_id="4", feed_decimals=19),
    ]
    silver, quarantine = _transform(spark, rows)
    errors_by_round = {
        row["round_id"]: set(row["validation_errors"])
        for row in quarantine.collect()
    }

    assert silver.count() == 0
    assert len(errors_by_round) == 4
    assert "invalid:answer_raw" in errors_by_round["1"]
    assert "non_positive:answer_raw" in errors_by_round["2"]
    assert "invalid:answer_raw" in errors_by_round["3"]
    assert "invalid:feed_decimals" in errors_by_round["4"]


def test_declared_schema_uses_exact_types_and_nullable_snapshot() -> None:
    schema = get_market_price_schema()

    assert schema["price"].dataType == DecimalType(
        PRICE_PRECISION,
        PRICE_SCALE,
    )
    assert schema["snapshot_id"].nullable is True
    assert schema["round_id"].dataType == StringType()
    assert schema["answer_raw"].dataType == StringType()
    assert schema["block_number"].dataType == LongType()


def test_repeated_build_is_idempotent_and_writes_date_partition(
    spark: SparkSession,
    tmp_path,
) -> None:
    bronze_root = tmp_path / "bronze" / "market_data"
    silver_path = tmp_path / "silver" / "market_prices"
    quarantine_path = tmp_path / "quarantine" / "market_data_silver"
    duplicate_rows = [
        _row(bronze_file="older.parquet"),
        _row(
            bronze_processed_at="2026-07-23T12:00:04Z",
            bronze_file="newer.parquet",
            kafka_offset=2,
        ),
    ]
    bronze = spark.createDataFrame(duplicate_rows, BRONZE_SCHEMA).withColumn(
        "observation_date",
        to_date(lit("2026-07-23")),
    )
    (
        bronze.write.mode("overwrite")
        .partitionBy("chain", "observation_date")
        .parquet(str(bronze_root))
    )

    first = build_market_data_silver(
        spark,
        bronze_root=bronze_root,
        historical_root=None,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )
    second = build_market_data_silver(
        spark,
        bronze_root=bronze_root,
        historical_root=None,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )

    assert first.bronze_records_read == 2
    assert first.duplicate_records_removed == 1
    assert first.silver_records_written == 1
    assert second == first
    assert spark.read.parquet(str(silver_path)).count() == 1
    assert (silver_path / "price_date=2026-07-23").is_dir()


def test_build_unions_live_and_historical_bronze(
    spark: SparkSession,
    tmp_path,
) -> None:
    live_root = tmp_path / "bronze" / "market_data"
    historical_root = tmp_path / "bronze_backfill" / "market_data"
    silver_path = tmp_path / "silver" / "market_prices"
    quarantine_path = tmp_path / "quarantine" / "market_data_silver"
    live = spark.createDataFrame([_row()], BRONZE_SCHEMA).withColumn(
        "observation_date", to_date(lit("2026-07-23"))
    )
    historical = spark.createDataFrame(
        [
            _row(source_topic="historical_chainlink_rpc"),
            _row(
                round_id="43",
                source_topic="historical_chainlink_rpc",
                kafka_partition=None,
                kafka_offset=None,
            ),
        ],
        BRONZE_SCHEMA,
    ).withColumn("observation_date", to_date(lit("2026-07-23")))
    live.write.mode("overwrite").partitionBy(
        "chain", "observation_date"
    ).parquet(str(live_root))
    historical.write.mode("overwrite").partitionBy(
        "chain", "observation_date"
    ).parquet(str(historical_root))

    stats = build_market_data_silver(
        spark,
        bronze_root=live_root,
        historical_root=historical_root,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )

    assert stats.bronze_records_read == 3
    assert stats.duplicate_records_removed == 1
    assert stats.silver_records_written == 2
