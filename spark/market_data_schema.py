"""Spark schema for exact Chainlink market-data observations."""

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_market_data_observation_schema() -> StructType:
    """Return the Kafka message schema emitted by the market-data poller."""
    return StructType(
        [
            StructField("observation_id", StringType()),
            StructField("protocol", StringType()),
            StructField("event_type", StringType()),
            StructField("chain", StringType()),
            StructField("feed_address", StringType()),
            StructField("base_asset", StringType()),
            StructField("quote_asset", StringType()),
            # Exact Chainlink integers remain decimal strings.
            StructField("round_id", StringType()),
            StructField("answer_raw", StringType()),
            StructField("feed_decimals", IntegerType()),
            StructField("feed_updated_at", TimestampType()),
            StructField("observed_at", TimestampType()),
            StructField("block_number", LongType()),
            StructField("block_timestamp", TimestampType()),
            StructField("ingested_at", TimestampType()),
            StructField("producer_version", StringType()),
            StructField("schema_version", StringType()),
        ]
    )
