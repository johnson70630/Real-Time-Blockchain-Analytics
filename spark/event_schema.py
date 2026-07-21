from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_event_schema() -> StructType:
    """Return the common Kafka envelope and event payload schema."""
    payload_schema = StructType(
        [
            StructField("pool_address", StringType()),
            StructField("sender", StringType()),
            StructField("recipient", StringType()),
            StructField("owner", StringType()),
            StructField("tick_lower", IntegerType()),
            StructField("tick_upper", IntegerType()),
            StructField("amount", StringType()),
            StructField("amount0", StringType()),
            StructField("amount1", StringType()),
            StructField("sqrt_price_x96", StringType()),
            StructField("liquidity", StringType()),
            StructField("tick", IntegerType()),
            StructField("raw_data", StringType()),
            StructField("raw_topics", ArrayType(StringType())),
        ]
    )

    return StructType(
        [
            StructField("protocol", StringType()),
            StructField("chain", StringType()),
            StructField("event_type", StringType()),
            StructField("block_number", IntegerType()),
            StructField("transaction_hash", StringType()),
            StructField("log_index", IntegerType()),
            StructField("block_timestamp", TimestampType()),
            StructField("producer_version", StringType()),
            StructField("schema_version", StringType()),
            StructField("ingested_at", TimestampType()),
            StructField("payload", payload_schema),
        ]
    )
