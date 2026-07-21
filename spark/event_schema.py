from pyspark.sql.types import (
    ArrayType,
    BooleanType,
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
            StructField("contract_address", StringType()),
            StructField("reserve", StringType()),
            StructField("user", StringType()),
            StructField("on_behalf_of", StringType()),
            StructField("amount_raw", StringType()),
            StructField("interest_rate_mode", IntegerType()),
            StructField("borrow_rate_raw", StringType()),
            StructField("referral_code", IntegerType()),
            StructField("repayer", StringType()),
            StructField("use_atokens", BooleanType()),
            StructField("collateral_asset", StringType()),
            StructField("debt_asset", StringType()),
            StructField("debt_to_cover_raw", StringType()),
            StructField(
                "liquidated_collateral_amount_raw",
                StringType(),
            ),
            StructField("liquidator", StringType()),
            StructField("receive_atoken", BooleanType()),
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
