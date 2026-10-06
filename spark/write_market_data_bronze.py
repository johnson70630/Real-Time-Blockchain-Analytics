"""Land raw market-data observations and malformed records in Bronze storage."""

import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    size,
    to_date,
    udf,
)
from pyspark.sql.types import ArrayType, StringType

from config.logging import configure_logging
from config.settings import (
    DATA_LAKE,
    MARKET_DATA_KAFKA_TOPIC,
)
from market_data.validation import validate_observation_message
from spark.kafka_stream import create_spark_session, read_kafka_stream
from spark.market_data_schema import get_market_data_observation_schema
from spark.bronze import (
    append_partitioned_parquet,
    ensure_directories,
    hive_partition_path_column,
    select_kafka_records,
)

logger = logging.getLogger(__name__)

MARKET_DATA_BRONZE_OUTPUT_PATH = DATA_LAKE.get("bronze_market_data")
MARKET_DATA_QUARANTINE_PATH = DATA_LAKE.get("quarantine_market_data")
MARKET_DATA_BRONZE_CHECKPOINT_PATH = DATA_LAKE.get(
    "checkpoint_market_data_bronze"
)


def validation_error_values(raw_value: str) -> list[str]:
    """Return canonical message errors without expanding a large Spark plan."""
    _, errors = validate_observation_message(raw_value)
    return list(errors)


_validation_errors = udf(
    validation_error_values,
    returnType=ArrayType(StringType(), containsNull=False),
)


def build_market_data_records(raw_df: DataFrame) -> DataFrame:
    """Parse Kafka observations and attach deterministic validation errors."""
    schema = get_market_data_observation_schema()
    parsed = (
        select_kafka_records(raw_df)
        .withColumn(
            "observation",
            from_json(col("json_value"), schema),
        )
        .withColumn(
            "validation_errors",
            _validation_errors(col("json_value")),
        )
    )

    observation_date = to_date(col("observation.observed_at"))
    return parsed.select(
        "source_topic",
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
        "kafka_key",
        "json_value",
        "validation_errors",
        observation_date.alias("observation_date"),
        *(
            col(f"observation.{field}").alias(field)
            for field in schema.fieldNames()
        ),
        current_timestamp().alias("bronze_processed_at"),
        hive_partition_path_column(
            MARKET_DATA_BRONZE_OUTPUT_PATH,
            (
                ("chain", col("observation.chain")),
                ("observation_date", observation_date),
            ),
        ).alias("bronze_file"),
    )


def write_market_data_batch(
    batch_df: DataFrame,
    batch_id: int,
    *,
    bronze_path: str | Path = MARKET_DATA_BRONZE_OUTPUT_PATH,
    quarantine_path: str | Path = MARKET_DATA_QUARANTINE_PATH,
) -> None:
    """Write one micro-batch to Bronze and quarantine without deduplication."""
    valid_df = batch_df.filter(size("validation_errors") == 0).drop(
        "validation_errors"
    )
    invalid_df = (
        batch_df.filter(size("validation_errors") > 0)
        .withColumn("quarantined_at", current_timestamp())
        .withColumn("quarantine_date", to_date(col("kafka_timestamp")))
    )
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    logger.info(
        "Market-data batch=%s received=%s valid=%s quarantined=%s",
        batch_id,
        valid_count + invalid_count,
        valid_count,
        invalid_count,
    )

    try:
        if valid_count:
            append_partitioned_parquet(
                valid_df,
                bronze_path,
                ("chain", "observation_date"),
            )
            logger.info(
                "Market-data batch=%s wrote=%s path=%s",
                batch_id,
                valid_count,
                bronze_path,
            )
        if invalid_count:
            append_partitioned_parquet(
                invalid_df,
                quarantine_path,
                ("quarantine_date",),
            )
            logger.warning(
                "Market-data batch=%s quarantined=%s path=%s",
                batch_id,
                invalid_count,
                quarantine_path,
            )
    except Exception:
        logger.exception("Market-data batch=%s write failed", batch_id)
        raise


def write_market_data_stream(records_df: DataFrame) -> None:
    """Start the checkpointed market-data Bronze and quarantine writer."""
    ensure_directories(
        MARKET_DATA_BRONZE_OUTPUT_PATH,
        MARKET_DATA_QUARANTINE_PATH,
        MARKET_DATA_BRONZE_CHECKPOINT_PATH,
    )

    logger.info("Reading market observations from: %s", MARKET_DATA_KAFKA_TOPIC)
    logger.info("Writing market-data Bronze to: %s", MARKET_DATA_BRONZE_OUTPUT_PATH)
    logger.info("Writing malformed observations to: %s", MARKET_DATA_QUARANTINE_PATH)

    query = (
        records_df.writeStream.outputMode("append")
        .foreachBatch(write_market_data_batch)
        .option(
            "checkpointLocation",
            str(MARKET_DATA_BRONZE_CHECKPOINT_PATH),
        )
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


def main() -> None:
    configure_logging()
    spark = create_spark_session("WriteMarketDataBronze")
    spark.sparkContext.setLogLevel("WARN")
    raw_df = read_kafka_stream(spark, MARKET_DATA_KAFKA_TOPIC)
    records_df = build_market_data_records(raw_df)
    write_market_data_stream(records_df)


if __name__ == "__main__":
    main()
