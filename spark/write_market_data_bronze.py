"""Land raw market-data observations and malformed records in Bronze storage."""

import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    array_compact,
    col,
    concat,
    current_timestamp,
    from_json,
    lit,
    lower,
    size,
    to_date,
    trim,
    when,
)

from config.logging import configure_logging
from config.settings import (
    MARKET_DATA_BRONZE_CHECKPOINT_PATH,
    MARKET_DATA_BRONZE_OUTPUT_PATH,
    MARKET_DATA_KAFKA_TOPIC,
    MARKET_DATA_QUARANTINE_PATH,
)
from market_data.validation import REQUIRED_MARKET_DATA_FIELDS
from spark.kafka_stream import create_spark_session, read_kafka_stream
from spark.market_data_schema import get_market_data_observation_schema
from spark.bronze import (
    append_partitioned_parquet,
    ensure_directories,
    hive_partition_path_column,
    select_kafka_records,
)

logger = logging.getLogger(__name__)


def _missing_or_empty(field: str):
    value = col(f"observation.{field}")
    return value.isNull() | (
        (value.cast("string").isNotNull())
        & (trim(value.cast("string")) == "")
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
            array_compact(
                array(
                    *(
                        when(
                            _missing_or_empty(field),
                            lit(f"missing_or_empty:{field}"),
                        )
                        for field in REQUIRED_MARKET_DATA_FIELDS
                    ),
                    when(
                        col("observation.protocol") != "chainlink",
                        lit("invalid:protocol"),
                    ),
                    when(
                        col("observation.event_type") != "price_update",
                        lit("invalid:event_type"),
                    ),
                    when(
                        ~col("observation.feed_address").rlike(
                            r"^0x[0-9a-fA-F]{40}$"
                        ),
                        lit("invalid:feed_address"),
                    ),
                    when(
                        ~col("observation.round_id").rlike(r"^[0-9]+$"),
                        lit("invalid:round_id"),
                    ),
                    when(
                        ~col("observation.answer_raw").rlike(r"^-?[0-9]+$"),
                        lit("invalid:answer_raw"),
                    ),
                    when(
                        ~col("observation.feed_decimals").between(0, 255),
                        lit("invalid:feed_decimals"),
                    ),
                    when(
                        col("observation.block_number") < 0,
                        lit("invalid:block_number"),
                    ),
                    when(
                        col("observation.observation_id")
                        != concat(
                            col("observation.chain"),
                            lit(":"),
                            lower(col("observation.feed_address")),
                            lit(":"),
                            col("observation.round_id"),
                        ),
                        lit("invalid:observation_id"),
                    ),
                )
            ),
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
    bronze_path: Path = MARKET_DATA_BRONZE_OUTPUT_PATH,
    quarantine_path: Path = MARKET_DATA_QUARANTINE_PATH,
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
