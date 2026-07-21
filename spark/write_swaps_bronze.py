import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    concat,
    current_timestamp,
    from_json,
    lit,
    to_date,
)

from config.logging import configure_logging
from config.metadata import PRODUCER_VERSION_FIELDS
from config.settings import BRONZE_OUTPUT_PATH, SPARK_CHECKPOINT_PATH
from spark.event_schema import get_event_schema
from spark.kafka_stream import create_spark_session, read_kafka_stream

logger = logging.getLogger(__name__)


def build_bronze_df(raw_df: DataFrame) -> DataFrame:
    schema = get_event_schema()

    return (
        raw_df.select(
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("json_value"),
        )
        .select(
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
            "kafka_key",
            "json_value",
            from_json(col("json_value"), schema).alias("event"),
        )
        .select(
            "kafka_timestamp",
            to_date(
                coalesce(col("event.block_timestamp"), col("kafka_timestamp"))
            ).alias("event_date"),
            "kafka_partition",
            "kafka_offset",
            "kafka_key",
            "json_value",
            col("event.protocol").alias("protocol"),
            col("event.chain").alias("chain"),
            col("event.event_type").alias("event_type"),
            col("event.block_number").alias("block_number"),
            col("event.transaction_hash").alias("transaction_hash"),
            col("event.log_index").alias("log_index"),
            col("event.block_timestamp").alias("block_timestamp"),
            # Keep producer provenance unchanged in the Bronze record.
            *(
                col(f"event.{field}").alias(field)
                for field in PRODUCER_VERSION_FIELDS
            ),
            col("event.payload").alias("payload"),
            col("event.payload.pool_address").alias("pool_address"),
            col("event.payload.raw_data").alias("raw_data"),
            col("event.payload.raw_topics").alias("raw_topics"),
            coalesce(col("event.ingested_at"), current_timestamp()).alias(
                "ingested_at"
            ),
            # Records when Spark parsed and accepted the Kafka message.
            current_timestamp().alias("bronze_processed_at"),
            # Spark assigns part filenames during the write, so retain the
            # deterministic output partition path for physical traceability.
            concat(
                lit(f"{BRONZE_OUTPUT_PATH.as_posix()}/protocol="),
                col("event.protocol"),
                lit("/chain="),
                col("event.chain"),
                lit("/event_type="),
                col("event.event_type"),
                lit("/event_date="),
                to_date(
                    coalesce(
                        col("event.block_timestamp"), col("kafka_timestamp")
                    )
                ).cast("string"),
            ).alias("bronze_file"),
        )
    )


def write_bronze_stream(bronze_df: DataFrame) -> None:
    BRONZE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    SPARK_CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)

    logger.info("Writing Bronze blockchain events to: %s", BRONZE_OUTPUT_PATH)
    logger.info("Using checkpoint path: %s", SPARK_CHECKPOINT_PATH)

    query = (
        bronze_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", str(BRONZE_OUTPUT_PATH))
        .option("checkpointLocation", str(SPARK_CHECKPOINT_PATH))
        .partitionBy("protocol", "chain", "event_type", "event_date")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


def main() -> None:
    configure_logging()
    spark = create_spark_session("WriteBlockchainEventsBronze")
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_kafka_stream(spark)
    bronze_df = build_bronze_df(raw_df)
    write_bronze_stream(bronze_df)


if __name__ == "__main__":
    main()
