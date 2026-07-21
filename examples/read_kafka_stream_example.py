"""Print normalized blockchain events from the configured Kafka topic."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json

from config.logging import configure_logging
from config.settings import KAFKA_TOPIC
from spark.event_schema import get_event_schema
from spark.kafka_stream import create_spark_session, read_kafka_stream

logger = logging.getLogger(__name__)


def parse_events(raw_df: DataFrame) -> DataFrame:
    """Parse Kafka JSON values using the shared event envelope schema."""
    return (
        raw_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), get_event_schema()).alias("event"))
        .select("event.*")
    )


def main() -> None:
    configure_logging()
    spark = create_spark_session("ReadBlockchainEventStream")
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Reading Kafka topic: %s", KAFKA_TOPIC)

    query = (
        parse_events(read_kafka_stream(spark))
        .writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
