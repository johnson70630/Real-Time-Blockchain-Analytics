import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    SPARK_KAFKA_CONNECTOR_PACKAGE,
)
from spark.event_schema import get_event_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ReadKafkaSwapStream")
        .config("spark.jars.packages", SPARK_KAFKA_CONNECTOR_PACKAGE)
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )


def parse_events(raw_df: DataFrame) -> DataFrame:
    schema = get_event_schema()

    return (
        raw_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("event"))
        .select("event.*")
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading Kafka topic: %s", KAFKA_TOPIC)

    raw_df = read_kafka_stream(spark)
    parsed_df = parse_events(raw_df)

    query = (
        parsed_df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
