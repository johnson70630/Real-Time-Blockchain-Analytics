"""Shared Spark session and Kafka stream construction."""

from pyspark.sql import DataFrame, SparkSession

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)
from spark.session import create_spark_session as _create_spark_session


def create_spark_session(app_name: str) -> SparkSession:
    """Create the shared Spark session with Kafka support enabled."""
    return _create_spark_session(app_name, include_kafka=True)


def read_kafka_stream(
    spark: SparkSession,
    topic: str = KAFKA_TOPIC,
) -> DataFrame:
    """Subscribe to a configured Kafka topic from latest offsets."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
