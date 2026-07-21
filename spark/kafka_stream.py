"""Shared Spark session and Kafka stream construction."""

from pyspark.sql import DataFrame, SparkSession

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    SPARK_KAFKA_CONNECTOR_PACKAGE,
)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session with the configured Kafka connector."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", SPARK_KAFKA_CONNECTOR_PACKAGE)
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Subscribe to the configured Kafka event topic from latest offsets."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
