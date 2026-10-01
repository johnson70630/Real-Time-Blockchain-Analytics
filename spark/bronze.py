"""Shared mechanics for Kafka-backed Bronze ingestion jobs."""

from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, concat, lit

KAFKA_RECORD_COLUMNS = (
    "source_topic",
    "kafka_timestamp",
    "kafka_partition",
    "kafka_offset",
    "kafka_key",
    "json_value",
)


def select_kafka_records(raw_df: DataFrame) -> DataFrame:
    """Project Kafka source metadata and retain the exact message JSON."""
    return raw_df.select(
        col("topic").alias("source_topic"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value"),
    )


def hive_partition_path(
    output_root: str | Path,
    partitions: tuple[tuple[str, Any], ...],
) -> str | Path:
    """Return a deterministic Hive partition path for concrete values."""
    path = str(output_root).rstrip("/")
    for name, value in partitions:
        path = f"{path}/{name}={value}"
    return Path(path) if isinstance(output_root, Path) else path


def hive_partition_path_column(
    output_root: str | Path,
    partitions: tuple[tuple[str, Column], ...],
) -> Column:
    """Build a Spark expression for a deterministic Hive partition path."""
    parts: list[Column] = [lit(str(output_root).rstrip("/"))]
    for name, value in partitions:
        parts.extend((lit(f"/{name}="), value.cast("string")))
    return concat(*parts)


def append_partitioned_parquet(
    frame: DataFrame,
    output_path: str | Path,
    partition_columns: tuple[str, ...],
) -> None:
    """Append a frame to a partitioned Parquet dataset."""
    (
        frame.write.mode("append")
        .partitionBy(*partition_columns)
        .parquet(str(output_path))
    )


def ensure_directories(*paths: str | Path) -> None:
    """Create storage and checkpoint directories used by a Bronze job."""
    for path in paths:
        value = str(path)
        if not value.startswith("s3a://"):
            Path(value).mkdir(parents=True, exist_ok=True)
