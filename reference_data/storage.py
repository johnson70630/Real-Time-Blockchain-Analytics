"""Spark-backed local/S3 storage for canonical reference datasets."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from reference_data.uniswap_v3_pools import (
    PoolBootstrapIssue,
    UniswapV3Pool,
    merge_pool_records,
)
from spark.parquet import write_partitioned_dataset_atomic

StoragePath = str | Path

POOL_SCHEMA = StructType(
    [
        StructField("chain", StringType(), False),
        StructField("protocol", StringType(), False),
        StructField("pool_address", StringType(), False),
        StructField("token0_address", StringType(), False),
        StructField("token1_address", StringType(), False),
        StructField("fee_tier", IntegerType(), False),
        StructField("tick_spacing", IntegerType(), False),
        StructField("factory_address", StringType(), False),
        StructField("created_block", LongType(), True),
        StructField("created_transaction_hash", StringType(), True),
        StructField("created_log_index", IntegerType(), True),
        StructField("created_block_timestamp", TimestampType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("producer_version", StringType(), False),
        StructField("schema_version", StringType(), False),
        StructField("processed_at", TimestampType(), False),
        StructField("metadata_source", StringType(), False),
        StructField("factory_verified", BooleanType(), False),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("pool_address", StringType(), False),
        StructField("reason", StringType(), False),
        StructField("checked_block", LongType(), False),
        StructField("processed_at", TimestampType(), False),
    ]
)

WATERMARK_SCHEMA = StructType(
    [
        StructField("processed_through_block", LongType(), False),
        StructField("processed_at", TimestampType(), False),
    ]
)


def path_exists(spark: SparkSession, path: StoragePath) -> bool:
    """Check a local or Hadoop-backed path without listing its contents."""
    jvm_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(str(path))
    filesystem = jvm_path.getFileSystem(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    return bool(filesystem.exists(jvm_path))


class PoolMetadataStore:
    """Persist pool metadata and its block-number high-water mark."""

    def __init__(
        self,
        spark: SparkSession,
        *,
        pool_path: StoragePath,
        watermark_path: StoragePath,
        quarantine_path: StoragePath | None = None,
    ) -> None:
        self.spark = spark
        self.pool_path = pool_path
        self.watermark_path = watermark_path
        self.quarantine_path = quarantine_path

    def load_records(self) -> tuple[UniswapV3Pool, ...]:
        """Load the canonical pool registry, or return an empty registry."""
        if not path_exists(self.spark, self.pool_path):
            return ()
        rows = self.spark.read.schema(POOL_SCHEMA).parquet(
            str(self.pool_path)
        )
        records = []
        for row in rows.orderBy("chain", "protocol", "pool_address").collect():
            values = row.asDict(recursive=True)
            values["created_block_timestamp"] = _as_utc(
                row.created_block_timestamp
            )
            values["ingested_at"] = _as_utc(row.ingested_at)
            values["processed_at"] = _as_utc(row.processed_at)
            # Records written before observed-pool bootstrap are authoritative
            # Factory-event records and predate these two schema columns.
            values["metadata_source"] = (
                row.metadata_source or "factory_pool_created"
            )
            values["factory_verified"] = (
                True
                if row.factory_verified is None
                else row.factory_verified
            )
            records.append(UniswapV3Pool(**values))
        records_tuple = tuple(records)
        canonical = merge_pool_records((), records_tuple)
        if len(canonical) != len(records_tuple):
            raise ValueError("Canonical pool registry contains duplicate rows")
        return canonical

    def write_records(self, records: tuple[UniswapV3Pool, ...]) -> None:
        """Atomically replace the canonical, deterministically ordered registry."""
        rows = [
            tuple(getattr(record, field.name) for field in POOL_SCHEMA.fields)
            for record in records
        ]
        frame = self.spark.createDataFrame(rows, POOL_SCHEMA).orderBy(
            "chain",
            "protocol",
            "pool_address",
        )
        write_partitioned_dataset_atomic(frame, self.pool_path, ())

    def load_watermark(self) -> int | None:
        """Return the last fully processed block, if state exists."""
        if not path_exists(self.spark, self.watermark_path):
            return None
        rows = self.spark.read.schema(WATERMARK_SCHEMA).parquet(
            str(self.watermark_path)
        ).collect()
        if len(rows) != 1:
            raise ValueError("Pool metadata watermark must contain one row")
        return int(rows[0].processed_through_block)

    def write_watermark(self, block_number: int, processed_at) -> None:
        """Replace the block-number high-water mark after a successful write."""
        frame = self.spark.createDataFrame(
            [(block_number, processed_at)],
            WATERMARK_SCHEMA,
        )
        write_partitioned_dataset_atomic(frame, self.watermark_path, ())

    def write_issues(self, issues: tuple[PoolBootstrapIssue, ...]) -> None:
        """Replace the observed-pool quarantine with the latest check results."""
        if self.quarantine_path is None:
            raise ValueError("A quarantine path is required to write issues")
        rows = [
            tuple(getattr(issue, field.name) for field in QUARANTINE_SCHEMA.fields)
            for issue in sorted(issues, key=lambda issue: issue.pool_address)
        ]
        frame = self.spark.createDataFrame(rows, QUARANTINE_SCHEMA).orderBy(
            "pool_address"
        )
        write_partitioned_dataset_atomic(frame, self.quarantine_path, ())


def _as_utc(value: datetime | None) -> datetime | None:
    """Restore PySpark timestamps to timezone-aware UTC values."""
    return value.astimezone(UTC) if value is not None else None


def observed_swap_pools(
    spark: SparkSession,
    silver_path: StoragePath,
) -> tuple[str, ...]:
    """Return distinct canonical pool addresses observed in Swap Silver."""
    if not path_exists(spark, silver_path):
        return ()
    rows = (
        spark.read.parquet(str(silver_path))
        .select(lower(col("pool_address")).alias("pool_address"))
        .filter(col("pool_address").isNotNull())
        .distinct()
        .orderBy("pool_address")
        .collect()
    )
    return tuple(row.pool_address for row in rows)
