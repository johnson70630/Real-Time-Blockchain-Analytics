"""Spark normalization paths used when durable storage is configured for S3."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    lit,
    row_number,
)

from config.settings import DATA_LAKE
from config.versions import SILVER_JOB_VERSION
from spark.parquet import write_partitioned_dataset_atomic


def read_bronze_events(spark: SparkSession) -> DataFrame:
    """Read all Hive-partitioned blockchain Bronze events from the lake."""
    path = DATA_LAKE.get("bronze_events")
    return (
        spark.read.option("basePath", path)
        .option("mergeSchema", "true")
        .parquet(path)
    )


def _event_id() -> Column:
    return concat_ws(
        "-",
        col("chain"),
        col("transaction_hash"),
        col("log_index").cast("string"),
    )


def _deduplicate(events: DataFrame) -> DataFrame:
    ordering = Window.partitionBy(
        "chain",
        "transaction_hash",
        "log_index",
    ).orderBy(
        col("kafka_timestamp").desc_nulls_last(),
        col("ingested_at").desc_nulls_last(),
    )
    return (
        events.withColumn("_row_number", row_number().over(ordering))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )


def build_uniswap_silver(spark: SparkSession) -> int:
    """Normalize and deterministically replace the cloud Uniswap Swap dataset."""
    bronze = read_bronze_events(spark)
    silver = _deduplicate(
        bronze.filter(
            (col("protocol") == "uniswap_v3")
            & (col("event_type") == "swap")
            & col("chain").isNotNull()
            & col("event_date").isNotNull()
            & col("transaction_hash").isNotNull()
            & col("pool_address").isNotNull()
            & col("block_number").isNotNull()
            & col("log_index").isNotNull()
        )
    ).select(
        _event_id().alias("event_id"),
        "protocol",
        "chain",
        "event_date",
        "block_timestamp",
        "event_type",
        "block_number",
        "transaction_hash",
        "pool_address",
        col("payload.amount0").alias("amount0_raw"),
        col("payload.amount1").alias("amount1_raw"),
        "log_index",
        "raw_data",
        "raw_topics",
        "kafka_timestamp",
        "ingested_at",
        "producer_version",
        "schema_version",
        "bronze_processed_at",
        "bronze_file",
        current_timestamp().alias("silver_processed_at"),
        lit(SILVER_JOB_VERSION).alias("silver_job_version"),
    )
    count = silver.count()
    write_partitioned_dataset_atomic(
        silver,
        DATA_LAKE.get("silver_uniswap_swaps"),
        (),
    )
    return count


@dataclass(frozen=True, slots=True)
class CloudAaveModel:
    """Define one Aave event payload projection and lake destination."""

    event_type: str
    dataset: str
    payload_fields: tuple[str, ...]


CLOUD_AAVE_MODELS = (
    CloudAaveModel(
        "borrow",
        "silver_aave_borrows",
        (
            "reserve",
            "user",
            "on_behalf_of",
            "amount_raw",
            "interest_rate_mode",
            "borrow_rate_raw",
            "referral_code",
        ),
    ),
    CloudAaveModel(
        "repay",
        "silver_aave_repays",
        ("reserve", "user", "repayer", "amount_raw", "use_atokens"),
    ),
    CloudAaveModel(
        "liquidation",
        "silver_aave_liquidations",
        (
            "collateral_asset",
            "debt_asset",
            "user",
            "debt_to_cover_raw",
            "liquidated_collateral_amount_raw",
            "liquidator",
            "receive_atoken",
        ),
    ),
)


def build_aave_silver(spark: SparkSession) -> dict[str, int]:
    """Normalize all configured cloud Aave event datasets from one Bronze read."""
    bronze = read_bronze_events(spark).filter(col("protocol") == "aave_v3")
    results: dict[str, int] = {}
    for model in CLOUD_AAVE_MODELS:
        payload_columns = (
            "contract_address",
            "raw_data",
            "raw_topics",
            *model.payload_fields,
        )
        events = _deduplicate(
            bronze.filter(col("event_type") == model.event_type)
        ).select(
            _event_id().alias("event_id"),
            "protocol",
            "chain",
            "event_type",
            "event_date",
            "block_timestamp",
            "transaction_hash",
            "block_number",
            "log_index",
            "kafka_timestamp",
            "ingested_at",
            "producer_version",
            "schema_version",
            "bronze_processed_at",
            "bronze_file",
            *(col(f"payload.{field}").alias(field) for field in payload_columns),
            current_timestamp().alias("silver_processed_at"),
            lit(SILVER_JOB_VERSION).alias("silver_job_version"),
        )
        count = events.count()
        write_partitioned_dataset_atomic(
            events,
            DATA_LAKE.get(model.dataset),
            (),
        )
        results[model.event_type] = count
    return results
