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
from config.storage import join_location
from config.versions import SILVER_JOB_VERSION
from reference_data.storage import path_exists
from spark.parquet import write_partitioned_dataset_atomic


def read_bronze_events(spark: SparkSession) -> DataFrame:
    """Read live and historical Hive-partitioned blockchain Bronze events."""
    path = DATA_LAKE.get("bronze_events")
    live = (
        spark.read.option("basePath", path)
        .option("mergeSchema", "true")
        .parquet(path)
    )
    backfill_path = DATA_LAKE.get("bronze_aave_backfill")
    sources = [live]
    if path_exists(spark, backfill_path):
        sources.append(
            spark.read.option("basePath", backfill_path)
            .option("mergeSchema", "true")
            .parquet(backfill_path)
        )
    forward_path = DATA_LAKE.get("bronze_aave_forward")
    if path_exists(spark, forward_path):
        sources.append(
            spark.read.option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .parquet(forward_path)
        )
    uniswap_recovery_path = DATA_LAKE.get("bronze_uniswap_recovery")
    if path_exists(spark, uniswap_recovery_path):
        sources.append(
            spark.read.option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .parquet(uniswap_recovery_path)
        )
    combined = sources[0]
    for source in sources[1:]:
        combined = combined.unionByName(source, allowMissingColumns=True)
    return combined


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


def build_uniswap_silver_increment(
    spark: SparkSession,
    *,
    start_block: int,
    end_block: int,
) -> int:
    """Normalize one committed recovery batch without rewriting base Silver."""
    if start_block < 0 or end_block < start_block:
        raise ValueError("Uniswap Silver increment range is invalid")
    batch_name = f"range_{start_block}_{end_block}"
    bronze_path = str(
        join_location(DATA_LAKE.get("bronze_uniswap_recovery"), batch_name)
    )
    if not path_exists(spark, bronze_path):
        raise FileNotFoundError(
            f"Uniswap recovery Bronze does not exist: {batch_name}"
        )
    output_path = str(
        join_location(
            DATA_LAKE.get("silver_uniswap_swaps"),
            "increments",
            batch_name,
        )
    )
    if path_exists(spark, output_path):
        return spark.read.parquet(output_path).count()
    bronze = spark.read.option("mergeSchema", "true").parquet(bronze_path)
    silver = _deduplicate(
        bronze.filter(
            (col("protocol") == "uniswap_v3")
            & (col("event_type") == "swap")
            & (col("block_number") >= start_block)
            & (col("block_number") <= end_block)
            & col("chain").isNotNull()
            & col("transaction_hash").isNotNull()
            & col("pool_address").isNotNull()
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
    silver_root = DATA_LAKE.get("silver_uniswap_swaps")
    if path_exists(spark, silver_root):
        existing_ids = spark.read.parquet(silver_root).select("event_id").distinct()
        silver = silver.join(existing_ids, on="event_id", how="left_anti")
    count = silver.count()
    write_partitioned_dataset_atomic(silver, output_path, ())
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
        events = _select_aave_model(bronze, model)
        count = events.count()
        write_partitioned_dataset_atomic(
            events,
            DATA_LAKE.get(model.dataset),
            (),
        )
        results[model.event_type] = count
    return results


def _select_aave_model(bronze: DataFrame, model: CloudAaveModel) -> DataFrame:
    """Project one canonical Aave Silver schema from a Bronze frame."""
    payload_columns = (
        "contract_address",
        "raw_data",
        "raw_topics",
        *model.payload_fields,
    )
    return _deduplicate(
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


def build_aave_silver_increment(
    spark: SparkSession,
    *,
    start_block: int,
    end_block: int,
) -> dict[str, int]:
    """Normalize one deterministic Bronze increment without rewriting history."""
    if start_block < 0 or end_block < start_block:
        raise ValueError("Aave Silver increment range is invalid")
    batch_name = f"range_{start_block}_{end_block}"
    bronze_path = str(
        join_location(DATA_LAKE.get("bronze_aave_forward"), batch_name)
    )
    if not path_exists(spark, bronze_path):
        raise FileNotFoundError(
            f"Forward Bronze increment does not exist: {batch_name}"
        )
    bronze = (
        spark.read.option("mergeSchema", "true")
        .parquet(bronze_path)
        .filter(
            (col("protocol") == "aave_v3")
            & (col("block_number") >= start_block)
            & (col("block_number") <= end_block)
        )
    )
    results: dict[str, int] = {}
    for model in CLOUD_AAVE_MODELS:
        events = _select_aave_model(bronze, model)
        count = events.count()
        output_path = str(
            join_location(
                DATA_LAKE.get(model.dataset),
                "increments",
                batch_name,
            )
        )
        write_partitioned_dataset_atomic(events, output_path, ())
        results[model.event_type] = count
    return results
