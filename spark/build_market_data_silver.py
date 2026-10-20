"""Build exact, deduplicated Chainlink market prices from Bronze observations."""

import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    array,
    array_compact,
    coalesce,
    col,
    current_timestamp,
    expr,
    get_json_object,
    lit,
    lower,
    row_number,
    sha2,
    size,
    struct,
    to_date,
    to_json,
    trim,
    try_to_timestamp,
    upper,
    when,
)
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.logging import configure_logging
from config.settings import (
    DATA_LAKE,
)
from config.storage import join_location
from config.versions import SILVER_JOB_VERSION
from backfill.chainlink_storage import ChainlinkBootstrapBatchStore
from reference_data.storage import path_exists
from spark.parquet import (
    delete_dataset,
    discover_parquet_files,
    is_remote_path,
    write_completion_marker,
    write_partitioned_dataset_atomic,
)
from spark.session import create_spark_session as create_shared_spark_session

logger = logging.getLogger(__name__)

MARKET_DATA_BRONZE_OUTPUT_PATH = DATA_LAKE.get("bronze_market_data")
MARKET_DATA_BACKFILL_OUTPUT_PATH = DATA_LAKE.get("bronze_chainlink_backfill")
MARKET_DATA_FORWARD_OUTPUT_PATH = DATA_LAKE.get("bronze_chainlink_forward")
MARKET_DATA_SILVER_DIR = DATA_LAKE.get("silver_chainlink_market_prices")
MARKET_DATA_SILVER_QUARANTINE_PATH = DATA_LAKE.get(
    "quarantine_market_data_silver"
)

# DECIMAL(38, 18) retains 18 fractional digits and up to 20 integer digits.
# The configured Chainlink feeds use 8 decimals, so their prices fit comfortably
# while remaining exact and never passing through FLOAT or DOUBLE.
PRICE_PRECISION = 38
PRICE_SCALE = 18
PRICE_TYPE = DecimalType(PRICE_PRECISION, PRICE_SCALE)
MAX_SUPPORTED_FEED_DECIMALS = PRICE_SCALE

SILVER_MARKET_PRICE_COLUMNS = (
    "observation_id",
    "snapshot_id",
    "protocol",
    "event_type",
    "chain",
    "feed_address",
    "base_asset",
    "quote_asset",
    "round_id",
    "answer_raw",
    "feed_decimals",
    "price",
    "feed_updated_at",
    "observed_at",
    "block_number",
    "block_timestamp",
    "ingested_at",
    "producer_version",
    "schema_version",
    "source_topic",
    "kafka_timestamp",
    "kafka_partition",
    "kafka_offset",
    "kafka_key",
    "json_value",
    "bronze_processed_at",
    "bronze_file",
    "silver_processed_at",
    "silver_job_version",
    "price_date",
)


@dataclass(frozen=True, slots=True)
class MarketDataSilverStats:
    """Operational row counts for one completed Silver build."""

    bronze_records_read: int
    valid_records: int
    invalid_records: int
    duplicate_records_removed: int
    silver_records_written: int


def get_market_price_schema() -> StructType:
    """Return the typed output contract for the Silver market price dataset."""
    return StructType(
        [
            StructField("observation_id", StringType(), False),
            StructField("snapshot_id", StringType(), True),
            StructField("protocol", StringType(), False),
            StructField("event_type", StringType(), True),
            StructField("chain", StringType(), False),
            StructField("feed_address", StringType(), False),
            StructField("base_asset", StringType(), False),
            StructField("quote_asset", StringType(), False),
            StructField("round_id", StringType(), False),
            StructField("answer_raw", StringType(), False),
            StructField("feed_decimals", IntegerType(), False),
            StructField(
                "price",
                DecimalType(PRICE_PRECISION, PRICE_SCALE),
                False,
            ),
            StructField("feed_updated_at", TimestampType(), False),
            StructField("observed_at", TimestampType(), True),
            StructField("block_number", LongType(), False),
            StructField("block_timestamp", TimestampType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("producer_version", StringType(), True),
            StructField("schema_version", StringType(), True),
            StructField("source_topic", StringType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("kafka_key", StringType(), True),
            StructField("json_value", StringType(), True),
            StructField("bronze_processed_at", TimestampType(), True),
            StructField("bronze_file", StringType(), True),
            StructField("silver_processed_at", TimestampType(), False),
            StructField("silver_job_version", StringType(), False),
            StructField("price_date", DateType(), False),
        ]
    )


def _column_or_null(
    frame: DataFrame,
    name: str,
    data_type: str,
) -> Column:
    """Return an existing column or a typed null for legacy Bronze files."""
    if name in frame.columns:
        return col(name)
    return lit(None).cast(data_type)


def _exact_price_expression() -> Column:
    """Scale an integer string into DECIMAL(38, 18) without floating point."""
    return expr(
        f"""
        TRY_CAST(
            CASE
                WHEN feed_decimals = 0 THEN answer_raw
                ELSE CONCAT(
                    SUBSTRING(
                        LPAD(
                            answer_raw,
                            GREATEST(
                                LENGTH(answer_raw),
                                feed_decimals + 1
                            ),
                            '0'
                        ),
                        1,
                        LENGTH(
                            LPAD(
                                answer_raw,
                                GREATEST(
                                    LENGTH(answer_raw),
                                    feed_decimals + 1
                                ),
                                '0'
                            )
                        ) - feed_decimals
                    ),
                    '.',
                    RIGHT(
                        LPAD(
                            answer_raw,
                            GREATEST(
                                LENGTH(answer_raw),
                                feed_decimals + 1
                            ),
                            '0'
                        ),
                        feed_decimals
                    )
                )
            END
            AS DECIMAL({PRICE_PRECISION}, {PRICE_SCALE})
        )
        """
    )


def prepare_market_data_records(bronze: DataFrame) -> DataFrame:
    """Normalize Bronze types and attach deterministic validation reasons."""
    snapshot_from_json = get_json_object(
        _column_or_null(bronze, "json_value", "string"),
        "$.snapshot_id",
    )
    snapshot_id = (
        coalesce(col("snapshot_id"), snapshot_from_json)
        if "snapshot_id" in bronze.columns
        else snapshot_from_json
    )

    normalized = bronze.select(
        _column_or_null(bronze, "observation_id", "string").cast("string").alias(
            "observation_id"
        ),
        snapshot_id.cast("string").alias("snapshot_id"),
        _column_or_null(bronze, "protocol", "string").cast("string").alias(
            "protocol"
        ),
        _column_or_null(bronze, "event_type", "string").cast("string").alias(
            "event_type"
        ),
        coalesce(
            _column_or_null(bronze, "chain", "string").cast("string"),
            get_json_object(
                _column_or_null(bronze, "json_value", "string"),
                "$.chain",
            ),
        ).alias("chain"),
        lower(
            trim(
                _column_or_null(bronze, "feed_address", "string").cast("string")
            )
        ).alias("feed_address"),
        upper(
            trim(_column_or_null(bronze, "base_asset", "string").cast("string"))
        ).alias("base_asset"),
        upper(
            trim(_column_or_null(bronze, "quote_asset", "string").cast("string"))
        ).alias("quote_asset"),
        _column_or_null(bronze, "round_id", "string").cast("string").alias(
            "round_id"
        ),
        _column_or_null(bronze, "answer_raw", "string").cast("string").alias(
            "answer_raw"
        ),
        _column_or_null(bronze, "feed_decimals", "int").cast("int").alias(
            "feed_decimals"
        ),
        try_to_timestamp(
            _column_or_null(bronze, "feed_updated_at", "timestamp")
        ).alias("feed_updated_at"),
        try_to_timestamp(
            _column_or_null(bronze, "observed_at", "timestamp")
        ).alias("observed_at"),
        _column_or_null(bronze, "block_number", "bigint").cast("bigint").alias(
            "block_number"
        ),
        try_to_timestamp(
            _column_or_null(bronze, "block_timestamp", "timestamp")
        ).alias("block_timestamp"),
        try_to_timestamp(
            _column_or_null(bronze, "ingested_at", "timestamp")
        ).alias("ingested_at"),
        _column_or_null(bronze, "producer_version", "string")
        .cast("string")
        .alias("producer_version"),
        _column_or_null(bronze, "schema_version", "string")
        .cast("string")
        .alias("schema_version"),
        _column_or_null(bronze, "source_topic", "string")
        .cast("string")
        .alias("source_topic"),
        try_to_timestamp(
            _column_or_null(bronze, "kafka_timestamp", "timestamp")
        ).alias("kafka_timestamp"),
        _column_or_null(bronze, "kafka_partition", "int")
        .cast("int")
        .alias("kafka_partition"),
        _column_or_null(bronze, "kafka_offset", "bigint")
        .cast("bigint")
        .alias("kafka_offset"),
        _column_or_null(bronze, "kafka_key", "string")
        .cast("string")
        .alias("kafka_key"),
        _column_or_null(bronze, "json_value", "string")
        .cast("string")
        .alias("json_value"),
        try_to_timestamp(
            _column_or_null(bronze, "bronze_processed_at", "timestamp")
        ).alias("bronze_processed_at"),
        _column_or_null(bronze, "bronze_file", "string")
        .cast("string")
        .alias("bronze_file"),
    ).withColumn("price", _exact_price_expression())

    def blank(name: str) -> Column:
        return col(name).isNull() | (trim(col(name)) == "")

    valid_answer_format = col("answer_raw").rlike(r"^[0-9]+$")
    answer_decimal = expr("TRY_CAST(answer_raw AS DECIMAL(38, 0))")

    return (
        normalized.withColumn(
            "validation_errors",
            array_compact(
                array(
                    *(
                        when(blank(field), lit(f"missing_or_blank:{field}"))
                        for field in (
                            "observation_id",
                            "chain",
                            "feed_address",
                            "base_asset",
                            "quote_asset",
                            "round_id",
                            "answer_raw",
                        )
                    ),
                    when(
                        col("protocol").isNull()
                        | (col("protocol") != "chainlink"),
                        lit("invalid:protocol"),
                    ),
                    when(
                        ~col("round_id").rlike(r"^[0-9]+$"),
                        lit("invalid:round_id"),
                    ),
                    when(~valid_answer_format, lit("invalid:answer_raw")),
                    when(
                        valid_answer_format
                        & answer_decimal.isNotNull()
                        & (answer_decimal <= 0),
                        lit("non_positive:answer_raw"),
                    ),
                    when(
                        valid_answer_format & answer_decimal.isNull(),
                        lit("out_of_range:answer_raw"),
                    ),
                    when(
                        col("feed_decimals").isNull()
                        | ~col("feed_decimals").between(
                            0,
                            MAX_SUPPORTED_FEED_DECIMALS,
                        ),
                        lit("invalid:feed_decimals"),
                    ),
                    when(
                        col("feed_updated_at").isNull(),
                        lit("invalid:feed_updated_at"),
                    ),
                    when(
                        col("block_number").isNull()
                        | (col("block_number") < 0),
                        lit("invalid:block_number"),
                    ),
                    when(col("price").isNull(), lit("out_of_range:price")),
                )
            ),
        )
        .withColumn("silver_processed_at", current_timestamp())
        .withColumn("silver_job_version", lit(SILVER_JOB_VERSION))
        .withColumn("price_date", to_date(col("feed_updated_at")))
    )


def deduplicate_market_prices(valid_records: DataFrame) -> DataFrame:
    """Keep the newest deterministic record for each observation identity."""
    fingerprint_columns = (
        col(name)
        for name in sorted(valid_records.columns)
        if name != "silver_processed_at"
    )
    ordering = Window.partitionBy("observation_id").orderBy(
        col("bronze_processed_at").desc_nulls_last(),
        col("observed_at").desc_nulls_last(),
        col("kafka_timestamp").desc_nulls_last(),
        col("kafka_partition").desc_nulls_last(),
        col("kafka_offset").desc_nulls_last(),
        sha2(to_json(struct(*fingerprint_columns)), 256).desc(),
    )
    return (
        valid_records.withColumn("_row_number", row_number().over(ordering))
        .filter(col("_row_number") == 1)
        .drop("_row_number", "validation_errors")
        .select(*SILVER_MARKET_PRICE_COLUMNS)
    )


def transform_market_data(
    bronze: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Return deduplicated Silver prices and row-level rejected records."""
    prepared = prepare_market_data_records(bronze)
    valid = prepared.filter(size("validation_errors") == 0)
    silver = deduplicate_market_prices(valid)
    quarantine = (
        prepared.filter(size("validation_errors") > 0)
        .withColumn("quarantined_at", current_timestamp())
        .withColumn(
            "quarantine_date",
            to_date(coalesce(col("feed_updated_at"), col("bronze_processed_at"))),
        )
    )
    return silver, quarantine


def read_market_data_bronze(
    spark: SparkSession,
    bronze_root: str | Path = MARKET_DATA_BRONZE_OUTPUT_PATH,
    historical_root: str | Path | None = MARKET_DATA_BACKFILL_OUTPUT_PATH,
    forward_root: str | Path | None = None,
    bootstrap_paths: tuple[str | Path, ...] = (),
) -> DataFrame:
    """Read live, historical, and forward Bronze with one compatible schema."""
    frames = []
    for root in (bronze_root, historical_root):
        if root is not None and path_exists(spark, root):
            frames.append(
                spark.read.option("basePath", str(root))
                .option("mergeSchema", "true")
                .parquet(str(root))
            )
    if forward_root is not None and path_exists(spark, forward_root):
        frames.append(
            spark.read.option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .parquet(str(forward_root))
        )
    frames.extend(
        spark.read.option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .parquet(str(path))
        for path in bootstrap_paths
    )
    if not frames:
        raise FileNotFoundError("No live or historical market-data Bronze found")
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined


def build_market_data_silver(
    spark: SparkSession,
    *,
    bronze_root: str | Path = MARKET_DATA_BRONZE_OUTPUT_PATH,
    historical_root: str | Path | None = MARKET_DATA_BACKFILL_OUTPUT_PATH,
    forward_root: str | Path | None = None,
    bootstrap_paths: tuple[str | Path, ...] = (),
    silver_path: str | Path = MARKET_DATA_SILVER_DIR,
    quarantine_path: str | Path = MARKET_DATA_SILVER_QUARANTINE_PATH,
) -> MarketDataSilverStats:
    """Rebuild Silver prices and rejected records atomically from Bronze."""
    roots = tuple(
        root
        for root in (
            bronze_root,
            historical_root,
            forward_root,
            *bootstrap_paths,
        )
        if root is not None
    )
    local_files = [
        discover_parquet_files(Path(root))
        for root in roots
        if not is_remote_path(root)
    ]
    remote_roots = tuple(root for root in roots if is_remote_path(root))
    if not remote_roots and not any(local_files):
        logger.info("No market-data Bronze files found; Silver is unchanged")
        return MarketDataSilverStats(0, 0, 0, 0, 0)

    try:
        bronze = read_market_data_bronze(
            spark,
            bronze_root,
            historical_root,
            forward_root,
            bootstrap_paths,
        ).cache()
    except FileNotFoundError:
        logger.info("No market-data Bronze files found; Silver is unchanged")
        return MarketDataSilverStats(0, 0, 0, 0, 0)
    silver, quarantine = transform_market_data(bronze)
    silver = silver.cache()
    quarantine = quarantine.cache()

    try:
        bronze_count = bronze.count()
        invalid_count = quarantine.count()
        silver_count = silver.count()
        valid_count = bronze_count - invalid_count
        duplicate_count = valid_count - silver_count

        logger.info(
            "Market Silver read=%s valid=%s invalid=%s duplicates_removed=%s",
            bronze_count,
            valid_count,
            invalid_count,
            duplicate_count,
        )
        write_partitioned_dataset_atomic(
            silver,
            silver_path,
            ("price_date",),
        )
        write_partitioned_dataset_atomic(
            quarantine,
            quarantine_path,
            ("quarantine_date",),
        )
        logger.info(
            "Market Silver wrote=%s path=%s quarantined=%s quarantine_path=%s",
            silver_count,
            silver_path,
            invalid_count,
            quarantine_path,
        )
        return MarketDataSilverStats(
            bronze_records_read=bronze_count,
            valid_records=valid_count,
            invalid_records=invalid_count,
            duplicate_records_removed=duplicate_count,
            silver_records_written=silver_count,
        )
    except Exception:
        logger.exception("Market-data Silver build failed")
        raise
    finally:
        quarantine.unpersist()
        silver.unpersist()
        bronze.unpersist()


def build_market_data_silver_increment(
    spark: SparkSession,
    *,
    start_block: int,
    end_block: int,
    bronze_root: str | Path = MARKET_DATA_FORWARD_OUTPUT_PATH,
    silver_root: str | Path = MARKET_DATA_SILVER_DIR,
    quarantine_root: str | Path = MARKET_DATA_SILVER_QUARANTINE_PATH,
) -> MarketDataSilverStats:
    """Normalize one marker-backed forward increment without rewriting history."""
    if start_block < 0 or end_block < start_block:
        raise ValueError("Chainlink Silver increment range is invalid")
    batch_name = f"block_{start_block}_{end_block}"
    paths = ChainlinkBootstrapBatchStore(
        spark,
        bronze_root=str(bronze_root),
        staging_root=DATA_LAKE.get("state_chainlink_forward_staging"),
        quarantine_root=DATA_LAKE.get("quarantine_chainlink_forward"),
        source_topic="chainlink_forward_rpc",
    ).committed_batch_paths()
    if not paths:
        raise FileNotFoundError(
            "Chainlink forward Bronze has no committed batches"
        )
    frames = [
        spark.read.option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .parquet(str(path))
        for path in paths
    ]
    bronze = frames[0]
    for frame in frames[1:]:
        bronze = bronze.unionByName(frame, allowMissingColumns=True)
    bronze = bronze.cache()
    silver, quarantine = transform_market_data(bronze)
    silver = silver.cache()
    quarantine = quarantine.cache()
    try:
        bronze_count = bronze.count()
        invalid_count = quarantine.count()
        silver_count = silver.count()
        valid_count = bronze_count - invalid_count
        duplicate_count = valid_count - silver_count
        output_path = join_location(silver_root, "increments", batch_name)
        completion_marker = join_location(output_path, "_COMMITTED")
        quarantine_path = join_location(
            quarantine_root,
            "increments",
            batch_name,
        )
        if path_exists(spark, completion_marker):
            persisted = spark.read.parquet(str(output_path))
            persisted_count = persisted.count()
            if persisted_count != persisted.select("observation_id").distinct().count():
                raise RuntimeError("Existing Chainlink Silver increment has duplicates")
            return MarketDataSilverStats(
                persisted_count,
                persisted_count,
                0,
                0,
                persisted_count,
            )
        if path_exists(spark, output_path):
            delete_dataset(spark, output_path)
        if invalid_count:
            write_partitioned_dataset_atomic(
                quarantine,
                quarantine_path,
                ("quarantine_date",),
            )
            raise RuntimeError("Chainlink forward Silver contains invalid rows")
        existing_silver = (
            spark.read.option("recursiveFileLookup", "true")
            .parquet(str(silver_root))
            .select("observation_id")
            .distinct()
        )
        increment = silver.join(existing_silver, "observation_id", "left_anti").cache()
        try:
            increment_count = increment.count()
            write_partitioned_dataset_atomic(
                increment.repartition("price_date"),
                output_path,
                ("price_date",),
            )
            persisted = spark.read.parquet(str(output_path))
            persisted_count = persisted.count()
            if (
                persisted_count != increment_count
                or persisted_count
                != persisted.select("observation_id").distinct().count()
            ):
                raise RuntimeError("Persisted Chainlink Silver increment is invalid")
            write_completion_marker(increment, output_path)
        finally:
            increment.unpersist()
        return MarketDataSilverStats(
            bronze_records_read=bronze_count,
            valid_records=valid_count,
            invalid_records=invalid_count,
            duplicate_records_removed=duplicate_count,
            silver_records_written=increment_count,
        )
    finally:
        quarantine.unpersist()
        silver.unpersist()
        bronze.unpersist()


def committed_chainlink_bootstrap_paths(
    spark: SparkSession,
) -> tuple[str, ...]:
    """Return only marker-backed historical bootstrap batches."""
    return ChainlinkBootstrapBatchStore(
        spark,
        bronze_root=DATA_LAKE.get("bronze_chainlink_bootstrap"),
        staging_root=DATA_LAKE.get("state_chainlink_bootstrap_staging"),
        quarantine_root=DATA_LAKE.get("quarantine_chainlink_bootstrap"),
    ).committed_batch_paths()


def build_chainlink_bootstrap_silver_increment(
    spark: SparkSession,
    *,
    historical_root: str | Path = MARKET_DATA_BACKFILL_OUTPUT_PATH,
    bootstrap_paths: tuple[str | Path, ...] | None = None,
    silver_root: str | Path = MARKET_DATA_SILVER_DIR,
    quarantine_root: str | Path = MARKET_DATA_SILVER_QUARANTINE_PATH,
) -> MarketDataSilverStats:
    """Write the complete bootstrap delta without replacing prior Silver."""
    paths = (
        committed_chainlink_bootstrap_paths(spark)
        if bootstrap_paths is None
        else bootstrap_paths
    )
    if not paths:
        raise FileNotFoundError("No committed Chainlink bootstrap batches found")
    increment_name = "bootstrap_515520400"
    output_path = join_location(silver_root, "increments", increment_name)
    completion_marker = join_location(output_path, "_COMMITTED")
    if path_exists(spark, completion_marker):
        existing = spark.read.parquet(str(output_path))
        count = existing.count()
        duplicate_count = count - existing.select("observation_id").distinct().count()
        if duplicate_count:
            raise RuntimeError("Existing Chainlink bootstrap Silver has duplicates")
        return MarketDataSilverStats(count, count, 0, 0, count)
    if path_exists(spark, output_path):
        logger.warning(
            "Removing incomplete Chainlink bootstrap Silver increment path=%s",
            output_path,
        )
        delete_dataset(spark, output_path)

    frames = [
        spark.read.option("basePath", str(historical_root))
        .option("mergeSchema", "true")
        .parquet(str(historical_root))
    ]
    frames.extend(
        spark.read.option("recursiveFileLookup", "true")
        .option("mergeSchema", "true")
        .parquet(str(path))
        for path in paths
    )
    bronze = frames[0]
    for frame in frames[1:]:
        bronze = bronze.unionByName(frame, allowMissingColumns=True)
    bronze = bronze.cache()
    silver, quarantine = transform_market_data(bronze)
    silver = silver.cache()
    quarantine = quarantine.cache()
    try:
        invalid_count = quarantine.count()
        if invalid_count:
            quarantine_path = join_location(
                quarantine_root,
                "increments",
                increment_name,
            )
            write_partitioned_dataset_atomic(
                quarantine,
                quarantine_path,
                ("quarantine_date",),
            )
            raise RuntimeError("Chainlink bootstrap Silver contains invalid rows")
        existing_silver = (
            spark.read.option("recursiveFileLookup", "true")
            .parquet(str(silver_root))
            .select("observation_id")
            .distinct()
        )
        increment = silver.join(existing_silver, "observation_id", "left_anti").cache()
        try:
            bronze_count = bronze.count()
            silver_count = silver.count()
            increment_count = increment.count()
            duplicate_count = bronze_count - silver_count
            compact_increment = increment.repartition("price_date")
            write_partitioned_dataset_atomic(
                compact_increment,
                output_path,
                ("price_date",),
            )
            persisted = spark.read.parquet(str(output_path))
            persisted_count = persisted.count()
            persisted_unique_count = (
                persisted.select("observation_id").distinct().count()
            )
            if (
                persisted_count != increment_count
                or persisted_unique_count != increment_count
            ):
                raise RuntimeError(
                    "Persisted Chainlink bootstrap Silver failed validation"
                )
            write_completion_marker(increment, output_path)
            return MarketDataSilverStats(
                bronze_records_read=bronze_count,
                valid_records=bronze_count,
                invalid_records=0,
                duplicate_records_removed=duplicate_count,
                silver_records_written=increment_count,
            )
        finally:
            increment.unpersist()
    finally:
        quarantine.unpersist()
        silver.unpersist()
        bronze.unpersist()


def create_spark_session() -> SparkSession:
    """Create the shared local or S3-enabled Spark session."""
    spark = create_shared_spark_session("BuildMarketDataSilver")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def main() -> None:
    configure_logging()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        build_market_data_silver(
            spark,
            forward_root=MARKET_DATA_FORWARD_OUTPUT_PATH,
            bootstrap_paths=committed_chainlink_bootstrap_paths(spark),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
