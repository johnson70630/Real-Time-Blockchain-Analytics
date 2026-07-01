from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "uniswap_v3_swaps"

BRONZE_OUTPUT_PATH = "data/bronze/swaps"
CHECKPOINT_PATH = "data/checkpoints/swaps_bronze"

schema = StructType(
    [
        StructField("chain", StringType()),
        StructField("event_type", StringType()),
        StructField("block_number", IntegerType()),
        StructField("transaction_hash", StringType()),
        StructField("pool_address", StringType()),
        StructField("log_index", IntegerType()),
        StructField("raw_data", StringType()),
        StructField("raw_topics", ArrayType(StringType())),
    ]
)

spark = (
    SparkSession.builder.appName("WriteUniswapSwapsBronze")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

bronze_df = (
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
        "kafka_partition",
        "kafka_offset",
        "kafka_key",
        "json_value",
        col("event.chain"),
        col("event.event_type"),
        col("event.block_number"),
        col("event.transaction_hash"),
        col("event.pool_address"),
        col("event.log_index"),
        col("event.raw_data"),
        col("event.raw_topics"),
        current_timestamp().alias("ingested_at"),
    )
)

query = (
    bronze_df.writeStream.outputMode("append")
    .format("parquet")
    .option("path", BRONZE_OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Writing bronze swap events to: {BRONZE_OUTPUT_PATH}")
print(f"Using checkpoint path: {CHECKPOINT_PATH}")

query.awaitTermination()