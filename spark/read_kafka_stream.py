from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructType, StructField

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "uniswap_v3_swaps"

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
    SparkSession.builder.appName("UniswapV3SwapStream")
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

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), schema).alias("event"))
    .select("event.*")
)

query = (
    parsed_df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()