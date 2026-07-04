import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "uniswap_v3_swaps"
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def get_swap_schema() -> StructType:
    return StructType(
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


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ReadKafkaSwapStream")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
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
    schema = get_swap_schema()

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