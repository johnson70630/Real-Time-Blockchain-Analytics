"""Single-host, single-checkpoint Spark Bronze writer for the EC2 pilot."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from pathlib import Path
from threading import Event

from runtime.contracts import (
    BUCKET,
    PILOT_PREFIX,
    REGION,
    TOPIC,
    validate_environment,
    writer_lock,
)

logger = logging.getLogger(__name__)
STATUS = Path("/run/pilot/status.json")
DRAIN = Path("/run/pilot/drain")


def stream_options(bootstrap: str = "kafka:9092", topic: str = TOPIC) -> dict:
    return {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "true",
        "maxOffsetsPerTrigger": "5000",
    }


def start_query(
    spark, output: str, checkpoint: str, *, bootstrap="kafka:9092", topic=TOPIC
):
    from spark import write_swaps_bronze

    raw = (
        spark.readStream.format("kafka")
        .options(**stream_options(bootstrap, topic))
        .load()
    )
    # The canonical transformation retains event envelopes, offsets, dates,
    # and provenance. Its output-root constant is configured before import.
    bronze = write_swaps_bronze.build_bronze_df(raw)
    return (
        bronze.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output)
        .option("checkpointLocation", checkpoint)
        .partitionBy("protocol", "chain", "event_type", "event_date")
        .trigger(processingTime="10 seconds")
        .start()
    )


def ensure_checkpoint_pair(
    output_exists: bool, checkpoint_exists: bool, metadata_exists: bool
):
    if output_exists and not checkpoint_exists:
        raise RuntimeError("CHECKPOINT_MISSING_RECOVERY_REQUIRED")
    if checkpoint_exists and not output_exists:
        raise RuntimeError("BRONZE_MISSING_RECOVERY_REQUIRED")
    if checkpoint_exists and not metadata_exists:
        raise RuntimeError("CHECKPOINT_INCOMPLETE_RECOVERY_REQUIRED")


def kafka_end_offsets(bootstrap="kafka:9092", topic=TOPIC) -> dict[str, int]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "pilot-drain-probe",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=15)
        info = metadata.topics[topic]
        if info.error:
            raise RuntimeError("KAFKA_TOPIC_METADATA_FAILED")
        return {
            str(partition): consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=15,
                cached=False,
            )[1]
            for partition in info.partitions
        }
    finally:
        consumer.close()


def verify_drained(progress: dict, ends: dict[str, int], topic=TOPIC) -> None:
    sources = progress.get("sources", [])
    offsets = sources[0].get("endOffset", {}) if sources else {}
    if isinstance(offsets, str):
        offsets = json.loads(offsets)
    actual = offsets.get(topic, {})
    if {str(k): int(v) for k, v in actual.items()} != ends:
        raise RuntimeError("SPARK_DRAIN_OFFSETS_DO_NOT_MATCH_KAFKA")


def progress_dict(progress) -> dict:
    # Spark 4 wraps query IDs as UUID objects in its Python mapping. Its
    # canonical JSON property preserves metrics and serializes IDs correctly.
    serialized = getattr(progress, "json", None)
    return (
        json.loads(serialized) if isinstance(serialized, str) else dict(progress or {})
    )


def drain_query(query, commit_exists, *, get_offsets=kafka_end_offsets, topic=TOPIC):
    ends = get_offsets()
    query.processAllAvailable()
    progress = progress_dict(query.lastProgress)
    verify_drained(progress, ends, topic)
    batch = progress.get("batchId", -1)
    # Spark may report an idle trigger with the next batch number even though
    # no new data/commit was necessary. Its offsets must still match Kafka.
    candidates = [batch]
    if progress.get("numInputRows") == 0 and batch > 0:
        candidates.append(batch - 1)
    committed = next(
        (value for value in candidates if value >= 0 and commit_exists(value)), None
    )
    if committed is None:
        raise RuntimeError("FINAL_CHECKPOINT_COMMIT_NOT_FOUND")
    query.stop()
    progress["verifiedCheckpointBatchId"] = committed
    return progress


def write_status(progress: dict, *, drained=False, path=STATUS):
    value = {
        "heartbeat": time.time(),
        "drained": drained,
        "progress": progress_dict(progress),
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


def healthy(path=STATUS, *, now=None) -> bool:
    try:
        value = json.loads(path.read_text())
        return (
            not value["drained"]
            and bool(value["progress"])
            and (time.time() if now is None else now) - value["heartbeat"] < 60
        )
    except (OSError, ValueError, KeyError):
        return False


def run_stream(stop: Event):
    from pyspark.sql import SparkSession

    root = f"s3a://{BUCKET}/{PILOT_PREFIX}"
    output, checkpoint = f"{root}/bronze/swaps", f"{root}/checkpoints/swaps_bronze"
    spark = (
        SparkSession.builder.appName("PilotUniswapBronze")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
        )
        .getOrCreate()
    )
    query = None
    try:
        spark.sparkContext.setLogLevel("WARN")
        jvm = spark.sparkContext._jvm
        if jvm.java.lang.System.getProperty("java.specification.version") != "17":
            raise RuntimeError("JAVA_17_REQUIRED")
        if spark.version != "4.0.1":
            raise RuntimeError("SPARK_4_0_1_REQUIRED")
        if jvm.org.apache.hadoop.util.VersionInfo.getVersion() != "3.4.1":
            raise RuntimeError("HADOOP_3_4_1_REQUIRED")
        path = jvm.org.apache.hadoop.fs.Path
        fs = path(root).getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        # Verifies role-based S3 read/list before starting any write.
        ensure_checkpoint_pair(
            fs.exists(path(output)),
            fs.exists(path(checkpoint)),
            fs.exists(path(checkpoint + "/metadata")),
        )
        query = start_query(spark, output, checkpoint)
        previous_batch = None
        while not stop.is_set() and not DRAIN.exists():
            if query.awaitTermination(5):
                raise RuntimeError("STREAM_TERMINATED_UNEXPECTEDLY")
            progress = query.lastProgress or {}
            write_status(progress)
            if progress.get("batchId") != previous_batch:
                previous_batch = progress.get("batchId")
                logger.info(
                    "BRONZE_PROGRESS batch=%s rows=%s",
                    previous_batch,
                    progress.get("numInputRows", 0),
                )
        # Producer must already be stopped. This operation completes the file
        # sink/checkpoint commit before comparing durable offsets to the broker.
        progress = drain_query(
            query, lambda batch: fs.exists(path(f"{checkpoint}/commits/{batch}"))
        )
        write_status(progress, drained=True)
        logger.info(
            "SPARK_DRAIN_VERIFIED batch=%s", progress["verifiedCheckpointBatchId"]
        )
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


def main() -> int:
    from config.logging import configure_logging

    configure_logging()
    stop = Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    try:
        validate_environment(os.environ)
        with writer_lock(Path("/run/pilot/writer.lock")):
            DRAIN.unlink(missing_ok=True)
            STATUS.unlink(missing_ok=True)
            run_stream(stop)
        return 0
    except Exception as error:
        logger.error("PILOT_STREAM_FAILED type=%s", type(error).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
