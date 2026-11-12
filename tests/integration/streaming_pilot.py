"""Synthetic container integration. No chain provider, AWS, or credentials."""

import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession

from runtime.stream import (
    drain_query,
    kafka_end_offsets,
    start_query,
    verify_drained,
    write_status,
)
from spark import write_swaps_bronze

BOOTSTRAP = "kafka:9092"
TOPIC = "synthetic-pilot-events"
OUTPUT = "/test-data/bronze"
CHECKPOINT = "/test-data/checkpoint"


def publish(start, count):
    publisher = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    errors = []
    for number in range(start, start + count):
        event = {
            "protocol": "uniswap_v3",
            "chain": "arbitrum",
            "event_type": "swap",
            "block_number": number + 100,
            "transaction_hash": "0x" + f"{number:064x}",
            "log_index": 0,
            "block_timestamp": "2026-01-01T00:00:00+00:00",
            "producer_version": "offline-fixture",
            "schema_version": "1.0.0",
            "ingested_at": "2026-01-01T00:00:00+00:00",
            "payload": {
                "pool_address": "0x" + "11" * 20,
                "raw_data": "0x",
                "raw_topics": [],
            },
        }
        publisher.produce(
            TOPIC,
            key=event["transaction_hash"],
            value=json.dumps(event),
            on_delivery=lambda error, _message: errors.append(error),
        )
    assert publisher.flush(30) == 0
    assert errors == [None] * count


def main():
    phase = sys.argv[1]
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if phase == "initial":
        for future in admin.create_topics(
            [NewTopic(TOPIC, num_partitions=1, replication_factor=1)]
        ).values():
            future.result(30)
        publish(0, 3)  # Must not be skipped on the first checkpoint.
    elif phase == "resume":
        assert kafka_end_offsets(BOOTSTRAP, TOPIC) == {"0": 3}, (
            "Kafka logs did not survive restart"
        )
        publish(3, 2)
    elif phase == "loss":
        for future in admin.delete_topics([TOPIC]).values():
            future.result(30)
        for _ in range(30):
            if TOPIC not in admin.list_topics(timeout=5).topics:
                break
            time.sleep(1)
        for future in admin.create_topics(
            [NewTopic(TOPIC, num_partitions=1, replication_factor=1)]
        ).values():
            future.result(30)
        publish(9, 1)

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("OfflinePilotIntegration")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    write_swaps_bronze.BRONZE_OUTPUT_PATH = OUTPUT
    query = start_query(spark, OUTPUT, CHECKPOINT, bootstrap=BOOTSTRAP, topic=TOPIC)
    try:
        if phase == "loss":
            try:
                query.processAllAvailable()
            except StreamingQueryException:
                print("KAFKA_OFFSET_LOSS_REJECTED", flush=True)
                return
            raise AssertionError("Missing Kafka offsets were silently accepted")
        query.processAllAvailable()
        rows = spark.read.parquet(OUTPUT).collect()
        expected = 3 if phase == "initial" else 5
        assert len(rows) == expected
        assert {(row.kafka_partition, row.kafka_offset) for row in rows} == {
            (0, n) for n in range(expected)
        }
        assert all(
            row.json_value and row.producer_version == "offline-fixture" for row in rows
        )
        verify_drained(query.lastProgress, kafka_end_offsets(BOOTSTRAP, TOPIC), TOPIC)
        commits = sorted(
            p.name for p in Path(CHECKPOINT, "commits").iterdir() if p.name.isdigit()
        )
        assert commits, "No durable checkpoint commit"
        if phase != "initial":
            progress = drain_query(
                query,
                lambda batch: Path(CHECKPOINT, "commits", str(batch)).exists(),
                get_offsets=lambda: kafka_end_offsets(BOOTSTRAP, TOPIC),
                topic=TOPIC,
            )
            write_status(progress, drained=True, path=Path("/test-data/status.json"))
            assert (
                json.loads(Path("/test-data/status.json").read_text())["drained"]
                is True
            )
        print(
            f"{phase.upper()}_VERIFIED rows={expected} commits={commits} progress_batch={query.lastProgress.get('batchId')} input_rows={query.lastProgress.get('numInputRows')}",
            flush=True,
        )
        if phase == "initial":
            # Deliberately skip graceful query/JVM shutdown after a committed
            # batch, then restart the broker and resume the same checkpoint.
            os._exit(0)
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
