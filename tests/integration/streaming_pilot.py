"""Synthetic container integration. No chain provider, AWS, or credentials."""

import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Producer, TopicCollection
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
from runtime.recovery import RecoveryBlocked, assess_recovery

BOOTSTRAP = "kafka:9092"
TOPIC = "synthetic-pilot-events"
OUTPUT = "/test-data/bronze"
CHECKPOINT = "/test-data/checkpoint"
BASELINE = Path("/test-data/recovery-baseline.json")


def recovery_snapshot(admin):
    """Synthetic local volumes only; called with no Spark/producer process running."""
    commits = [
        int(p.name) for p in Path(CHECKPOINT, "commits").iterdir() if p.name.isdigit()
    ]
    batch = max(commits)
    sink_batches = [
        int(p.name.removesuffix(".compact"))
        for p in Path(OUTPUT, "_spark_metadata").iterdir()
        if p.name.removesuffix(".compact").isdigit()
    ]
    offset_lines = Path(CHECKPOINT, "offsets", str(batch)).read_text().splitlines()
    assert offset_lines[0] == "v1"
    topic = admin.describe_topics(TopicCollection([TOPIC]), request_timeout=15)[
        TOPIC
    ].result(20)
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": "offline-recovery-probe"}
    )
    try:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(TOPIC, 0), timeout=15, cached=False
        )
    finally:
        consumer.close()
    return {
        "schema_version": 1,
        "scope": {
            "storage_identity": "synthetic-compose-volumes:test-kafka:test-spark",
            "cluster_id": admin.list_topics(timeout=15).cluster_id,
            "topic": TOPIC,
            "topic_id": str(topic.topic_id),
            "output": OUTPUT,
            "checkpoint": CHECKPOINT,
            "query_id": json.loads(Path(CHECKPOINT, "metadata").read_text())["id"],
        },
        "checkpoint_batch": batch,
        "sink_batch": max(sink_batches),
        "checkpoint_end_offsets": json.loads(offset_lines[-1])[TOPIC],
        "kafka_begin_offsets": {"0": low},
        "kafka_end_offsets": {"0": high},
        "checkpoint_metadata_exists": True,
        "checkpoint_commit_exists": True,
        "sink_metadata_exists": True,
        "writer_active": False,
    }


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
    assert phase in {"initial", "snapshot", "resume", "repeat", "identity-loss", "loss"}
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if phase == "snapshot":
        BASELINE.write_text(json.dumps(recovery_snapshot(admin)))
        print("OFFLINE_RECOVERY_BASELINE_CAPTURED", flush=True)
        return
    if phase in {"resume", "repeat"}:
        result = assess_recovery(
            json.loads(BASELINE.read_text()), recovery_snapshot(admin)
        )
        assert result["cloud_start_authorized"] is False
        print("OFFLINE_RECOVERY_IDENTITY_AND_OFFSETS_VERIFIED", flush=True)
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
    elif phase in {"loss", "identity-loss"}:
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
        if phase == "identity-loss":
            baseline = json.loads(BASELINE.read_text())
            publish(100, baseline["kafka_end_offsets"]["0"])
            observed = recovery_snapshot(admin)
            # Equal offsets alone cannot detect this destructive topic replacement.
            assert observed["kafka_end_offsets"] == baseline["kafka_end_offsets"]
            assert observed["scope"]["topic_id"] != baseline["scope"]["topic_id"]
            try:
                assess_recovery(baseline, observed)
            except RecoveryBlocked as error:
                assert str(error) == "RECOVERY_IDENTITY_CHANGED"
                print(
                    "RECREATED_TOPIC_WITH_IDENTICAL_OFFSETS_REJECTED_BEFORE_SPARK",
                    flush=True,
                )
                return
            raise AssertionError("Recreated topic was accepted")
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
