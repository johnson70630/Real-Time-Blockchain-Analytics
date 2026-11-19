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
from runtime.recovery import RecoveryBlocked
from runtime.recovery_metadata import (
    LocalMetadataSource,
    MetadataCollector,
    KafkaMetadataProbe,
    digest,
)
from runtime.recovery_operator import preview, run_offline
from uuid import uuid4

BOOTSTRAP = "kafka:9092"
TOPIC = "synthetic-pilot-events"
OUTPUT = "/test-data/bronze"
CHECKPOINT = "/test-data/checkpoint"
BASELINE = Path("/test-data/recovery-baseline.json")


def recovery_collector(writer_active=lambda: False):
    return MetadataCollector(
        LocalMetadataSource("/test-data"),
        KafkaMetadataProbe(TOPIC),
        lambda: "synthetic-compose-volumes:test-kafka:test-spark",
        writer_active,
    )


def verify_rows(spark, expected):
    rows = spark.read.parquet(OUTPUT).collect()
    assert len(rows) == expected
    assert {(row.kafka_partition, row.kafka_offset) for row in rows} == {
        (0, n) for n in range(expected)
    }
    assert all(
        row.json_value and row.producer_version == "offline-fixture" for row in rows
    )


class OfflineRecoveryBackend:
    profile = "synthetic-offline"

    def __init__(self, spark, phase):
        self.spark, self.phase, self.query = spark, phase, None

    def active(self):
        return self.query is not None and self.query.isActive

    def start_reader(self):
        self.query = start_query(
            self.spark, OUTPUT, CHECKPOINT, bootstrap=BOOTSTRAP, topic=TOPIC
        )
        return True

    def start_source(self):
        if self.phase == "resume":
            publish(3, 2)
        return True

    def stop_source(self):
        return True  # publish waits for all synthetic delivery acknowledgements.

    def drain_reader(self):
        # On a resumed idle query, processAllAvailable may return before Spark
        # publishes its first progress event. Never treat missing progress as a
        # verified drain; wait within a bounded interval for real offsets.
        self.query.processAllAvailable()
        deadline = time.monotonic() + 45
        ends = kafka_end_offsets(BOOTSTRAP, TOPIC)
        while True:
            try:
                verify_drained(self.query.lastProgress or {}, ends, TOPIC)
                break
            except (RuntimeError, AttributeError, TypeError):
                pass
            if not self.query.isActive or time.monotonic() >= deadline:
                raise RuntimeError("OFFLINE_PROGRESS_NOT_OBSERVED")
            self.query.awaitTermination(0.5)
        progress = drain_query(
            self.query,
            lambda batch: Path(CHECKPOINT, "commits", str(batch)).exists(),
            get_offsets=lambda: kafka_end_offsets(BOOTSTRAP, TOPIC),
            topic=TOPIC,
        )
        write_status(progress, drained=True, path=Path("/test-data/status.json"))
        return True

    def stop_reader(self):
        if self.query is not None:
            self.query.stop()
        return not self.active()

    def verify_output(self, before, after):
        verify_rows(self.spark, 5)
        return after["checkpoint_end_offsets"] == {"0": 5}


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
        envelope = recovery_collector().collect()
        BASELINE.write_text(json.dumps(envelope))
        BASELINE.with_suffix(".sha256").write_text(digest(envelope))
        print("OFFLINE_RECOVERY_BASELINE_CAPTURED", flush=True)
        return
    if phase in {"resume", "repeat"}:
        result = preview(
            json.loads(BASELINE.read_text()),
            recovery_collector().collect(),
            BASELINE.with_suffix(".sha256").read_text(),
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
        # The recovery operator publishes only after checking and starting its reader.
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
            publish(100, baseline["snapshot"]["kafka_end_offsets"]["0"])
            observed = recovery_collector().collect()
            # Equal offsets alone cannot detect this destructive topic replacement.
            assert (
                observed["snapshot"]["kafka_end_offsets"]
                == baseline["snapshot"]["kafka_end_offsets"]
            )
            assert (
                observed["snapshot"]["scope"]["topic_id"]
                != baseline["snapshot"]["scope"]["topic_id"]
            )
            try:
                preview(baseline, observed, BASELINE.with_suffix(".sha256").read_text())
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
    if phase in {"resume", "repeat"}:
        backend = OfflineRecoveryBackend(spark, phase)
        try:
            result = run_offline(
                json.loads(BASELINE.read_text()),
                BASELINE.with_suffix(".sha256").read_text(),
                recovery_collector(backend.active),
                backend,
                str(uuid4()),
            )
            assert result["status"] == "OFFLINE_RECOVERY_COMPLETED"
            assert result["cloud_start_authorized"] is False
            print(
                f"{phase.upper()}_VERIFIED rows=5 operator=OFFLINE_RECOVERY_COMPLETED",
                flush=True,
            )
            return
        finally:
            backend.stop_reader()
            spark.stop()
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
        expected = 3 if phase == "initial" else 5
        verify_rows(spark, expected)
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
