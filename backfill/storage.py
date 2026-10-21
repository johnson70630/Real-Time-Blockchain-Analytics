"""Spark-backed persistence for canonical historical Aave Bronze events."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from backfill.aave_v3 import BackfillIssue, EventEnvelope, merge_events
from config.storage import join_location
from reference_data.storage import path_exists
from spark.event_schema import get_event_schema
from spark.parquet import (
    delete_dataset,
    write_completion_marker,
    write_partitioned_dataset_atomic,
)


_event_schema = get_event_schema()
_payload_schema = _event_schema["payload"].dataType

BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("event_date", DateType(), False),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_key", StringType(), True),
        StructField("json_value", StringType(), False),
        StructField("protocol", StringType(), False),
        StructField("chain", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("block_number", IntegerType(), False),
        StructField("transaction_hash", StringType(), False),
        StructField("log_index", IntegerType(), False),
        StructField("block_timestamp", TimestampType(), False),
        StructField("producer_version", StringType(), False),
        StructField("schema_version", StringType(), False),
        StructField("payload", _payload_schema, False),
        StructField("pool_address", StringType(), True),
        StructField("raw_data", StringType(), False),
        StructField("raw_topics", ArrayType(StringType()), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("bronze_processed_at", TimestampType(), False),
        StructField("bronze_file", StringType(), False),
    ]
)

_TIMESTAMP_FIELDS = frozenset(
    {
        "kafka_timestamp",
        "block_timestamp",
        "ingested_at",
        "bronze_processed_at",
    }
)
BRONZE_INPUT_SCHEMA = StructType(
    [
        StructField(
            field.name,
            StringType() if field.name in _TIMESTAMP_FIELDS else field.dataType,
            field.nullable,
        )
        for field in BRONZE_SCHEMA.fields
    ]
)

STATE_SCHEMA = StructType(
    [
        StructField("next_end_block", IntegerType(), False),
        StructField("last_head_block", IntegerType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

ISSUE_SCHEMA = StructType(
    [
        StructField("block_number", IntegerType(), True),
        StructField("transaction_hash", StringType(), True),
        StructField("log_index", IntegerType(), True),
        StructField("reason", StringType(), False),
        StructField("processed_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True, slots=True)
class BackfillState:
    next_end_block: int
    last_head_block: int
    updated_at: datetime


def _utc(value: datetime) -> datetime:
    # PySpark collects timestamp values as local-naive Python datetimes.
    # astimezone() interprets those in the host zone before converting to UTC.
    return value.astimezone(UTC)


def _event_frame(
    spark: SparkSession,
    events: tuple[EventEnvelope, ...],
    processed_at: datetime,
    bronze_file: str,
):
    rows = []
    for event in events:
        block_timestamp = datetime.fromisoformat(event.block_timestamp)
        ingested_at = datetime.fromisoformat(event.ingested_at)
        payload = {
            field.name: event.payload.get(field.name)
            for field in _payload_schema.fields
        }
        rows.append(
            (
                None,
                block_timestamp.date(),
                None,
                None,
                None,
                json.dumps(
                    event.to_dict(),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                event.protocol,
                event.chain,
                event.event_type,
                event.block_number,
                event.transaction_hash,
                event.log_index,
                block_timestamp.isoformat(),
                event.producer_version,
                event.schema_version,
                payload,
                event.payload.get("pool_address"),
                event.payload["raw_data"],
                event.payload["raw_topics"],
                ingested_at.isoformat(),
                processed_at.isoformat(),
                bronze_file,
            )
        )
    partition_count = max(1, min(256, (len(rows) + 249) // 250))
    row_rdd = spark.sparkContext.parallelize(rows, partition_count)
    frame = spark.createDataFrame(row_rdd, BRONZE_INPUT_SCHEMA)
    for field in _TIMESTAMP_FIELDS:
        frame = frame.withColumn(field, to_timestamp(field))
    return frame.orderBy("block_number", "log_index")


class AaveBackfillStore:
    """Atomically replace deduplicated historical Bronze and resume state."""

    def __init__(self, spark: SparkSession, *, bronze_path: str, state_path: str, quarantine_path: str) -> None:
        self.spark = spark
        self.bronze_path = bronze_path
        self.state_path = state_path
        self.quarantine_path = quarantine_path

    def load_events(self) -> tuple[EventEnvelope, ...]:
        if not path_exists(self.spark, self.bronze_path):
            return ()
        rows = self.spark.read.schema(BRONZE_SCHEMA).parquet(self.bronze_path).collect()
        events = tuple(
            EventEnvelope(
                protocol=row.protocol,
                chain=row.chain,
                event_type=row.event_type,
                block_number=row.block_number,
                transaction_hash=row.transaction_hash,
                log_index=row.log_index,
                block_timestamp=_utc(row.block_timestamp).isoformat(),
                ingested_at=_utc(row.ingested_at).isoformat(),
                payload=row.payload.asDict(recursive=True),
                producer_version=row.producer_version,
                schema_version=row.schema_version,
            )
            for row in rows
        )
        canonical = merge_events((), events)
        if len(canonical) != len(events):
            raise ValueError("Historical Aave Bronze contains duplicate events")
        return canonical

    def write_events(self, events: tuple[EventEnvelope, ...], processed_at: datetime) -> None:
        rows = []
        for event in events:
            block_timestamp = datetime.fromisoformat(event.block_timestamp)
            ingested_at = datetime.fromisoformat(event.ingested_at)
            partition = (
                f"{self.bronze_path}/protocol={event.protocol}/chain={event.chain}/"
                f"event_type={event.event_type}/event_date={block_timestamp.date()}"
            )
            payload = {field.name: event.payload.get(field.name) for field in _payload_schema.fields}
            rows.append(
                (
                    None,
                    block_timestamp.date(),
                    None,
                    None,
                    None,
                    json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":")),
                    event.protocol,
                    event.chain,
                    event.event_type,
                    event.block_number,
                    event.transaction_hash,
                    event.log_index,
                    block_timestamp.isoformat(),
                    event.producer_version,
                    event.schema_version,
                    payload,
                    None,
                    event.payload["raw_data"],
                    event.payload["raw_topics"],
                    ingested_at.isoformat(),
                    processed_at.isoformat(),
                    partition,
                )
            )
        frame = self.spark.createDataFrame(rows, BRONZE_INPUT_SCHEMA)
        for field in _TIMESTAMP_FIELDS:
            frame = frame.withColumn(field, to_timestamp(field))
        frame = frame.orderBy("block_number", "log_index")
        write_partitioned_dataset_atomic(
            frame,
            self.bronze_path,
            ("protocol", "chain", "event_type", "event_date"),
        )

    def load_state(self) -> BackfillState | None:
        if not path_exists(self.spark, self.state_path):
            return None
        rows = self.spark.read.schema(STATE_SCHEMA).parquet(self.state_path).collect()
        if len(rows) != 1:
            raise ValueError("Aave backfill state must contain exactly one row")
        row = rows[0]
        return BackfillState(row.next_end_block, row.last_head_block, _utc(row.updated_at))

    def write_state(self, state: BackfillState) -> None:
        frame = self.spark.createDataFrame(
            [(state.next_end_block, state.last_head_block, state.updated_at)],
            STATE_SCHEMA,
        )
        write_partitioned_dataset_atomic(frame, self.state_path, ())

    def write_issues(self, issues: tuple[BackfillIssue, ...]) -> None:
        rows = [
            (issue.block_number, issue.transaction_hash, issue.log_index, issue.reason, issue.processed_at)
            for issue in issues
        ]
        frame = self.spark.createDataFrame(rows, ISSUE_SCHEMA).orderBy(
            "block_number", "log_index"
        )
        write_partitioned_dataset_atomic(frame, self.quarantine_path, ())


class AaveForwardBatchStore:
    """Persist deterministic, independently retryable Aave forward batches."""

    def __init__(
        self,
        spark: SparkSession,
        *,
        bronze_path: str,
        quarantine_path: str,
    ) -> None:
        self.spark = spark
        self.bronze_path = bronze_path
        self.quarantine_path = quarantine_path

    @staticmethod
    def _batch_name(start_block: int, end_block: int) -> str:
        if start_block < 0 or end_block < start_block:
            raise ValueError("Forward batch range is invalid")
        return f"range_{start_block}_{end_block}"

    def write_and_validate(
        self,
        *,
        start_block: int,
        end_block: int,
        events: tuple[EventEnvelope, ...],
        issues: tuple[BackfillIssue, ...],
        processed_at: datetime,
    ) -> None:
        """Write one batch and verify counts, identities, and block bounds."""
        batch_name = self._batch_name(start_block, end_block)
        event_path = str(join_location(self.bronze_path, batch_name))
        issue_path = str(join_location(self.quarantine_path, batch_name))
        canonical = merge_events((), events)
        if len(canonical) != len(events):
            raise ValueError("Forward batch contains duplicate event identities")

        write_partitioned_dataset_atomic(
            _event_frame(
                self.spark,
                canonical,
                processed_at,
                event_path,
            ),
            event_path,
            (),
        )
        issue_rows = [
            (
                issue.block_number,
                issue.transaction_hash,
                issue.log_index,
                issue.reason,
                issue.processed_at,
            )
            for issue in issues
        ]
        issue_frame = self.spark.createDataFrame(
            issue_rows,
            ISSUE_SCHEMA,
        ).orderBy("block_number", "log_index")
        write_partitioned_dataset_atomic(issue_frame, issue_path, ())

        persisted = self.spark.read.schema(BRONZE_SCHEMA).parquet(event_path)
        if persisted.count() != len(canonical):
            raise RuntimeError("Forward Bronze persisted row count mismatch")
        if persisted.dropDuplicates(
            ("chain", "transaction_hash", "log_index")
        ).count() != len(canonical):
            raise RuntimeError("Forward Bronze contains duplicate identities")
        if persisted.filter(
            (persisted.block_number < start_block)
            | (persisted.block_number > end_block)
        ).count():
            raise RuntimeError("Forward Bronze contains out-of-range events")
        persisted_issues = self.spark.read.schema(ISSUE_SCHEMA).parquet(
            issue_path
        )
        if persisted_issues.count() != len(issues):
            raise RuntimeError("Forward quarantine persisted row count mismatch")


class UniswapRecoveryBatchStore(AaveForwardBatchStore):
    """Persist committed Uniswap recovery batches outside streaming Bronze."""

    def write_and_validate(
        self,
        *,
        start_block: int,
        end_block: int,
        events: tuple[EventEnvelope, ...],
        issues: tuple[BackfillIssue, ...],
        processed_at: datetime,
        contract_fingerprint: str,
        pool_count: int,
        event_topic_count: int,
        deduplication_paths: tuple[tuple[str, bool], ...] = (),
    ) -> None:
        """Write, validate, then expose an explicit complete-batch marker."""
        if len(contract_fingerprint) != 64:
            raise ValueError("Recovery contract fingerprint must be SHA-256")
        if pool_count <= 0 or event_topic_count <= 0:
            raise ValueError("Recovery contract counts must be positive")
        batch_name = self._batch_name(start_block, end_block)
        event_path = str(join_location(self.bronze_path, batch_name))
        issue_path = str(join_location(self.quarantine_path, batch_name))
        contract_marker = (
            f"_CONTRACT_{contract_fingerprint}_POOLS_{pool_count}_"
            f"TOPICS_{event_topic_count}"
        )
        committed_marker = str(join_location(event_path, "_COMMITTED"))
        if path_exists(self.spark, committed_marker):
            expected_contract = str(join_location(event_path, contract_marker))
            if not path_exists(self.spark, expected_contract):
                raise RuntimeError("Committed recovery batch contract mismatch")
            return
        if path_exists(self.spark, event_path):
            delete_dataset(self.spark, event_path)
        if path_exists(self.spark, issue_path):
            delete_dataset(self.spark, issue_path)

        canonical = merge_events((), events)
        if len(canonical) != len(events):
            raise ValueError("Recovery batch contains duplicate event identities")
        frame = _event_frame(
            self.spark,
            canonical,
            processed_at,
            event_path,
        )
        existing_keys = None
        for path, hive_partitioned in deduplication_paths:
            if not path_exists(self.spark, path):
                continue
            reader = self.spark.read.option("mergeSchema", "true")
            reader = (
                reader.option("basePath", path)
                if hive_partitioned
                else reader.option("recursiveFileLookup", "true")
            )
            keys = reader.parquet(path).select(
                "chain",
                "transaction_hash",
                "log_index",
            )
            existing_keys = (
                keys
                if existing_keys is None
                else existing_keys.unionByName(keys)
            )
        if existing_keys is not None:
            frame = frame.join(
                existing_keys.distinct(),
                on=["chain", "transaction_hash", "log_index"],
                how="left_anti",
            )
        expected_count = frame.count()
        write_partitioned_dataset_atomic(frame, event_path, ())

        issue_rows = [
            (
                issue.block_number,
                issue.transaction_hash,
                issue.log_index,
                issue.reason,
                issue.processed_at,
            )
            for issue in issues
        ]
        issue_frame = self.spark.createDataFrame(issue_rows, ISSUE_SCHEMA).orderBy(
            "block_number",
            "log_index",
        )
        write_partitioned_dataset_atomic(issue_frame, issue_path, ())

        persisted = self.spark.read.schema(BRONZE_SCHEMA).parquet(event_path)
        if persisted.count() != expected_count:
            raise RuntimeError("Recovery Bronze persisted row count mismatch")
        if persisted.dropDuplicates(
            ("chain", "transaction_hash", "log_index")
        ).count() != expected_count:
            raise RuntimeError("Recovery Bronze contains duplicate identities")
        if persisted.filter(
            (persisted.block_number < start_block)
            | (persisted.block_number > end_block)
        ).count():
            raise RuntimeError("Recovery Bronze contains out-of-range events")
        if self.spark.read.schema(ISSUE_SCHEMA).parquet(issue_path).count() != len(
            issues
        ):
            raise RuntimeError("Recovery quarantine persisted row count mismatch")
        write_completion_marker(
            persisted,
            event_path,
            marker_name=contract_marker,
        )
        write_completion_marker(persisted, event_path)
