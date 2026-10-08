"""Spark-backed persistence for historical Chainlink Bronze observations."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from backfill.chainlink import HistoricalIssue, merge_observations
from market_data.models import MarketDataObservation
from reference_data.storage import path_exists
from spark.parquet import write_partitioned_dataset_atomic

BRONZE_SCHEMA = StructType(
    [
        StructField("source_topic", StringType(), False),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_key", StringType(), True),
        StructField("json_value", StringType(), False),
        StructField("observation_date", DateType(), False),
        StructField("observation_id", StringType(), False),
        StructField("protocol", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("chain", StringType(), False),
        StructField("feed_address", StringType(), False),
        StructField("base_asset", StringType(), False),
        StructField("quote_asset", StringType(), False),
        StructField("round_id", StringType(), False),
        StructField("answer_raw", StringType(), False),
        StructField("feed_decimals", IntegerType(), False),
        StructField("feed_updated_at", TimestampType(), False),
        StructField("observed_at", TimestampType(), False),
        StructField("block_number", LongType(), False),
        StructField("block_timestamp", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("producer_version", StringType(), False),
        StructField("schema_version", StringType(), False),
        StructField("bronze_processed_at", TimestampType(), False),
        StructField("bronze_file", StringType(), False),
    ]
)

_TIMESTAMP_FIELDS = frozenset(
    {
        "feed_updated_at",
        "observed_at",
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
        StructField("feed_address", StringType(), False),
        StructField("phase_id", IntegerType(), False),
        StructField("start_round_id", StringType(), False),
        StructField("end_round_id", StringType(), False),
        StructField("next_round_id", StringType(), False),
        StructField("from_block", LongType(), False),
        StructField("to_block", LongType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

ISSUE_SCHEMA = StructType(
    [
        StructField("feed_address", StringType(), False),
        StructField("round_id", StringType(), False),
        StructField("reason", StringType(), False),
        StructField("processed_at", TimestampType(), False),
        StructField("retryable", BooleanType(), False),
    ]
)


@dataclass(frozen=True, slots=True)
class ChainlinkBackfillState:
    """Durable progress for one feed-proxy phase."""

    feed_address: str
    phase_id: int
    start_round_id: int
    end_round_id: int
    next_round_id: int
    from_block: int
    to_block: int
    updated_at: datetime

    @property
    def key(self) -> tuple[str, int]:
        return self.feed_address.lower(), self.phase_id


def _as_utc(value: datetime) -> datetime:
    # PySpark collects timestamp values as host-local naive datetimes.
    # astimezone() applies that host offset before normalizing to UTC.
    return value.astimezone(UTC)


class ChainlinkBackfillStore:
    """Persist exact historical observations, progress, and rejected rounds."""

    def __init__(
        self,
        spark: SparkSession,
        *,
        bronze_path: str,
        state_path: str,
        quarantine_path: str,
    ) -> None:
        self.spark = spark
        self.bronze_path = bronze_path
        self.state_path = state_path
        self.quarantine_path = quarantine_path

    def load_observations(self) -> tuple[MarketDataObservation, ...]:
        """Load canonical observations and reject duplicate stored identities."""
        if not path_exists(self.spark, self.bronze_path):
            return ()
        rows = self.spark.read.schema(BRONZE_SCHEMA).parquet(self.bronze_path).collect()
        observations = tuple(
            MarketDataObservation(
                observation_id=row.observation_id,
                protocol=row.protocol,
                event_type=row.event_type,
                chain=row.chain,
                feed_address=row.feed_address,
                base_asset=row.base_asset,
                quote_asset=row.quote_asset,
                round_id=row.round_id,
                answer_raw=row.answer_raw,
                feed_decimals=row.feed_decimals,
                feed_updated_at=_as_utc(row.feed_updated_at).isoformat(),
                observed_at=_as_utc(row.observed_at).isoformat(),
                block_number=row.block_number,
                block_timestamp=_as_utc(row.block_timestamp).isoformat(),
                ingested_at=_as_utc(row.ingested_at).isoformat(),
                producer_version=row.producer_version,
                schema_version=row.schema_version,
            )
            for row in rows
        )
        canonical = merge_observations((), observations)
        if len(canonical) != len(observations):
            raise ValueError("Historical Chainlink Bronze contains duplicate observations")
        return canonical

    def write_observations(
        self,
        observations: tuple[MarketDataObservation, ...],
        processed_at: datetime,
    ) -> None:
        """Atomically replace deterministic historical Bronze partitions."""
        rows = []
        for observation in observations:
            values = observation.to_dict()
            updated_at = datetime.fromisoformat(observation.feed_updated_at)
            partition = (
                f"{self.bronze_path}/chain={observation.chain}/"
                f"observation_date={updated_at.date()}"
            )
            rows.append(
                (
                    "historical_chainlink_rpc",
                    None,
                    None,
                    None,
                    None,
                    json.dumps(values, sort_keys=True, separators=(",", ":")),
                    updated_at.date(),
                    *tuple(values[field] for field in values),
                    processed_at.isoformat(),
                    partition,
                )
            )
        frame = self.spark.createDataFrame(rows, BRONZE_INPUT_SCHEMA)
        for field in _TIMESTAMP_FIELDS:
            frame = frame.withColumn(field, to_timestamp(field))
        frame = frame.orderBy("feed_address", "round_id")
        write_partitioned_dataset_atomic(
            frame,
            self.bronze_path,
            ("chain", "observation_date"),
        )

    def load_state(self) -> tuple[ChainlinkBackfillState, ...]:
        """Load phase checkpoints, or return no progress for a first run."""
        if not path_exists(self.spark, self.state_path):
            return ()
        rows = self.spark.read.schema(STATE_SCHEMA).parquet(self.state_path).collect()
        states = tuple(
            ChainlinkBackfillState(
                feed_address=row.feed_address,
                phase_id=row.phase_id,
                start_round_id=int(row.start_round_id),
                end_round_id=int(row.end_round_id),
                next_round_id=int(row.next_round_id),
                from_block=row.from_block,
                to_block=row.to_block,
                updated_at=_as_utc(row.updated_at),
            )
            for row in rows
        )
        if len({state.key for state in states}) != len(states):
            raise ValueError("Chainlink backfill state contains duplicate feed phases")
        return states

    def write_state(self, states: tuple[ChainlinkBackfillState, ...]) -> None:
        """Atomically persist deterministic per-phase checkpoints."""
        rows = [
            (
                state.feed_address.lower(),
                state.phase_id,
                str(state.start_round_id),
                str(state.end_round_id),
                str(state.next_round_id),
                state.from_block,
                state.to_block,
                state.updated_at,
            )
            for state in sorted(states, key=lambda item: item.key)
        ]
        frame = self.spark.createDataFrame(rows, STATE_SCHEMA)
        write_partitioned_dataset_atomic(frame, self.state_path, ())

    def write_issues(self, issues: tuple[HistoricalIssue, ...]) -> None:
        """Persist rejected historical rounds without exposing payload data."""
        rows = [
            (
                issue.feed_address,
                issue.round_id,
                issue.reason,
                issue.processed_at,
                issue.retryable,
            )
            for issue in issues
        ]
        frame = self.spark.createDataFrame(rows, ISSUE_SCHEMA).orderBy(
            "feed_address", "round_id"
        )
        write_partitioned_dataset_atomic(frame, self.quarantine_path, ())

    def load_issues(self) -> tuple[HistoricalIssue, ...]:
        """Load previously rejected rounds so bounded runs retain their audit."""
        if not path_exists(self.spark, self.quarantine_path):
            return ()
        rows = self.spark.read.schema(ISSUE_SCHEMA).parquet(
            self.quarantine_path
        ).collect()
        return tuple(
            HistoricalIssue(
                feed_address=row.feed_address,
                round_id=row.round_id,
                reason=row.reason,
                processed_at=_as_utc(row.processed_at),
                retryable=(
                    bool(row.retryable)
                    if row.retryable is not None
                    else "429" in row.reason
                ),
            )
            for row in rows
        )
