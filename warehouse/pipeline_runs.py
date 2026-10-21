"""Snowflake persistence for durable provider-neutral pipeline run metrics."""

from __future__ import annotations

import json
import os
import re
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

import snowflake.connector

from orchestration.observability import (
    DuplicateRunError,
    ErrorStage,
    PipelineRunRecord,
    RunIdentity,
    RunMetrics,
    RunStateConflictError,
    RunStatus,
    RunType,
)
from warehouse.load_raw import (
    DEFAULT_CONNECTION_NAME,
    DEFAULT_WAREHOUSE,
    PIPELINE_ROLE,
    validate_key_pair_connection_config,
)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _identifier(value: str, setting: str) -> str:
    normalized = value.strip().upper()
    if not _IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError(f"{setting} must be a valid Snowflake identifier")
    return normalized


@dataclass(frozen=True, slots=True)
class PipelineRunTableConfig:
    """Non-secret names for the pre-provisioned observability table."""

    database: str = "BLOCKCHAIN_ANALYTICS"
    schema: str = "CONTROL"
    table: str = "PIPELINE_RUNS"

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> PipelineRunTableConfig:
        values = os.environ if environ is None else environ
        return cls(
            database=values.get("SNOWFLAKE_DATABASE", "BLOCKCHAIN_ANALYTICS"),
            schema=values.get("SNOWFLAKE_CONTROL_SCHEMA", "CONTROL"),
            table=values.get("SNOWFLAKE_PIPELINE_RUNS_TABLE", "PIPELINE_RUNS"),
        )

    @property
    def qualified_table(self) -> str:
        return ".".join(
            (
                _identifier(self.database, "SNOWFLAKE_DATABASE"),
                _identifier(self.schema, "SNOWFLAKE_CONTROL_SCHEMA"),
                _identifier(self.table, "SNOWFLAKE_PIPELINE_RUNS_TABLE"),
            )
        )


class SnowflakePipelineRunStore:
    """Create and finish one row per stable pipeline/run identity."""

    def __init__(
        self,
        connection: Any,
        config: PipelineRunTableConfig | None = None,
    ) -> None:
        self.connection = connection
        self.config = config or PipelineRunTableConfig.from_env()

    @staticmethod
    def _key(identity: RunIdentity) -> tuple[str, str]:
        return identity.pipeline_name, identity.run_id

    def start(self, record: PipelineRunRecord) -> PipelineRunRecord:
        if record.status is not RunStatus.RUNNING:
            raise ValueError("A new pipeline run must start in RUNNING state")
        table = self.config.qualified_table
        sql = f"""
            INSERT INTO {table} (
                pipeline_name, run_id, run_type, started_at, status,
                metric_details
            )
            SELECT %s, %s, %s, %s, %s, PARSE_JSON('{{}}')
            WHERE NOT EXISTS (
                SELECT 1 FROM {table}
                WHERE pipeline_name = %s AND run_id = %s
            )
        """
        key = self._key(record.identity)
        parameters = (
            *key,
            record.run_type.value,
            record.started_at,
            record.status.value,
            *key,
        )
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql, parameters)
            if cursor.rowcount != 1:
                raise DuplicateRunError(
                    f"Pipeline run {record.identity.pipeline_name!r}/"
                    f"{record.identity.run_id!r} already exists"
                )
        return record

    def finish(self, record: PipelineRunRecord) -> PipelineRunRecord:
        if record.status is RunStatus.RUNNING:
            raise ValueError("finish requires a terminal run record")
        metrics = record.metrics
        sql = f"""
            UPDATE {self.config.qualified_table}
            SET finished_at = %s,
                status = %s,
                source_start_position = %s,
                source_end_position = %s,
                source_head_position = %s,
                safe_head_position = %s,
                watermark_before = %s,
                watermark_after = %s,
                made_progress = %s,
                records_read = %s,
                records_written = %s,
                records_inserted = %s,
                records_updated = %s,
                records_quarantined = %s,
                retry_count = %s,
                duration_seconds = %s,
                error_stage = %s,
                error_class = %s,
                metric_details = PARSE_JSON(%s)
            WHERE pipeline_name = %s
              AND run_id = %s
              AND status = 'RUNNING'
        """
        parameters = (
            record.finished_at,
            record.status.value,
            metrics.source_start_position,
            metrics.source_end_position,
            metrics.source_head_position,
            metrics.safe_head_position,
            metrics.watermark_before,
            metrics.watermark_after,
            metrics.made_progress,
            metrics.records_read,
            metrics.records_written,
            metrics.records_inserted,
            metrics.records_updated,
            metrics.records_quarantined,
            metrics.retry_count,
            record.duration_seconds,
            record.error_stage.value if record.error_stage else None,
            record.error_class,
            json.dumps(metrics.details, sort_keys=True),
            *self._key(record.identity),
        )
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql, parameters)
            if cursor.rowcount != 1:
                raise RunStateConflictError(
                    "Pipeline run is missing, duplicated, or already terminal"
                )
        return record

    def increment_metrics(
        self,
        identity: RunIdentity,
        metrics: RunMetrics,
    ) -> None:
        """Atomically add child-process work to one still-running parent run."""
        copy_files = int(metrics.details.get("copy_files", 0))
        sql = f"""
            UPDATE {self.config.qualified_table}
            SET records_read = records_read + %s,
                records_written = records_written + %s,
                records_inserted = records_inserted + %s,
                records_updated = records_updated + %s,
                records_quarantined = records_quarantined + %s,
                retry_count = retry_count + %s,
                metric_details = OBJECT_INSERT(
                    metric_details,
                    'copy_files',
                    COALESCE(metric_details:copy_files::NUMBER, 0) + %s,
                    TRUE
                )
            WHERE pipeline_name = %s
              AND run_id = %s
              AND status = 'RUNNING'
        """
        parameters = (
            metrics.records_read,
            metrics.records_written,
            metrics.records_inserted,
            metrics.records_updated,
            metrics.records_quarantined,
            metrics.retry_count,
            copy_files,
            *self._key(identity),
        )
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql, parameters)
            if cursor.rowcount != 1:
                raise RunStateConflictError(
                    "Running parent record is unavailable for metric aggregation"
                )

    def get(self, identity: RunIdentity) -> PipelineRunRecord | None:
        sql = f"""
            SELECT run_type, started_at, finished_at, status,
                   source_start_position, source_end_position,
                   source_head_position, safe_head_position,
                   watermark_before, watermark_after,
                   records_read, records_written, records_inserted,
                   records_updated, records_quarantined, retry_count,
                   duration_seconds, error_stage, error_class, metric_details
            FROM {self.config.qualified_table}
            WHERE pipeline_name = %s AND run_id = %s
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql, self._key(identity))
            rows = cursor.fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise RunStateConflictError("Duplicate pipeline run identity rows")
        row = rows[0]
        details = row[19] or {}
        if isinstance(details, str):
            details = json.loads(details)
        metrics = RunMetrics(
            source_start_position=_optional_int(row[4]),
            source_end_position=_optional_int(row[5]),
            source_head_position=_optional_int(row[6]),
            safe_head_position=_optional_int(row[7]),
            watermark_before=_optional_int(row[8]),
            watermark_after=_optional_int(row[9]),
            records_read=int(row[10]),
            records_written=int(row[11]),
            records_inserted=int(row[12]),
            records_updated=int(row[13]),
            records_quarantined=int(row[14]),
            retry_count=int(row[15]),
            details=details,
        )
        status = RunStatus(str(row[3]))
        return PipelineRunRecord(
            identity=identity,
            run_type=RunType(str(row[0])),
            started_at=_timestamp(row[1]),
            finished_at=_timestamp(row[2]) if row[2] is not None else None,
            status=status,
            duration_seconds=(float(row[16]) if row[16] is not None else None),
            metrics=metrics,
            error_stage=ErrorStage(str(row[17])) if row[17] else None,
            error_class=str(row[18]) if row[18] else None,
        )


def _optional_int(value: Any) -> int | None:
    return int(value) if value is not None else None


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("Snowflake timestamp result must be a datetime")
    return value


def connect_pipeline_run_store(
    connection_name: str = DEFAULT_CONNECTION_NAME,
    warehouse: str = DEFAULT_WAREHOUSE,
) -> Any:
    """Connect with the existing pipeline role and disable secondary roles."""
    validate_key_pair_connection_config(connection_name)
    connection = snowflake.connector.connect(
        connection_name=connection_name,
        role=PIPELINE_ROLE,
        warehouse=warehouse,
        application="BLOCKCHAIN_PIPELINE_OBSERVABILITY",
    )
    try:
        with closing(connection.cursor()) as cursor:
            cursor.execute("USE SECONDARY ROLES NONE")
            cursor.execute("SELECT CURRENT_ROLE()")
            current_role = str(cursor.fetchone()[0]).upper()
        if current_role != PIPELINE_ROLE:
            raise RuntimeError(
                f"Expected Snowflake role {PIPELINE_ROLE}, found {current_role}"
            )
    except Exception:
        connection.close()
        raise
    return connection
