"""Snowflake CAS persistence for pre-seeded durable alert state."""

from __future__ import annotations

import os
import re
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from orchestration.alerts import (
    AlertConcurrencyError,
    AlertReasonCode,
    AlertRecord,
    AlertSeverity,
    AlertState,
    DuplicateAlertKeyError,
    MONITORED_PIPELINES,
)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _identifier(value: str, setting: str) -> str:
    normalized = value.strip().upper()
    if not _IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError(f"{setting} must be a valid Snowflake identifier")
    return normalized


@dataclass(frozen=True, slots=True)
class AlertStateTableConfig:
    database: str = "BLOCKCHAIN_ANALYTICS"
    schema: str = "CONTROL"
    table: str = "ALERT_STATE"

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> AlertStateTableConfig:
        values = os.environ if environ is None else environ
        return cls(
            database=values.get("SNOWFLAKE_DATABASE", "BLOCKCHAIN_ANALYTICS"),
            schema=values.get("SNOWFLAKE_CONTROL_SCHEMA", "CONTROL"),
            table=values.get("SNOWFLAKE_ALERT_STATE_TABLE", "ALERT_STATE"),
        )

    @property
    def qualified_table(self) -> str:
        return ".".join(
            (
                _identifier(self.database, "SNOWFLAKE_DATABASE"),
                _identifier(self.schema, "SNOWFLAKE_CONTROL_SCHEMA"),
                _identifier(self.table, "SNOWFLAKE_ALERT_STATE_TABLE"),
            )
        )


class SnowflakeAlertStore:
    """Read exactly one row and update it with an optimistic state version."""

    def __init__(
        self,
        connection: Any,
        config: AlertStateTableConfig | None = None,
    ) -> None:
        self.connection = connection
        self.config = config or AlertStateTableConfig.from_env()

    def get(self, alert_key: str) -> AlertRecord:
        sql = f"""
            SELECT alert_key, check_name, pipeline_name, reason_code,
                   state, severity, opened_at, last_observed_at, resolved_at,
                   incident_generation, occurrence_count, current_value,
                   warning_threshold, stale_threshold, state_version
            FROM {self.config.qualified_table}
            WHERE alert_key = %s
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql, (alert_key,))
            rows = cursor.fetchall()
        if not rows:
            raise RuntimeError(
                f"Pre-seeded alert state is missing for {alert_key!r}"
            )
        if len(rows) != 1:
            raise DuplicateAlertKeyError(
                f"Duplicate durable alert state for {alert_key!r}"
            )
        row = rows[0]
        return AlertRecord(
            alert_key=str(row[0]),
            check_name=str(row[1]),
            pipeline_name=str(row[2]),
            reason_code=AlertReasonCode(str(row[3])),
            state=AlertState(str(row[4])),
            severity=AlertSeverity(str(row[5])) if row[5] else None,
            opened_at=_timestamp(row[6]),
            last_observed_at=_timestamp(row[7]),
            resolved_at=_timestamp(row[8]),
            incident_generation=int(row[9]),
            occurrence_count=int(row[10]),
            current_value=_integer(row[11]),
            warning_threshold=_integer(row[12]),
            stale_threshold=_integer(row[13]),
            state_version=int(row[14]),
        )

    def compare_and_set(
        self,
        expected_version: int,
        candidate: AlertRecord,
    ) -> AlertRecord:
        sql = f"""
            UPDATE {self.config.qualified_table}
            SET state = %s,
                severity = %s,
                opened_at = %s,
                last_observed_at = %s,
                resolved_at = %s,
                incident_generation = %s,
                occurrence_count = %s,
                current_value = %s,
                warning_threshold = %s,
                stale_threshold = %s,
                state_version = %s
            WHERE alert_key = %s
              AND state_version = %s
        """
        parameters = (
            candidate.state.value,
            candidate.severity.value if candidate.severity else None,
            candidate.opened_at,
            candidate.last_observed_at,
            candidate.resolved_at,
            candidate.incident_generation,
            candidate.occurrence_count,
            candidate.current_value,
            candidate.warning_threshold,
            candidate.stale_threshold,
            candidate.state_version,
            candidate.alert_key,
            expected_version,
        )
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql, parameters)
            if cursor.rowcount != 1:
                raise AlertConcurrencyError(
                    f"Alert state changed concurrently for {candidate.alert_key!r}"
                )
        return candidate


def latest_pipeline_statuses(connection: Any) -> dict[str, str]:
    """Return the latest terminal result for each monitored logical pipeline."""
    placeholders = ", ".join("%s" for _ in MONITORED_PIPELINES)
    sql = f"""
        SELECT pipeline_name, status
        FROM BLOCKCHAIN_ANALYTICS.CONTROL.PIPELINE_RUNS
        WHERE pipeline_name IN ({placeholders})
          AND status IN ('FAILED', 'SUCCEEDED', 'NO_OP')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY pipeline_name
            ORDER BY finished_at DESC, started_at DESC
        ) = 1
    """
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, MONITORED_PIPELINES)
        rows = cursor.fetchall()
    return {str(pipeline): str(status) for pipeline, status in rows}


def _timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError("Snowflake alert timestamp must be a datetime")
    return value


def _integer(value: Any) -> int | None:
    return int(value) if value is not None else None
