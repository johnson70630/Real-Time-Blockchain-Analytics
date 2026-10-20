"""Snowflake persistence for durable pipeline watermarks."""

from __future__ import annotations

import os
import re
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Mapping

import snowflake.connector

from orchestration.watermark import (
    WatermarkCommitConflictError,
    WatermarkIdentity,
    WatermarkState,
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
class WatermarkTableConfig:
    """Non-secret names for the pre-provisioned control table."""

    database: str = "BLOCKCHAIN_ANALYTICS"
    schema: str = "CONTROL"
    table: str = "PIPELINE_WATERMARKS"

    def __post_init__(self) -> None:
        _identifier(self.database, "SNOWFLAKE_DATABASE")
        _identifier(self.schema, "SNOWFLAKE_CONTROL_SCHEMA")
        _identifier(self.table, "SNOWFLAKE_WATERMARK_TABLE")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> WatermarkTableConfig:
        """Load object names without reading credentials."""
        values = os.environ if environ is None else environ
        return cls(
            database=values.get(
                "SNOWFLAKE_DATABASE",
                "BLOCKCHAIN_ANALYTICS",
            ),
            schema=values.get("SNOWFLAKE_CONTROL_SCHEMA", "CONTROL"),
            table=values.get(
                "SNOWFLAKE_WATERMARK_TABLE",
                "PIPELINE_WATERMARKS",
            ),
        )

    @property
    def qualified_table(self) -> str:
        """Return the validated fully qualified control-table name."""
        return ".".join(
            (
                _identifier(self.database, "SNOWFLAKE_DATABASE"),
                _identifier(self.schema, "SNOWFLAKE_CONTROL_SCHEMA"),
                _identifier(self.table, "SNOWFLAKE_WATERMARK_TABLE"),
            )
        )


class SnowflakeWatermarkStore:
    """Persist one current progress row per independent pipeline identity."""

    def __init__(
        self,
        connection: Any,
        config: WatermarkTableConfig | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.connection = connection
        self.config = config or WatermarkTableConfig.from_env()
        self.clock = clock or (lambda: datetime.now(UTC))

    @staticmethod
    def _key_parameters(identity: WatermarkIdentity) -> tuple[Any, ...]:
        return (
            identity.pipeline_name,
            identity.source_name,
            identity.chain_id,
            identity.watermark_type,
        )

    def get(self, identity: WatermarkIdentity) -> WatermarkState | None:
        """Read exactly one current row, rejecting corrupt duplicate state."""
        sql = f"""
            SELECT committed_value, successful_run_id, updated_at
            FROM {self.config.qualified_table}
            WHERE pipeline_name = %s
              AND source_name = %s
              AND chain_id = %s
              AND watermark_type = %s
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql, self._key_parameters(identity))
            rows = cursor.fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError(
                f"Duplicate watermark state for {identity.pipeline_name!r}"
            )
        committed_value, run_id, updated_at = rows[0]
        return WatermarkState(
            identity=identity,
            committed_value=int(committed_value),
            successful_run_id=str(run_id),
            updated_at=updated_at,
        )

    def initialize(
        self,
        identity: WatermarkIdentity,
        value: int,
        successful_run_id: str,
    ) -> WatermarkState:
        """Insert explicitly selected first state in one transaction."""
        updated_at = self.clock()
        sql = f"""
            INSERT INTO {self.config.qualified_table} (
                pipeline_name,
                source_name,
                chain_id,
                watermark_type,
                committed_value,
                successful_run_id,
                updated_at
            )
            SELECT %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {self.config.qualified_table}
                WHERE pipeline_name = %s
                  AND source_name = %s
                  AND chain_id = %s
                  AND watermark_type = %s
            )
        """
        key = self._key_parameters(identity)
        parameters = (*key, value, successful_run_id, updated_at, *key)
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("BEGIN TRANSACTION")
            try:
                cursor.execute(sql, parameters)
                if cursor.rowcount != 1:
                    raise WatermarkCommitConflictError(
                        "Watermark was initialized concurrently"
                    )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise
        return WatermarkState(identity, value, successful_run_id, updated_at)

    def advance(
        self,
        identity: WatermarkIdentity,
        *,
        expected_value: int,
        candidate_value: int,
        successful_run_id: str,
    ) -> WatermarkState:
        """Advance with optimistic concurrency and a monotonic SQL predicate."""
        updated_at = self.clock()
        sql = f"""
            UPDATE {self.config.qualified_table}
            SET committed_value = %s,
                successful_run_id = %s,
                updated_at = %s
            WHERE pipeline_name = %s
              AND source_name = %s
              AND chain_id = %s
              AND watermark_type = %s
              AND committed_value = %s
              AND committed_value < %s
        """
        parameters = (
            candidate_value,
            successful_run_id,
            updated_at,
            *self._key_parameters(identity),
            expected_value,
            candidate_value,
        )
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("BEGIN TRANSACTION")
            try:
                cursor.execute(sql, parameters)
                if cursor.rowcount != 1:
                    raise WatermarkCommitConflictError(
                        "Watermark changed before the candidate could commit"
                    )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise
        return WatermarkState(
            identity,
            candidate_value,
            successful_run_id,
            updated_at,
        )

    def initialize_many(
        self,
        values: Mapping[WatermarkIdentity, int],
        successful_run_id: str,
    ) -> tuple[WatermarkState, ...]:
        """Initialize related identities atomically, rolling back all on conflict."""
        updated_at = self.clock()
        ordered = sorted(values, key=self._identity_sort_key)
        insert_sql = f"""
            INSERT INTO {self.config.qualified_table} (
                pipeline_name, source_name, chain_id, watermark_type,
                committed_value, successful_run_id, updated_at
            )
            SELECT %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM {self.config.qualified_table}
                WHERE pipeline_name = %s AND source_name = %s
                  AND chain_id = %s AND watermark_type = %s
            )
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("BEGIN TRANSACTION")
            try:
                for identity in ordered:
                    key = self._key_parameters(identity)
                    cursor.execute(
                        insert_sql,
                        (
                            *key,
                            values[identity],
                            successful_run_id,
                            updated_at,
                            *key,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise WatermarkCommitConflictError(
                            "Related watermark initialization conflicted"
                        )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise
        return tuple(
            WatermarkState(
                identity,
                values[identity],
                successful_run_id,
                updated_at,
            )
            for identity in ordered
        )

    def advance_many(
        self,
        expected_values: Mapping[WatermarkIdentity, int],
        candidate_values: Mapping[WatermarkIdentity, int],
        successful_run_id: str,
    ) -> tuple[WatermarkState, ...]:
        """Advance related identities in one optimistic Snowflake transaction."""
        if set(expected_values) != set(candidate_values):
            raise ValueError("Expected and candidate watermark identities must match")
        updated_at = self.clock()
        ordered = sorted(expected_values, key=self._identity_sort_key)
        select_sql = f"""
            SELECT committed_value, successful_run_id, updated_at
            FROM {self.config.qualified_table}
            WHERE pipeline_name = %s AND source_name = %s
              AND chain_id = %s AND watermark_type = %s
        """
        update_sql = f"""
            UPDATE {self.config.qualified_table}
            SET committed_value = %s,
                successful_run_id = %s,
                updated_at = %s
            WHERE pipeline_name = %s AND source_name = %s
              AND chain_id = %s AND watermark_type = %s
              AND committed_value = %s
              AND committed_value <= %s
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("BEGIN TRANSACTION")
            try:
                prior: dict[WatermarkIdentity, tuple[int, str, datetime]] = {}
                for identity in ordered:
                    expected = expected_values[identity]
                    cursor.execute(select_sql, self._key_parameters(identity))
                    rows = cursor.fetchall()
                    if len(rows) != 1 or int(rows[0][0]) != expected:
                        raise WatermarkCommitConflictError(
                            "Related watermark changed before atomic commit"
                        )
                    prior[identity] = (
                        int(rows[0][0]),
                        str(rows[0][1]),
                        rows[0][2],
                    )
                for identity in ordered:
                    expected = expected_values[identity]
                    candidate = candidate_values[identity]
                    if candidate == expected:
                        continue
                    cursor.execute(
                        update_sql,
                        (
                            candidate,
                            successful_run_id,
                            updated_at,
                            *self._key_parameters(identity),
                            expected,
                            candidate,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise WatermarkCommitConflictError(
                            "Related watermark changed before atomic commit"
                        )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise
        return tuple(
            (
                WatermarkState(
                    identity,
                    candidate_values[identity],
                    successful_run_id,
                    updated_at,
                )
                if candidate_values[identity] != expected_values[identity]
                else WatermarkState(identity, *prior[identity])
            )
            for identity in ordered
        )

    @staticmethod
    def _identity_sort_key(
        identity: WatermarkIdentity,
    ) -> tuple[str, str, int, str]:
        return (
            identity.pipeline_name,
            identity.source_name,
            identity.chain_id,
            identity.watermark_type,
        )


def connect_watermark_store(
    connection_name: str = DEFAULT_CONNECTION_NAME,
    warehouse: str = DEFAULT_WAREHOUSE,
) -> Any:
    """Connect using the pipeline role, JWT credentials, and no secondary roles."""
    validate_key_pair_connection_config(connection_name)
    connection = snowflake.connector.connect(
        connection_name=connection_name,
        role=PIPELINE_ROLE,
        warehouse=warehouse,
        application="BLOCKCHAIN_WATERMARK_CONTROL",
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
