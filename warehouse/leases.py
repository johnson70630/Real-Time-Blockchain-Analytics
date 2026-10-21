"""Versioned Snowflake leases for single-writer pipeline execution."""

from __future__ import annotations

import os
import re
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Mapping

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
DEFAULT_LEASE_TTL = timedelta(hours=2)


class PipelineLeaseError(RuntimeError):
    """Base exception for pipeline lease failures."""


class PipelineLeaseHeldError(PipelineLeaseError):
    """Raised when another run owns an unexpired lease."""


class PipelineLeaseConflictError(PipelineLeaseError):
    """Raised when a compare-and-set lease mutation loses a race."""


class PipelineLeaseOwnershipError(PipelineLeaseError):
    """Raised when a stale or non-owner run tries to mutate a lease."""


class PipelineLeaseStateError(PipelineLeaseError):
    """Raised when the pre-provisioned lease identity is missing or duplicated."""


def _identifier(value: str, setting: str) -> str:
    normalized = value.strip().upper()
    if not _IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError(f"{setting} must be a valid Snowflake identifier")
    return normalized


@dataclass(frozen=True, slots=True)
class PipelineLeaseIdentity:
    """Natural identity of one pre-provisioned pipeline lease row."""

    pipeline_name: str
    source_name: str
    chain_id: int

    def __post_init__(self) -> None:
        if not self.pipeline_name.strip() or not self.source_name.strip():
            raise ValueError("Lease pipeline and source names must not be empty")
        if self.chain_id <= 0:
            raise ValueError("Lease chain_id must be greater than zero")


@dataclass(frozen=True, slots=True)
class PipelineLeaseState:
    """Current durable state of one pipeline lease."""

    identity: PipelineLeaseIdentity
    owner_run_id: str | None
    acquired_at: datetime | None
    expires_at: datetime | None
    lease_version: int


@dataclass(frozen=True, slots=True)
class PipelineLeaseHandle:
    """Owner/version token required to renew or release a lease safely."""

    identity: PipelineLeaseIdentity
    owner_run_id: str
    acquired_at: datetime
    expires_at: datetime
    lease_version: int


@dataclass(frozen=True, slots=True)
class PipelineLeaseTableConfig:
    """Non-secret names for the pre-provisioned pipeline lease table."""

    database: str = "BLOCKCHAIN_ANALYTICS"
    schema: str = "CONTROL"
    table: str = "PIPELINE_RUN_LEASES"

    def __post_init__(self) -> None:
        _identifier(self.database, "SNOWFLAKE_DATABASE")
        _identifier(self.schema, "SNOWFLAKE_CONTROL_SCHEMA")
        _identifier(self.table, "SNOWFLAKE_RUN_LEASES_TABLE")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> PipelineLeaseTableConfig:
        """Load control-object names without reading credentials."""
        values = os.environ if environ is None else environ
        return cls(
            database=values.get("SNOWFLAKE_DATABASE", "BLOCKCHAIN_ANALYTICS"),
            schema=values.get("SNOWFLAKE_CONTROL_SCHEMA", "CONTROL"),
            table=values.get(
                "SNOWFLAKE_RUN_LEASES_TABLE",
                "PIPELINE_RUN_LEASES",
            ),
        )

    @property
    def qualified_table(self) -> str:
        """Return the validated fully qualified table name."""
        return ".".join(
            (
                _identifier(self.database, "SNOWFLAKE_DATABASE"),
                _identifier(self.schema, "SNOWFLAKE_CONTROL_SCHEMA"),
                _identifier(self.table, "SNOWFLAKE_RUN_LEASES_TABLE"),
            )
        )


class SnowflakePipelineLeaseStore:
    """Acquire and release pre-seeded leases with optimistic version checks.

    A two-hour TTL provides crash recovery for finite incremental jobs. Normal
    runs are comfortably shorter, so the current design deliberately has no
    heartbeat.
    """

    def __init__(
        self,
        connection: Any,
        config: PipelineLeaseTableConfig | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.connection = connection
        self.config = config or PipelineLeaseTableConfig.from_env()
        self.clock = clock or (lambda: datetime.now(UTC))

    @staticmethod
    def _key(identity: PipelineLeaseIdentity) -> tuple[Any, ...]:
        return (
            identity.pipeline_name,
            identity.source_name,
            identity.chain_id,
        )

    def get(self, identity: PipelineLeaseIdentity) -> PipelineLeaseState:
        """Read exactly one pre-provisioned lease row without changing it."""
        query = f"""
            SELECT owner_run_id, acquired_at, expires_at, lease_version
            FROM {self.config.qualified_table}
            WHERE pipeline_name = %s
              AND source_name = %s
              AND chain_id = %s
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(query, self._key(identity))
            rows = cursor.fetchall()
        return self._state_from_rows(identity, rows)

    def acquire(
        self,
        identity: PipelineLeaseIdentity,
        owner_run_id: str,
        *,
        ttl: timedelta = DEFAULT_LEASE_TTL,
    ) -> PipelineLeaseHandle:
        """Acquire an available or expired lease with compare-and-set semantics."""
        owner = owner_run_id.strip()
        if not owner:
            raise ValueError("owner_run_id must not be empty")
        if ttl <= timedelta(0):
            raise ValueError("Lease TTL must be greater than zero")
        now = self._aware_now()
        expires_at = now + ttl
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("BEGIN TRANSACTION")
            try:
                state = self._get_with_cursor(cursor, identity)
                if self._is_active(state, now):
                    raise PipelineLeaseHeldError(
                        self._held_message(state, now)
                    )
                new_version = state.lease_version + 1
                cursor.execute(
                    self._acquire_query(),
                    (
                        owner,
                        now,
                        expires_at,
                        new_version,
                        *self._key(identity),
                        state.lease_version,
                        now,
                    ),
                )
                if cursor.rowcount != 1:
                    current = self._get_with_cursor(cursor, identity)
                    raise PipelineLeaseConflictError(
                        "Pipeline lease changed during acquisition; current "
                        f"owner={current.owner_run_id!r} "
                        f"version={current.lease_version}"
                    )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise
        return PipelineLeaseHandle(
            identity=identity,
            owner_run_id=owner,
            acquired_at=now,
            expires_at=expires_at,
            lease_version=new_version,
        )

    def renew(
        self,
        handle: PipelineLeaseHandle,
        *,
        ttl: timedelta = DEFAULT_LEASE_TTL,
    ) -> PipelineLeaseHandle:
        """Extend a current owner token; stale owners cannot mutate a takeover."""
        if ttl <= timedelta(0):
            raise ValueError("Lease TTL must be greater than zero")
        now = self._aware_now()
        expires_at = now + ttl
        new_version = handle.lease_version + 1
        query = f"""
            UPDATE {self.config.qualified_table}
            SET expires_at = %s,
                lease_version = %s
            WHERE pipeline_name = %s
              AND source_name = %s
              AND chain_id = %s
              AND owner_run_id = %s
              AND lease_version = %s
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(
                query,
                (
                    expires_at,
                    new_version,
                    *self._key(handle.identity),
                    handle.owner_run_id,
                    handle.lease_version,
                ),
            )
            if cursor.rowcount != 1:
                self.connection.rollback()
                raise PipelineLeaseOwnershipError(
                    "Lease renewal rejected because owner or version changed"
                )
            self.connection.commit()
        return PipelineLeaseHandle(
            identity=handle.identity,
            owner_run_id=handle.owner_run_id,
            acquired_at=handle.acquired_at,
            expires_at=expires_at,
            lease_version=new_version,
        )

    def release(self, handle: PipelineLeaseHandle) -> PipelineLeaseState:
        """Release only the exact owner/version token returned by acquisition."""
        new_version = handle.lease_version + 1
        query = f"""
            UPDATE {self.config.qualified_table}
            SET owner_run_id = NULL,
                acquired_at = NULL,
                expires_at = NULL,
                lease_version = %s
            WHERE pipeline_name = %s
              AND source_name = %s
              AND chain_id = %s
              AND owner_run_id = %s
              AND lease_version = %s
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(
                query,
                (
                    new_version,
                    *self._key(handle.identity),
                    handle.owner_run_id,
                    handle.lease_version,
                ),
            )
            if cursor.rowcount != 1:
                self.connection.rollback()
                raise PipelineLeaseOwnershipError(
                    "Lease release rejected because owner or version changed"
                )
            self.connection.commit()
        return PipelineLeaseState(handle.identity, None, None, None, new_version)

    def _get_with_cursor(
        self,
        cursor: Any,
        identity: PipelineLeaseIdentity,
    ) -> PipelineLeaseState:
        query = f"""
            SELECT owner_run_id, acquired_at, expires_at, lease_version
            FROM {self.config.qualified_table}
            WHERE pipeline_name = %s
              AND source_name = %s
              AND chain_id = %s
        """
        cursor.execute(query, self._key(identity))
        return self._state_from_rows(identity, cursor.fetchall())

    def _acquire_query(self) -> str:
        return f"""
            UPDATE {self.config.qualified_table}
            SET owner_run_id = %s,
                acquired_at = %s,
                expires_at = %s,
                lease_version = %s
            WHERE pipeline_name = %s
              AND source_name = %s
              AND chain_id = %s
              AND lease_version = %s
              AND (owner_run_id IS NULL OR expires_at <= %s)
        """

    @staticmethod
    def _state_from_rows(
        identity: PipelineLeaseIdentity,
        rows: list[tuple[Any, ...]],
    ) -> PipelineLeaseState:
        if len(rows) != 1:
            raise PipelineLeaseStateError(
                "Expected exactly one pre-provisioned lease row for "
                f"{identity.pipeline_name}/{identity.source_name}/{identity.chain_id}; "
                f"found {len(rows)}"
            )
        owner, acquired_at, expires_at, version = rows[0]
        return PipelineLeaseState(
            identity=identity,
            owner_run_id=None if owner is None else str(owner),
            acquired_at=acquired_at,
            expires_at=expires_at,
            lease_version=int(version),
        )

    @staticmethod
    def _is_active(state: PipelineLeaseState, now: datetime) -> bool:
        return (
            state.owner_run_id is not None
            and state.expires_at is not None
            and state.expires_at > now
        )

    @staticmethod
    def _held_message(state: PipelineLeaseState, now: datetime) -> str:
        return (
            "Pipeline lease is held by another run "
            f"until {state.expires_at}; checked at {now}"
        )

    def _aware_now(self) -> datetime:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Lease clock must return a timezone-aware datetime")
        return now.astimezone(UTC)
