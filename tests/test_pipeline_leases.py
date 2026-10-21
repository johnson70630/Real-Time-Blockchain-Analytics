"""Tests for shared single-writer leases and completed-range concurrency."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from orchestration.aave_forward import CompletedBlockRange
from orchestration.uniswap_recovery import UNISWAP_FORWARD_LEASE
from orchestration.watermark import UNISWAP_FORWARD_WATERMARK
from warehouse.completed_ranges import SnowflakeCompletedRangeStore
from warehouse.leases import (
    DEFAULT_LEASE_TTL,
    PipelineLeaseHeldError,
    PipelineLeaseOwnershipError,
    PipelineLeaseState,
    PipelineLeaseTableConfig,
    SnowflakePipelineLeaseStore,
)

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class LeaseMemoryCursor:
    def __init__(self, connection: LeaseMemoryConnection) -> None:
        self.connection = connection
        self.rowcount = 0
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> LeaseMemoryCursor:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def close(self) -> None:
        return None

    def execute(
        self,
        query: str,
        parameters: tuple[Any, ...] | None = None,
    ) -> None:
        normalized = " ".join(query.split()).upper()
        values = parameters or ()
        if normalized in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"}:
            return
        if normalized.startswith("SELECT OWNER_RUN_ID"):
            state = self.connection.state
            self.rows = [
                (
                    state.owner_run_id,
                    state.acquired_at,
                    state.expires_at,
                    state.lease_version,
                )
            ]
            return
        state = self.connection.state
        if "SET OWNER_RUN_ID = %S" in normalized:
            owner, acquired, expires, version = values[:4]
            expected_version, checked_at = values[-2:]
            available = (
                state.owner_run_id is None
                or state.expires_at is not None
                and state.expires_at <= checked_at
            )
            if state.lease_version == expected_version and available:
                self.connection.state = PipelineLeaseState(
                    state.identity,
                    owner,
                    acquired,
                    expires,
                    version,
                )
                self.rowcount = 1
            else:
                self.rowcount = 0
            return
        if "SET EXPIRES_AT = %S" in normalized:
            expires, version = values[:2]
            owner, expected_version = values[-2:]
            if (
                state.owner_run_id == owner
                and state.lease_version == expected_version
            ):
                self.connection.state = replace(
                    state,
                    expires_at=expires,
                    lease_version=version,
                )
                self.rowcount = 1
            else:
                self.rowcount = 0
            return
        if "SET OWNER_RUN_ID = NULL" in normalized:
            version = values[0]
            owner, expected_version = values[-2:]
            if (
                state.owner_run_id == owner
                and state.lease_version == expected_version
            ):
                self.connection.state = PipelineLeaseState(
                    state.identity,
                    None,
                    None,
                    None,
                    version,
                )
                self.rowcount = 1
            else:
                self.rowcount = 0
            return
        raise AssertionError(f"Unexpected SQL: {normalized}")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class LeaseMemoryConnection:
    def __init__(self) -> None:
        self.state = PipelineLeaseState(
            UNISWAP_FORWARD_LEASE,
            None,
            None,
            None,
            0,
        )

    def cursor(self) -> LeaseMemoryCursor:
        return LeaseMemoryCursor(self)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


class LedgerMemoryCursor:
    def __init__(self, connection: LedgerMemoryConnection) -> None:
        self.connection = connection
        self.rowcount = 0
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> LedgerMemoryCursor:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def close(self) -> None:
        return None

    def execute(
        self,
        query: str,
        parameters: tuple[Any, ...] | None = None,
    ) -> None:
        normalized = " ".join(query.split()).upper()
        values = parameters or ()
        if normalized in {"BEGIN TRANSACTION", "COMMIT", "ROLLBACK"}:
            return
        if normalized.startswith("SELECT SUCCESSFUL_RUN_ID"):
            start_block, end_block = values[-2:]
            self.rows = [
                (item.successful_run_id, item.completed_at)
                for item in self.connection.rows
                if item.start_block == start_block
                and item.end_block == end_block
            ]
            return
        if normalized.startswith("INSERT INTO"):
            self.connection.rows.append(
                CompletedBlockRange(
                    start_block=int(values[4]),
                    end_block=int(values[5]),
                    successful_run_id=str(values[6]),
                    completed_at=values[7],
                )
            )
            self.rowcount = 1
            return
        raise AssertionError(f"Unexpected SQL: {normalized}")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class LedgerMemoryConnection:
    def __init__(self) -> None:
        self.rows: list[CompletedBlockRange] = []

    def cursor(self) -> LedgerMemoryCursor:
        return LedgerMemoryCursor(self)


def _store(
    connection: LeaseMemoryConnection,
    clock: MutableClock,
) -> SnowflakePipelineLeaseStore:
    return SnowflakePipelineLeaseStore(connection, clock=clock)


def test_lease_table_names_and_two_hour_ttl() -> None:
    config = PipelineLeaseTableConfig.from_env({})
    assert config.qualified_table.endswith("CONTROL.PIPELINE_RUN_LEASES")
    assert DEFAULT_LEASE_TTL == timedelta(hours=2)
    with pytest.raises(ValueError, match="valid Snowflake identifier"):
        replace(config, table="unsafe; DROP TABLE x")


def test_active_lease_blocks_another_owner_and_release_is_reusable() -> None:
    connection = LeaseMemoryConnection()
    clock = MutableClock(NOW)
    store = _store(connection, clock)

    first = store.acquire(UNISWAP_FORWARD_LEASE, "run-a")
    with pytest.raises(PipelineLeaseHeldError):
        store.acquire(UNISWAP_FORWARD_LEASE, "run-b")

    released = store.release(first)
    second = store.acquire(UNISWAP_FORWARD_LEASE, "run-b")

    assert released.owner_run_id is None
    assert first.lease_version == 1
    assert released.lease_version == 2
    assert second.lease_version == 3


def test_expired_takeover_rejects_stale_owner_mutations() -> None:
    connection = LeaseMemoryConnection()
    clock = MutableClock(NOW)
    store = _store(connection, clock)
    first = store.acquire(
        UNISWAP_FORWARD_LEASE,
        "run-a",
        ttl=timedelta(minutes=1),
    )

    clock.value = NOW + timedelta(minutes=2)
    second = store.acquire(UNISWAP_FORWARD_LEASE, "run-b")

    assert second.lease_version == first.lease_version + 1
    with pytest.raises(PipelineLeaseOwnershipError):
        store.release(first)
    with pytest.raises(PipelineLeaseOwnershipError):
        store.renew(first)
    released = store.release(second)
    assert released.owner_run_id is None
    assert released.lease_version == second.lease_version + 1


def test_prior_race_is_serialized_and_completed_evidence_stays_unique() -> None:
    lease_connection = LeaseMemoryConnection()
    ledger_connection = LedgerMemoryConnection()
    clock = MutableClock(NOW)
    leases = _store(lease_connection, clock)
    ledger = SnowflakeCompletedRangeStore(ledger_connection)
    completed = CompletedBlockRange(100, 109, "run-a", NOW)

    writer_a = leases.acquire(UNISWAP_FORWARD_LEASE, "run-a")
    with pytest.raises(PipelineLeaseHeldError):
        leases.acquire(UNISWAP_FORWARD_LEASE, "run-b")
    ledger.record_range(UNISWAP_FORWARD_WATERMARK, completed)
    leases.release(writer_a)

    writer_b = leases.acquire(UNISWAP_FORWARD_LEASE, "run-b")
    ledger.record_range(UNISWAP_FORWARD_WATERMARK, completed)
    leases.release(writer_b)

    assert len(ledger_connection.rows) == 1
    assert {
        (item.start_block, item.end_block)
        for item in ledger_connection.rows
    } == {(100, 109)}
