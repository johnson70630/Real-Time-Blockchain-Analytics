"""Tests for the append-only Snowflake completed-range ledger."""

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from orchestration.aave_forward import CompletedBlockRange
from orchestration.watermark import AAVE_LEGACY_BOOTSTRAP_RANGES
from warehouse.completed_ranges import (
    ConflictingCompletedRangeEvidence,
    CompletedRangeTableConfig,
    DuplicateCompletedRangeEvidence,
    SnowflakeCompletedRangeStore,
    summarize_completed_range_coverage,
)

NOW = datetime(2026, 10, 13, tzinfo=UTC)


def _mock_connection(
    *,
    fetchall: list[tuple[Any, ...]] | None = None,
    rowcount: int = 1,
) -> tuple[MagicMock, MagicMock]:
    cursor = MagicMock()
    cursor.fetchall.return_value = fetchall or []
    cursor.rowcount = rowcount
    connection = MagicMock()
    connection.cursor.return_value = cursor
    return connection, cursor


def test_completed_range_table_names_are_validated() -> None:
    config = CompletedRangeTableConfig.from_env({})
    assert config.qualified_table == (
        "BLOCKCHAIN_ANALYTICS.CONTROL.PIPELINE_COMPLETED_RANGES"
    )
    with pytest.raises(ValueError, match="valid Snowflake identifier"):
        replace(config, table="unsafe; DROP TABLE x")


def test_store_lists_ordered_ranges_for_exact_identity() -> None:
    connection, cursor = _mock_connection(
        fetchall=[(100, 109, "run-1", NOW), (110, 119, "run-2", NOW)]
    )

    result = SnowflakeCompletedRangeStore(connection).list_ranges(
        AAVE_LEGACY_BOOTSTRAP_RANGES
    )

    assert result == (
        CompletedBlockRange(100, 109, "run-1", NOW),
        CompletedBlockRange(110, 119, "run-2", NOW),
    )
    query, parameters = cursor.execute.call_args.args
    assert "ORDER BY start_block" in query
    assert parameters == ("aave_forward", "alchemy_rpc", 42161, "block_number")


def test_new_range_is_inserted_and_values_are_bound() -> None:
    connection, cursor = _mock_connection(rowcount=1)
    completed = CompletedBlockRange(100, 109, "run-safe", NOW)

    result = SnowflakeCompletedRangeStore(connection).record_range(
        AAVE_LEGACY_BOOTSTRAP_RANGES,
        completed,
    )

    insert_call = cursor.execute.call_args_list[2]
    query, parameters = insert_call.args
    assert result == completed
    assert "VALUES" in query
    assert "run-safe" not in query
    assert "run-safe" in parameters
    assert parameters.count(100) == 1
    assert parameters.count(109) == 1
    assert cursor.execute.call_args_list[0].args == ("BEGIN TRANSACTION",)
    assert cursor.execute.call_args_list[-1].args == ("COMMIT",)


def test_exact_range_retry_is_a_noop() -> None:
    connection, cursor = _mock_connection(
        fetchall=[("run-safe", NOW)],
    )
    completed = CompletedBlockRange(100, 109, "run-safe", NOW)

    result = SnowflakeCompletedRangeStore(connection).record_range(
        AAVE_LEGACY_BOOTSTRAP_RANGES,
        completed,
    )

    assert result == completed
    assert all(
        "INSERT INTO" not in call.args[0]
        for call in cursor.execute.call_args_list
    )
    assert cursor.execute.call_args_list[-1].args == ("COMMIT",)


def test_store_rejects_unexpected_insert_cardinality() -> None:
    connection, cursor = _mock_connection(rowcount=2)
    with pytest.raises(RuntimeError, match="insert count"):
        SnowflakeCompletedRangeStore(connection).record_range(
            AAVE_LEGACY_BOOTSTRAP_RANGES,
            CompletedBlockRange(100, 109, "run", NOW),
        )
    assert cursor.execute.call_args_list[-1].args == ("ROLLBACK",)


def test_multiple_completed_ranges_are_committed_atomically() -> None:
    connection, cursor = _mock_connection(rowcount=1)
    cursor.fetchall.side_effect = [[], []]
    ranges = (
        CompletedBlockRange(100, 109, "run", NOW),
        CompletedBlockRange(110, 119, "run", NOW),
    )

    result = SnowflakeCompletedRangeStore(connection).record_ranges(
        AAVE_LEGACY_BOOTSTRAP_RANGES,
        ranges,
    )

    assert result == ranges
    calls = cursor.execute.call_args_list
    assert calls[0].args == ("BEGIN TRANSACTION",)
    assert "SELECT successful_run_id" in calls[1].args[0]
    assert "INSERT INTO" in calls[2].args[0]
    assert "SELECT successful_run_id" in calls[3].args[0]
    assert "INSERT INTO" in calls[4].args[0]
    assert calls[5].args == ("COMMIT",)


def test_duplicate_and_conflicting_evidence_are_rejected() -> None:
    completed = CompletedBlockRange(100, 109, "run-safe", NOW)
    duplicate_connection, duplicate_cursor = _mock_connection(
        fetchall=[("run-safe", NOW), ("run-safe", NOW)],
    )
    with pytest.raises(DuplicateCompletedRangeEvidence):
        SnowflakeCompletedRangeStore(duplicate_connection).record_range(
            AAVE_LEGACY_BOOTSTRAP_RANGES,
            completed,
        )
    assert duplicate_cursor.execute.call_args_list[-1].args == ("ROLLBACK",)

    conflict_connection, conflict_cursor = _mock_connection(
        fetchall=[("other-run", NOW)],
    )
    with pytest.raises(ConflictingCompletedRangeEvidence):
        SnowflakeCompletedRangeStore(conflict_connection).record_range(
            AAVE_LEGACY_BOOTSTRAP_RANGES,
            completed,
        )
    assert conflict_cursor.execute.call_args_list[-1].args == ("ROLLBACK",)


def test_coverage_uses_logical_distinct_ranges_and_reports_duplicates() -> None:
    ranges = (
        CompletedBlockRange(100, 109, "run-a", NOW),
        CompletedBlockRange(100, 109, "run-a", NOW),
        CompletedBlockRange(110, 119, "run-b", NOW),
        CompletedBlockRange(125, 129, "run-c", NOW),
    )

    coverage = summarize_completed_range_coverage(100, ranges)

    assert coverage.physical_rows == 4
    assert coverage.logical_ranges == 3
    assert coverage.duplicate_groups == 1
    assert coverage.highest_contiguous_block == 119
    assert coverage.gaps == ((120, 124),)


def test_provisioning_sql_is_owner_run_and_runtime_is_append_only() -> None:
    sql = Path("warehouse/provision_completed_ranges.sql").read_text(
        encoding="utf-8"
    ).upper()
    assert "USE ROLE BLOCKCHAIN_ADMIN_ROLE" in sql
    assert "TO ROLE BLOCKCHAIN_PIPELINE_ROLE" in sql
    assert "GRANT SELECT, INSERT" in sql
    assert "GRANT UPDATE" not in sql
    assert "GRANT DELETE" not in sql
    assert "GRANT CREATE" not in sql
