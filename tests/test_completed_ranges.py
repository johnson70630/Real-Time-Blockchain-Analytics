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
    CompletedRangeTableConfig,
    SnowflakeCompletedRangeStore,
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


@pytest.mark.parametrize("rowcount", [0, 1])
def test_exact_range_retry_is_safe_and_values_are_bound(rowcount: int) -> None:
    connection, cursor = _mock_connection(rowcount=rowcount)
    completed = CompletedBlockRange(100, 109, "run-safe", NOW)

    result = SnowflakeCompletedRangeStore(connection).record_range(
        AAVE_LEGACY_BOOTSTRAP_RANGES,
        completed,
    )

    insert_call = cursor.execute.call_args_list[1]
    query, parameters = insert_call.args
    assert result == completed
    assert "WHERE NOT EXISTS" in query
    assert "run-safe" not in query
    assert "run-safe" in parameters
    assert parameters.count(100) == 2
    assert parameters.count(109) == 2
    assert cursor.execute.call_args_list[0].args == ("BEGIN TRANSACTION",)
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
    assert "INSERT INTO" in calls[1].args[0]
    assert "INSERT INTO" in calls[2].args[0]
    assert calls[3].args == ("COMMIT",)


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
