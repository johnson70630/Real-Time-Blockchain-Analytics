"""Unit tests for durable forward watermark semantics."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from orchestration.watermark import (
    AAVE_FORWARD_WATERMARK,
    CHAINLINK_FORWARD_WATERMARK,
    MAX_WATERMARK_VALUE,
    MissingWatermarkError,
    ProcessingMode,
    RegressiveWatermarkError,
    UnsafeWatermarkCommitError,
    WatermarkAlreadyInitializedError,
    WatermarkCommitConflictError,
    WatermarkIdentity,
    WatermarkState,
    chainlink_feed_watermark,
    commit_watermark,
    get_watermark,
    initialize_watermark,
    validate_candidate,
)
from warehouse.load_raw import PIPELINE_ROLE
from warehouse.watermarks import (
    SnowflakeWatermarkStore,
    WatermarkTableConfig,
    connect_watermark_store,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


class MemoryStore:
    """Small deterministic test double implementing the store protocol."""

    def __init__(self) -> None:
        self.states: dict[WatermarkIdentity, WatermarkState] = {}
        self.mutations = 0

    def get(self, identity: WatermarkIdentity) -> WatermarkState | None:
        return self.states.get(identity)

    def initialize(
        self,
        identity: WatermarkIdentity,
        value: int,
        successful_run_id: str,
    ) -> WatermarkState:
        state = WatermarkState(identity, value, successful_run_id, NOW)
        self.states[identity] = state
        self.mutations += 1
        return state

    def advance(
        self,
        identity: WatermarkIdentity,
        *,
        expected_value: int,
        candidate_value: int,
        successful_run_id: str,
    ) -> WatermarkState:
        current = self.states[identity]
        if current.committed_value != expected_value:
            raise WatermarkCommitConflictError("concurrent update")
        state = WatermarkState(
            identity,
            candidate_value,
            successful_run_id,
            NOW,
        )
        self.states[identity] = state
        self.mutations += 1
        return state


def test_missing_watermark_is_not_implicitly_initialized() -> None:
    store = MemoryStore()
    assert get_watermark(store, AAVE_FORWARD_WATERMARK) is None
    with pytest.raises(MissingWatermarkError, match="initialized explicitly"):
        commit_watermark(
            store,
            AAVE_FORWARD_WATERMARK,
            200,
            "run-1",
            processing_succeeded=True,
        )
    assert store.mutations == 0


def test_explicit_initialization_can_be_read_and_not_repeated() -> None:
    store = MemoryStore()
    initialized = initialize_watermark(
        store,
        AAVE_FORWARD_WATERMARK,
        100,
        "bootstrap-1",
    )
    assert get_watermark(store, AAVE_FORWARD_WATERMARK) == initialized
    with pytest.raises(WatermarkAlreadyInitializedError):
        initialize_watermark(
            store,
            AAVE_FORWARD_WATERMARK,
            100,
            "bootstrap-2",
        )


def test_candidates_are_forward_equal_or_regressive() -> None:
    assert validate_candidate(100, 200) is True
    assert validate_candidate(100, 100) is False
    with pytest.raises(RegressiveWatermarkError):
        validate_candidate(100, 99)


def test_very_large_block_values_are_exact_and_bounded() -> None:
    assert validate_candidate(MAX_WATERMARK_VALUE - 1, MAX_WATERMARK_VALUE)
    with pytest.raises(ValueError, match="NUMBER"):
        validate_candidate(0, MAX_WATERMARK_VALUE + 1)


def test_successful_commit_advances_and_repeat_is_noop() -> None:
    store = MemoryStore()
    initialize_watermark(store, AAVE_FORWARD_WATERMARK, 100, "bootstrap")
    advanced = commit_watermark(
        store,
        AAVE_FORWARD_WATERMARK,
        200,
        "run-2",
        processing_succeeded=True,
    )
    repeated = commit_watermark(
        store,
        AAVE_FORWARD_WATERMARK,
        200,
        "run-2-repeat",
        processing_succeeded=True,
    )
    assert advanced.committed_value == 200
    assert repeated == advanced
    assert repeated.successful_run_id == "run-2"
    assert store.mutations == 2


def test_failed_processing_leaves_state_unchanged() -> None:
    store = MemoryStore()
    initial = initialize_watermark(
        store,
        AAVE_FORWARD_WATERMARK,
        100,
        "bootstrap",
    )
    with pytest.raises(UnsafeWatermarkCommitError, match="successful"):
        commit_watermark(
            store,
            AAVE_FORWARD_WATERMARK,
            200,
            "failed-run",
            processing_succeeded=False,
        )
    assert get_watermark(store, AAVE_FORWARD_WATERMARK) == initial
    assert store.mutations == 1


def test_independent_pipeline_identities_do_not_share_progress() -> None:
    store = MemoryStore()
    initialize_watermark(store, AAVE_FORWARD_WATERMARK, 100, "aave")
    initialize_watermark(store, CHAINLINK_FORWARD_WATERMARK, 300, "chainlink")
    commit_watermark(
        store,
        AAVE_FORWARD_WATERMARK,
        200,
        "aave-2",
        processing_succeeded=True,
    )
    assert store.get(AAVE_FORWARD_WATERMARK).committed_value == 200
    assert store.get(CHAINLINK_FORWARD_WATERMARK).committed_value == 300


def test_historical_backfill_cannot_advance_forward_watermark() -> None:
    store = MemoryStore()
    initial = initialize_watermark(
        store,
        AAVE_FORWARD_WATERMARK,
        100,
        "bootstrap",
    )
    with pytest.raises(UnsafeWatermarkCommitError, match="backfills"):
        commit_watermark(
            store,
            AAVE_FORWARD_WATERMARK,
            200,
            "bounded-backfill",
            processing_succeeded=True,
            processing_mode=ProcessingMode.BACKFILL,
        )
    assert store.get(AAVE_FORWARD_WATERMARK) == initial


def test_control_object_names_are_validated() -> None:
    config = WatermarkTableConfig.from_env({})
    assert config.qualified_table == (
        "BLOCKCHAIN_ANALYTICS.CONTROL.PIPELINE_WATERMARKS"
    )
    with pytest.raises(ValueError, match="valid Snowflake identifier"):
        replace(config, table="unsafe; DROP TABLE x")


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


def test_snowflake_store_binds_values_instead_of_interpolating_them() -> None:
    connection, cursor = _mock_connection()
    store = SnowflakeWatermarkStore(
        connection,
        WatermarkTableConfig(),
        clock=lambda: NOW,
    )
    identity = WatermarkIdentity("test_pipeline", "test_source", 42161)
    state = store.initialize(identity, 100, "test-run")
    insert_call = cursor.execute.call_args_list[1]
    assert "%s" in insert_call.args[0]
    assert "test-run" not in insert_call.args[0]
    assert "test-run" in insert_call.args[1]
    assert state.committed_value == 100


def test_snowflake_store_reads_initialized_state() -> None:
    connection, _cursor = _mock_connection(
        fetchall=[(100, "bootstrap", NOW)]
    )
    state = SnowflakeWatermarkStore(connection).get(
        AAVE_FORWARD_WATERMARK
    )
    assert state == WatermarkState(
        AAVE_FORWARD_WATERMARK,
        100,
        "bootstrap",
        NOW,
    )


def test_snowflake_store_rejects_optimistic_commit_conflict() -> None:
    connection, cursor = _mock_connection(rowcount=0)
    store = SnowflakeWatermarkStore(connection, clock=lambda: NOW)
    with pytest.raises(WatermarkCommitConflictError):
        store.advance(
            AAVE_FORWARD_WATERMARK,
            expected_value=100,
            candidate_value=200,
            successful_run_id="run-2",
        )
    executed = [call.args[0] for call in cursor.execute.call_args_list]
    assert executed[-1] == "ROLLBACK"


def test_snowflake_store_commits_related_watermarks_in_one_transaction() -> None:
    connection, cursor = _mock_connection(rowcount=1)
    cursor.fetchall.side_effect = [
        [(10, "bootstrap", NOW)],
        [(100, "bootstrap", NOW)],
    ]
    store = SnowflakeWatermarkStore(connection, clock=lambda: NOW)
    feed = chainlink_feed_watermark("0x" + "12" * 20)
    states = store.advance_many(
        {CHAINLINK_FORWARD_WATERMARK: 100, feed: 10},
        {CHAINLINK_FORWARD_WATERMARK: 200, feed: 11},
        "chainlink-run",
    )
    executed = [call.args[0] for call in cursor.execute.call_args_list]
    assert executed[0] == "BEGIN TRANSACTION"
    assert executed[-1] == "COMMIT"
    assert executed.count("BEGIN TRANSACTION") == 1
    assert len(states) == 2


def test_snowflake_multi_watermark_initialization_conflict_rolls_back() -> None:
    class ConflictCursor:
        def __init__(self) -> None:
            self.rowcount = 1
            self.commands: list[str] = []
            self.inserts = 0

        def execute(self, sql, _parameters=None):
            self.commands.append(sql)
            if sql.lstrip().startswith("INSERT"):
                self.inserts += 1
                self.rowcount = int(self.inserts == 1)

        def close(self):
            return None

    cursor = ConflictCursor()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    store = SnowflakeWatermarkStore(connection, clock=lambda: NOW)
    feed = chainlink_feed_watermark("0x" + "12" * 20)

    with pytest.raises(WatermarkCommitConflictError, match="initialization"):
        store.initialize_many(
            {CHAINLINK_FORWARD_WATERMARK: 200, feed: 11},
            "chainlink-bootstrap",
        )

    assert cursor.commands[-1] == "ROLLBACK"
    assert "COMMIT" not in cursor.commands
    assert cursor.inserts == 2


def test_snowflake_multi_watermark_conflict_rolls_back() -> None:
    class ConflictCursor:
        def __init__(self) -> None:
            self.rowcount = 1
            self.commands: list[str] = []
            self.updates = 0
            self.selects = 0

        def execute(self, sql, _parameters=None):
            self.commands.append(sql)
            if sql.lstrip().startswith("UPDATE"):
                self.updates += 1
                self.rowcount = int(self.updates == 1)

        def fetchall(self):
            self.selects += 1
            return [
                (10 if self.selects == 1 else 999, "bootstrap", NOW)
            ]

        def close(self):
            return None

    cursor = ConflictCursor()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    store = SnowflakeWatermarkStore(connection, clock=lambda: NOW)
    feed = chainlink_feed_watermark("0x" + "12" * 20)
    with pytest.raises(WatermarkCommitConflictError, match="Related"):
        store.advance_many(
            {CHAINLINK_FORWARD_WATERMARK: 100, feed: 10},
            {CHAINLINK_FORWARD_WATERMARK: 200, feed: 11},
            "chainlink-run",
        )
    assert cursor.commands[-1] == "ROLLBACK"
    assert "COMMIT" not in cursor.commands
    assert cursor.updates == 0


def test_runtime_connection_uses_pipeline_role_and_disables_secondary_roles() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = (PIPELINE_ROLE,)
    connection = MagicMock()
    connection.cursor.return_value = cursor
    with (
        patch(
            "warehouse.watermarks.validate_key_pair_connection_config"
        ) as validate,
        patch(
            "warehouse.watermarks.snowflake.connector.connect",
            return_value=connection,
        ) as connect,
    ):
        assert connect_watermark_store("blockchain-dev", "WAREHOUSE") is connection
    validate.assert_called_once_with("blockchain-dev")
    assert connect.call_args.kwargs["role"] == "BLOCKCHAIN_PIPELINE_ROLE"
    assert "password" not in connect.call_args.kwargs
    assert cursor.execute.call_args_list[0].args[0] == (
        "USE SECONDARY ROLES NONE"
    )


def test_watermark_logging_does_not_include_connection_secrets(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "never-log-this-private-key-or-password"
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY", secret)
    store = MemoryStore()
    with caplog.at_level(logging.INFO):
        initialize_watermark(store, AAVE_FORWARD_WATERMARK, 100, "run-safe")
    assert secret not in caplog.text


def test_provisioning_sql_is_owner_run_and_least_privilege() -> None:
    sql = Path("warehouse/provision_watermarks.sql").read_text(
        encoding="utf-8"
    ).upper()
    assert "USE ROLE BLOCKCHAIN_ADMIN_ROLE" in sql
    assert "TO ROLE BLOCKCHAIN_PIPELINE_ROLE" in sql
    assert "GRANT SELECT, INSERT, UPDATE" in sql
    assert "GRANT DELETE" not in sql
    assert "GRANT CREATE" not in sql
