"""Unit tests for the reusable Snowflake RAW execution adapter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from warehouse.load_raw import (
    KEY_PAIR_AUTHENTICATOR,
    PIPELINE_ROLE,
    _copy_metrics,
    execute_dataset_load,
    validate_key_pair_connection_config,
    validate_stage_inputs,
    validate_target_tables,
)
from warehouse.snowflake import (
    UNISWAP_SWAPS,
    SnowflakeLandingConfig,
)


def test_key_pair_configuration_requires_jwt_and_private_key() -> None:
    valid = {
        "authenticator": KEY_PAIR_AUTHENTICATOR,
        "private_key_file": "/not/read/by/unit/test/key.p8",
    }
    with patch("warehouse.load_raw._connection_definition", return_value=valid):
        validate_key_pair_connection_config("test")

    with patch(
        "warehouse.load_raw._connection_definition",
        return_value={"authenticator": "externalbrowser"},
    ):
        with pytest.raises(ValueError, match="key-pair"):
            validate_key_pair_connection_config("test")


def test_key_pair_configuration_rejects_password_fallback() -> None:
    definition = {
        "authenticator": KEY_PAIR_AUTHENTICATOR,
        "private_key_file": "/not/read/by/unit/test/key.p8",
        "password": "must-not-be-used",
    }
    with patch(
        "warehouse.load_raw._connection_definition",
        return_value=definition,
    ):
        with pytest.raises(ValueError, match="Password authentication"):
            validate_key_pair_connection_config("test")


def test_pipeline_role_is_not_configurable() -> None:
    assert PIPELINE_ROLE == "BLOCKCHAIN_PIPELINE_ROLE"


def test_copy_metrics_ignore_skipped_files() -> None:
    files, rows = _copy_metrics(
        [
            {"file": "one.parquet", "status": "LOADED", "rows_loaded": 7},
            {"file": "two.parquet", "status": "SKIPPED", "rows_loaded": 0},
        ]
    )
    assert (files, rows) == (1, 7)
    assert _copy_metrics([{"status": "Copy executed with 0 files processed."}]) == (
        0,
        0,
    )


def test_execute_dataset_load_reports_non_secret_metrics() -> None:
    copy_cursor = MagicMock()
    copy_cursor.description = [
        ("file",),
        ("status",),
        ("rows_loaded",),
    ]
    copy_cursor.fetchall.return_value = [("one.parquet", "LOADED", 3)]
    begin_cursor = MagicMock(description=None)
    merge_cursor = MagicMock()
    merge_cursor.description = [
        ("number of rows inserted",),
        ("number of rows updated",),
    ]
    merge_cursor.fetchall.return_value = [(2, 1)]
    update_cursor = MagicMock(description=None)
    commit_cursor = MagicMock(description=None)
    final_cursor = MagicMock()
    final_cursor.description = [("final_rows",), ("duplicate_keys",)]
    final_cursor.fetchall.return_value = [(391, 0)]

    connection = MagicMock()
    connection.execute_string.return_value = [
        copy_cursor,
        begin_cursor,
        merge_cursor,
        update_cursor,
        commit_cursor,
    ]
    connection.cursor.return_value = final_cursor

    summary = execute_dataset_load(
        connection,
        SnowflakeLandingConfig(),
        UNISWAP_SWAPS,
    )

    assert summary.copy_files_processed == 1
    assert summary.copy_rows_loaded == 3
    assert summary.merge_inserted == 2
    assert summary.merge_updated == 1
    assert summary.final_rows == 391
    assert summary.duplicate_keys == 0


def test_stage_validation_fails_for_missing_parquet_input() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.cursor.return_value = cursor

    with pytest.raises(RuntimeError, match="No Parquet inputs"):
        validate_stage_inputs(
            connection,
            SnowflakeLandingConfig(),
            (UNISWAP_SWAPS,),
        )


def test_target_validation_reports_missing_provisioned_tables() -> None:
    connection = MagicMock()
    connection.cursor.return_value.execute.side_effect = [None, RuntimeError]

    with patch("warehouse.load_raw.snowflake.connector.Error", RuntimeError):
        with pytest.raises(RuntimeError, match="UNISWAP_SWAPS_LANDING"):
            validate_target_tables(
                connection,
                SnowflakeLandingConfig(),
                (UNISWAP_SWAPS,),
            )
