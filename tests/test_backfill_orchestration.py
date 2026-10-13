"""Unit contracts for bounded Airflow backfill orchestration."""

from __future__ import annotations

import pytest

from orchestration.backfill import (
    BackfillRequest,
    source_commands,
    validate_request,
    warehouse_command,
)


@pytest.mark.parametrize("backfill_type", ["aave", "chainlink"])
def test_supported_backfills_accept_finite_increasing_bounds(
    backfill_type: str,
) -> None:
    request = validate_request(backfill_type, 100, 109)
    assert request == BackfillRequest(backfill_type, 100, 109)
    assert request.block_count == 10


@pytest.mark.parametrize(
    ("backfill_type", "start", "end", "message"),
    [
        ("unsupported", 1, 2, "backfill_type"),
        ("aave", None, 2, "required"),
        ("aave", 1, None, "required"),
        ("chainlink", -1, 2, "negative"),
        ("chainlink", 2, 2, "less than"),
        ("chainlink", 3, 2, "less than"),
    ],
)
def test_invalid_backfill_requests_are_rejected_before_execution(
    backfill_type: str,
    start: int | None,
    end: int | None,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validate_request(backfill_type, start, end)


def test_aave_command_translates_inclusive_window_to_existing_cli() -> None:
    extraction, silver = source_commands(BackfillRequest("aave", 100, 109))
    assert extraction[1:3] == ("-m", "backfill.run_aave_v3")
    assert extraction[extraction.index("--to-block") + 1] == "109"
    assert extraction[extraction.index("--max-blocks") + 1] == "10"
    assert silver[1:3] == ("-m", "spark.build_aave_silver")


def test_chainlink_command_reuses_explicit_block_contract() -> None:
    extraction, silver = source_commands(
        BackfillRequest("chainlink", 100, 109)
    )
    assert extraction[1:3] == ("-m", "backfill.run_chainlink")
    assert extraction[extraction.index("--from-block") + 1] == "100"
    assert extraction[extraction.index("--to-block") + 1] == "109"
    assert silver[1:3] == ("-m", "spark.build_market_data_silver")


def test_warehouse_selection_is_limited_to_selected_source() -> None:
    aave = warehouse_command(
        BackfillRequest("aave", 100, 109),
        validate_only=False,
    )
    chainlink = warehouse_command(
        BackfillRequest("chainlink", 100, 109),
        validate_only=True,
    )
    assert "silver_aave_borrows" in aave
    assert "silver_chainlink_market_prices" not in aave
    assert "--validate-inputs" in chainlink
    assert "silver_chainlink_market_prices" in chainlink
