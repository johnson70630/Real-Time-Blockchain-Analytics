"""Tests for deterministic timestamp-to-block boundary resolution."""

from datetime import UTC, datetime, timedelta

import pytest

from backfill.block_search import resolve_first_block_at_or_after

START = datetime(2026, 10, 1, tzinfo=UTC)


class TimestampRpc:
    """Deterministic block timestamp test double."""

    def __init__(self, latest_block: int = 20) -> None:
        self.latest_block = latest_block
        self.lookups: list[int] = []

    def get_latest_block_number(self) -> int:
        return self.latest_block

    def get_block_timestamp(self, block_number: int) -> datetime:
        self.lookups.append(block_number)
        return START + timedelta(seconds=block_number * 2)


def test_resolves_first_block_at_or_after_boundary_and_predecessor() -> None:
    rpc = TimestampRpc()

    result = resolve_first_block_at_or_after(
        rpc,
        START + timedelta(seconds=11),
        lowest_block=0,
    )

    assert result.selected_block == 6
    assert result.selected_timestamp == START + timedelta(seconds=12)
    assert result.previous_block == 5
    assert result.previous_timestamp == START + timedelta(seconds=10)


def test_exact_boundary_selects_matching_block() -> None:
    result = resolve_first_block_at_or_after(
        TimestampRpc(),
        START + timedelta(seconds=20),
        lowest_block=0,
        highest_block=20,
    )

    assert result.selected_block == 10
    assert result.selected_timestamp == START + timedelta(seconds=20)


def test_search_rejects_naive_or_out_of_range_boundary() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_first_block_at_or_after(TimestampRpc(), datetime(2026, 10, 1))

    with pytest.raises(ValueError, match="later than the search range"):
        resolve_first_block_at_or_after(
            TimestampRpc(latest_block=5),
            START + timedelta(seconds=11),
        )


def test_search_rejects_invalid_block_bounds() -> None:
    with pytest.raises(ValueError, match="bounds"):
        resolve_first_block_at_or_after(
            TimestampRpc(),
            START,
            lowest_block=10,
            highest_block=9,
        )
