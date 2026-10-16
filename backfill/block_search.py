"""Deterministic timestamp-to-block boundary resolution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol


class BlockTimestampRpc(Protocol):
    """Minimal RPC surface required for timestamp boundary searches."""

    def get_latest_block_number(self) -> int: ...

    def get_block_timestamp(self, block_number: int) -> datetime: ...


@dataclass(frozen=True, slots=True)
class ResolvedBlockBoundary:
    """The first block at/after a timestamp and its predecessor."""

    boundary_timestamp: datetime
    previous_block: int | None
    previous_timestamp: datetime | None
    selected_block: int
    selected_timestamp: datetime


def _as_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def resolve_first_block_at_or_after(
    rpc: BlockTimestampRpc,
    boundary_timestamp: datetime,
    *,
    lowest_block: int = 0,
    highest_block: int | None = None,
) -> ResolvedBlockBoundary:
    """Binary-search the first block whose timestamp reaches the boundary."""
    boundary = _as_utc(
        boundary_timestamp,
        field_name="boundary_timestamp",
    )
    upper = (
        rpc.get_latest_block_number()
        if highest_block is None
        else highest_block
    )
    if isinstance(lowest_block, bool) or not isinstance(lowest_block, int):
        raise TypeError("lowest_block must be an integer")
    if isinstance(upper, bool) or not isinstance(upper, int):
        raise TypeError("highest_block must be an integer")
    if lowest_block < 0 or upper < lowest_block:
        raise ValueError("Block search bounds are invalid")

    upper_timestamp = _as_utc(
        rpc.get_block_timestamp(upper),
        field_name="upper block timestamp",
    )
    if upper_timestamp < boundary:
        raise ValueError("Boundary timestamp is later than the search range")

    low = lowest_block
    high = upper
    while low < high:
        middle = (low + high) // 2
        timestamp = _as_utc(
            rpc.get_block_timestamp(middle),
            field_name="block timestamp",
        )
        if timestamp >= boundary:
            high = middle
        else:
            low = middle + 1

    selected_timestamp = _as_utc(
        rpc.get_block_timestamp(low),
        field_name="selected block timestamp",
    )
    previous_block = low - 1 if low > lowest_block else None
    previous_timestamp = (
        _as_utc(
            rpc.get_block_timestamp(previous_block),
            field_name="previous block timestamp",
        )
        if previous_block is not None
        else None
    )
    if selected_timestamp < boundary or (
        previous_timestamp is not None
        and previous_timestamp >= boundary
    ):
        raise RuntimeError("RPC timestamps do not bracket the boundary")

    return ResolvedBlockBoundary(
        boundary_timestamp=boundary,
        previous_block=previous_block,
        previous_timestamp=previous_timestamp,
        selected_block=low,
        selected_timestamp=selected_timestamp,
    )
