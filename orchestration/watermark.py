"""Domain contract for durable forward-processing watermarks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

MAX_WATERMARK_VALUE = 10**38 - 1
ARBITRUM_CHAIN_ID = 42161


class WatermarkError(RuntimeError):
    """Base error for watermark contract violations."""


class MissingWatermarkError(WatermarkError):
    """Raised when an operation requires explicitly initialized state."""


class WatermarkAlreadyInitializedError(WatermarkError):
    """Raised when explicit initialization is attempted twice."""


class RegressiveWatermarkError(WatermarkError):
    """Raised when a candidate would move durable progress backward."""


class WatermarkCommitConflictError(WatermarkError):
    """Raised when committed state changes during an optimistic update."""


class UnsafeWatermarkCommitError(WatermarkError):
    """Raised when a caller has not satisfied the forward commit boundary."""


class ProcessingMode(StrEnum):
    """Separate normal forward progress from explicit historical replay."""

    FORWARD = "forward"
    BACKFILL = "backfill"


@dataclass(frozen=True, slots=True)
class WatermarkIdentity:
    """Natural key for one independently progressing source pipeline."""

    pipeline_name: str
    source_name: str
    chain_id: int
    watermark_type: str = "block_number"

    def __post_init__(self) -> None:
        for field_name in ("pipeline_name", "source_name", "watermark_type"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        _validate_watermark_value(self.chain_id, field_name="chain_id")


@dataclass(frozen=True, slots=True)
class WatermarkState:
    """Current durable progress for one watermark identity."""

    identity: WatermarkIdentity
    committed_value: int
    successful_run_id: str
    updated_at: datetime


class WatermarkStore(Protocol):
    """Persistence boundary required by the watermark service functions."""

    def get(self, identity: WatermarkIdentity) -> WatermarkState | None: ...

    def initialize(
        self,
        identity: WatermarkIdentity,
        value: int,
        successful_run_id: str,
    ) -> WatermarkState: ...

    def advance(
        self,
        identity: WatermarkIdentity,
        *,
        expected_value: int,
        candidate_value: int,
        successful_run_id: str,
    ) -> WatermarkState: ...


def _validate_watermark_value(value: int, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must not be negative")
    if value > MAX_WATERMARK_VALUE:
        raise ValueError(
            f"{field_name} exceeds Snowflake NUMBER(38, 0) capacity"
        )
    return value


AAVE_FORWARD_WATERMARK = WatermarkIdentity(
    pipeline_name="aave_forward",
    source_name="aave_v3_pool",
    chain_id=ARBITRUM_CHAIN_ID,
)
# Immutable identity used by the accepted one-time bootstrap range evidence.
# New production progress and completed ranges use AAVE_FORWARD_WATERMARK.
AAVE_LEGACY_BOOTSTRAP_RANGES = WatermarkIdentity(
    pipeline_name="aave_forward",
    source_name="alchemy_rpc",
    chain_id=ARBITRUM_CHAIN_ID,
)
CHAINLINK_FORWARD_WATERMARK = WatermarkIdentity(
    pipeline_name="chainlink_forward",
    source_name="chainlink_proxy_rpc",
    chain_id=ARBITRUM_CHAIN_ID,
)


def _validate_run_id(successful_run_id: str) -> str:
    value = successful_run_id.strip()
    if not value:
        raise ValueError("successful_run_id must not be empty")
    return value


def get_watermark(
    store: WatermarkStore,
    identity: WatermarkIdentity,
) -> WatermarkState | None:
    """Read current durable progress without inventing missing state."""
    return store.get(identity)


def initialize_watermark(
    store: WatermarkStore,
    identity: WatermarkIdentity,
    value: int,
    successful_run_id: str,
) -> WatermarkState:
    """Create explicitly chosen initial state; never infer a source position."""
    _validate_watermark_value(value, field_name="initial_value")
    run_id = _validate_run_id(successful_run_id)
    if store.get(identity) is not None:
        raise WatermarkAlreadyInitializedError(
            f"Watermark {identity.pipeline_name!r} is already initialized"
        )
    return store.initialize(identity, value, run_id)


def validate_candidate(current_value: int, candidate_value: int) -> bool:
    """Validate monotonic progress and return whether an advance is needed."""
    _validate_watermark_value(current_value, field_name="current_value")
    _validate_watermark_value(candidate_value, field_name="candidate_value")
    if candidate_value < current_value:
        raise RegressiveWatermarkError(
            f"Candidate {candidate_value} is below committed value "
            f"{current_value}"
        )
    return candidate_value > current_value


def commit_watermark(
    store: WatermarkStore,
    identity: WatermarkIdentity,
    candidate_value: int,
    successful_run_id: str,
    *,
    processing_succeeded: bool,
    processing_mode: ProcessingMode = ProcessingMode.FORWARD,
) -> WatermarkState:
    """Advance only after the complete forward-processing boundary succeeds."""
    if processing_mode is not ProcessingMode.FORWARD:
        raise UnsafeWatermarkCommitError(
            "Historical backfills must not advance forward watermarks"
        )
    if not processing_succeeded:
        raise UnsafeWatermarkCommitError(
            "Watermark commit requires a successful processing boundary"
        )

    current = store.get(identity)
    if current is None:
        raise MissingWatermarkError(
            f"Watermark {identity.pipeline_name!r} must be initialized explicitly"
        )
    run_id = _validate_run_id(successful_run_id)
    should_advance = validate_candidate(
        current.committed_value,
        candidate_value,
    )
    if not should_advance:
        return current
    return store.advance(
        identity,
        expected_value=current.committed_value,
        candidate_value=candidate_value,
        successful_run_id=run_id,
    )
