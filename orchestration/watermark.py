"""Domain contract for durable forward-processing watermarks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Mapping, Protocol

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


class AtomicWatermarkStore(WatermarkStore, Protocol):
    """Store capable of committing related identities in one transaction."""

    def initialize_many(
        self,
        values: Mapping[WatermarkIdentity, int],
        successful_run_id: str,
    ) -> tuple[WatermarkState, ...]: ...

    def advance_many(
        self,
        expected_values: Mapping[WatermarkIdentity, int],
        candidate_values: Mapping[WatermarkIdentity, int],
        successful_run_id: str,
    ) -> tuple[WatermarkState, ...]: ...

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
    source_name="chainlink_feeds",
    chain_id=ARBITRUM_CHAIN_ID,
)
UNISWAP_FORWARD_WATERMARK = WatermarkIdentity(
    pipeline_name="uniswap_forward",
    source_name="uniswap_v3_pools",
    chain_id=ARBITRUM_CHAIN_ID,
)


def chainlink_feed_watermark(feed_address: str) -> WatermarkIdentity:
    """Return the provider-neutral round cursor for one feed proxy."""
    normalized = feed_address.strip().lower()
    if not normalized:
        raise ValueError("feed_address must not be empty")
    return WatermarkIdentity(
        pipeline_name="chainlink_forward",
        source_name=normalized,
        chain_id=ARBITRUM_CHAIN_ID,
        watermark_type="round_id",
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


def commit_watermark_from_expected(
    store: WatermarkStore,
    identity: WatermarkIdentity,
    *,
    expected_value: int,
    candidate_value: int,
    successful_run_id: str,
    processing_succeeded: bool,
) -> WatermarkState:
    """Commit against the position used to plan work, rejecting stale writers."""
    if not processing_succeeded:
        raise UnsafeWatermarkCommitError(
            "Watermark commit requires a successful processing boundary"
        )
    current = store.get(identity)
    if current is None:
        raise MissingWatermarkError(
            f"Watermark {identity.pipeline_name!r} must be initialized explicitly"
        )
    if current.committed_value != expected_value:
        raise WatermarkCommitConflictError(
            f"Expected committed value {expected_value}, found "
            f"{current.committed_value}"
        )
    run_id = _validate_run_id(successful_run_id)
    if not validate_candidate(expected_value, candidate_value):
        return current
    return store.advance(
        identity,
        expected_value=expected_value,
        candidate_value=candidate_value,
        successful_run_id=run_id,
    )


def initialize_watermarks_atomically(
    store: AtomicWatermarkStore,
    values: Mapping[WatermarkIdentity, int],
    successful_run_id: str,
) -> tuple[WatermarkState, ...]:
    """Initialize a related state set without permitting partial creation."""
    if not values:
        raise ValueError("At least one watermark is required")
    for value in values.values():
        _validate_watermark_value(value, field_name="initial_value")
    run_id = _validate_run_id(successful_run_id)
    initialized = [identity for identity in values if store.get(identity)]
    if initialized:
        raise WatermarkAlreadyInitializedError(
            "Related watermarks must all be uninitialized"
        )
    return store.initialize_many(values, run_id)


def commit_watermarks_atomically(
    store: AtomicWatermarkStore,
    current_states: Mapping[WatermarkIdentity, WatermarkState],
    candidate_values: Mapping[WatermarkIdentity, int],
    successful_run_id: str,
    *,
    processing_succeeded: bool,
    processing_mode: ProcessingMode = ProcessingMode.FORWARD,
) -> tuple[WatermarkState, ...]:
    """Advance a complete related state set only after all work succeeds."""
    if processing_mode is not ProcessingMode.FORWARD:
        raise UnsafeWatermarkCommitError(
            "Historical backfills must not advance forward watermarks"
        )
    if not processing_succeeded:
        raise UnsafeWatermarkCommitError(
            "Watermark commit requires a successful processing boundary"
        )
    if not current_states or set(current_states) != set(candidate_values):
        raise ValueError("Current and candidate watermark identities must match")
    run_id = _validate_run_id(successful_run_id)
    expected_values: dict[WatermarkIdentity, int] = {}
    for identity, current in current_states.items():
        if current.identity != identity:
            raise ValueError("Watermark state identity mismatch")
        candidate = candidate_values[identity]
        validate_candidate(current.committed_value, candidate)
        expected_values[identity] = current.committed_value
    if all(
        candidate_values[identity] == expected
        for identity, expected in expected_values.items()
    ):
        return tuple(
            current_states[identity]
            for identity in sorted(current_states, key=_identity_sort_key)
        )
    return store.advance_many(expected_values, candidate_values, run_id)


def _identity_sort_key(identity: WatermarkIdentity) -> tuple[str, str, int, str]:
    return (
        identity.pipeline_name,
        identity.source_name,
        identity.chain_id,
        identity.watermark_type,
    )
