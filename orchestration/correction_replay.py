"""Pure contracts for bounded historical and correction replay.

Replay is deliberately separate from normal forward processing.  It compares a
complete fresh source snapshot with the records already retained for the same
inclusive block interval, and never owns a production watermark or completed-
range mutation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar


class ProcessingMode(StrEnum):
    """Explicit source-processing modes with distinct state semantics."""

    FORWARD = "forward"
    HISTORICAL_REPLAY = "historical"
    CORRECTION_REPLAY = "correction"


SUPPORTED_REPLAY_SOURCES = frozenset({"aave", "chainlink", "uniswap"})


@dataclass(frozen=True, slots=True)
class ReplayRequest:
    """One operator-selected inclusive source interval."""

    source: str
    start_block: int
    end_block: int
    mode: ProcessingMode

    def __post_init__(self) -> None:
        normalized_source = self.source.strip().lower()
        if normalized_source not in SUPPORTED_REPLAY_SOURCES:
            expected = ", ".join(sorted(SUPPORTED_REPLAY_SOURCES))
            raise ValueError(f"source must be one of: {expected}")
        if self.mode is ProcessingMode.FORWARD:
            raise ValueError(
                "forward mode belongs to the existing source-specific entrypoints"
            )
        if self.start_block < 0 or self.end_block < self.start_block:
            raise ValueError("replay block bounds must be inclusive and increasing")
        object.__setattr__(self, "source", normalized_source)

    @property
    def block_count(self) -> int:
        """Return the number of blocks in the inclusive replay interval."""
        return self.end_block - self.start_block + 1


@dataclass(frozen=True, slots=True, order=True)
class ReplayRecord:
    """Comparable source record stripped of mutable operational metadata."""

    identity: str
    block_number: int
    content_fingerprint: str

    def __post_init__(self) -> None:
        if not self.identity.strip() or not self.content_fingerprint.strip():
            raise ValueError("replay identity and fingerprint must not be empty")
        if self.block_number < 0:
            raise ValueError("replay block_number must not be negative")


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    """A bounded source response plus proof that the interval was complete."""

    records: tuple[ReplayRecord, ...]
    complete: bool
    request_count: int = 0


@dataclass(frozen=True, slots=True)
class ReplayReconciliation:
    """Deterministic comparison of stored and freshly observed identities."""

    unchanged: tuple[ReplayRecord, ...]
    added: tuple[ReplayRecord, ...]
    missing_from_source: tuple[ReplayRecord, ...]
    changed: tuple[tuple[ReplayRecord, ReplayRecord], ...]

    @property
    def requires_canonicality_correction(self) -> bool:
        """Return whether applying the result requires tombstone semantics."""
        return bool(self.missing_from_source or self.changed)

    @property
    def has_changes(self) -> bool:
        """Return whether the retained state differs from the fresh source."""
        return bool(self.added or self.missing_from_source or self.changed)


class IncompleteReplaySourceError(RuntimeError):
    """Raised before reconciliation when any source sub-query was incomplete."""


class ReplayCorrectionRequiredError(RuntimeError):
    """Raised when audit finds removals/changes that must not be hard-deleted."""


class ReplayWatermarkChangedError(RuntimeError):
    """Raised if a replay path changes production forward state."""


class DuplicateReplayIdentityError(RuntimeError):
    """Raised when one side contains conflicting copies of an identity."""


def _index_records(
    records: tuple[ReplayRecord, ...],
    *,
    request: ReplayRequest,
    side: str,
) -> dict[str, ReplayRecord]:
    indexed: dict[str, ReplayRecord] = {}
    for record in records:
        if not request.start_block <= record.block_number <= request.end_block:
            raise ValueError(
                f"{side} record {record.identity!r} is outside the replay interval"
            )
        current = indexed.get(record.identity)
        if current is not None and current != record:
            raise DuplicateReplayIdentityError(
                f"{side} contains conflicting identity {record.identity!r}"
            )
        indexed[record.identity] = record
    return indexed


def reconcile_records(
    request: ReplayRequest,
    *,
    stored: tuple[ReplayRecord, ...],
    fresh: SourceSnapshot,
) -> ReplayReconciliation:
    """Compare one complete fresh interval with its retained source records."""
    if not fresh.complete:
        raise IncompleteReplaySourceError(
            "Replay source interval is incomplete; no correction may be applied"
        )
    stored_by_id = _index_records(stored, request=request, side="stored")
    fresh_by_id = _index_records(fresh.records, request=request, side="fresh")
    stored_ids = set(stored_by_id)
    fresh_ids = set(fresh_by_id)
    shared_ids = stored_ids & fresh_ids
    changed_ids = {
        identity
        for identity in shared_ids
        if stored_by_id[identity].content_fingerprint
        != fresh_by_id[identity].content_fingerprint
    }
    unchanged_ids = shared_ids - changed_ids
    return ReplayReconciliation(
        unchanged=tuple(stored_by_id[key] for key in sorted(unchanged_ids)),
        added=tuple(fresh_by_id[key] for key in sorted(fresh_ids - stored_ids)),
        missing_from_source=tuple(
            stored_by_id[key] for key in sorted(stored_ids - fresh_ids)
        ),
        changed=tuple(
            (stored_by_id[key], fresh_by_id[key]) for key in sorted(changed_ids)
        ),
    )


State = TypeVar("State", bound=Mapping[str, int])


@dataclass(frozen=True, slots=True)
class ReplayExecution:
    """Result of an audit or an explicitly applied bounded replay."""

    request: ReplayRequest
    reconciliation: ReplayReconciliation
    applied: bool
    watermark_state: Mapping[str, int]
    source_snapshot: SourceSnapshot


def execute_replay(
    request: ReplayRequest,
    *,
    read_watermarks: Callable[[], State],
    load_stored: Callable[
        [ReplayRequest, SourceSnapshot], tuple[ReplayRecord, ...]
    ],
    fetch_fresh: Callable[[ReplayRequest], SourceSnapshot],
    apply_reconciliation: Callable[[ReplayReconciliation], None] | None = None,
    refresh_downstream: Callable[[], None] | None = None,
) -> ReplayExecution:
    """Audit or apply one replay while enforcing watermark immutability.

    An apply callback must implement auditable soft-delete/tombstone behavior for
    missing or changed records.  This coordinator intentionally offers no hard-
    delete primitive and has no completed-range store dependency.
    """
    before = dict(read_watermarks())
    try:
        fresh = fetch_fresh(request)
        result = reconcile_records(
            request,
            stored=load_stored(request, fresh),
            fresh=fresh,
        )
        if request.mode is ProcessingMode.HISTORICAL_REPLAY and (
            result.missing_from_source or result.changed
        ):
            raise ReplayCorrectionRequiredError(
                "Historical replay is add-only; use correction mode after review"
            )
        applied = apply_reconciliation is not None and result.has_changes
        if applied:
            apply_reconciliation(result)
            if refresh_downstream is None:
                raise ValueError("applied replay requires downstream refresh")
            refresh_downstream()
    except BaseException as error:
        after_failure = dict(read_watermarks())
        if after_failure != before:
            raise ReplayWatermarkChangedError(
                "Replay failure changed production watermark state"
            ) from error
        raise
    after = dict(read_watermarks())
    if after != before:
        raise ReplayWatermarkChangedError(
            "Replay changed production watermark state"
        )
    return ReplayExecution(request, result, applied, after, fresh)
