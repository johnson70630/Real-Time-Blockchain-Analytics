"""Provider-neutral lifecycle contracts for durable pipeline run metrics."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from time import monotonic
from typing import Callable, Mapping, Protocol

LOGGER = logging.getLogger(__name__)
MAX_POSITION = 10**38 - 1


class RunStatus(StrEnum):
    """Terminal and non-terminal pipeline execution states."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    NO_OP = "NO_OP"
    SKIPPED = "SKIPPED"


class RunType(StrEnum):
    """Stable execution categories independent from an RPC provider."""

    FORWARD = "forward"
    WAREHOUSE = "warehouse"
    BACKFILL = "backfill"
    CORRECTION = "correction"


class ErrorStage(StrEnum):
    """Small set of actionable high-level pipeline failure boundaries."""

    VALIDATION = "validation"
    SOURCE_FETCH = "source_fetch"
    PERSISTENCE = "persistence"
    SILVER = "silver"
    SNOWFLAKE = "snowflake"
    DBT = "dbt"
    RECONCILIATION = "reconciliation"
    WATERMARK_COMMIT = "watermark_commit"


class DuplicateRunError(RuntimeError):
    """Raised when a logical pipeline/run identity already exists."""


class RunStateConflictError(RuntimeError):
    """Raised when a run cannot make the requested lifecycle transition."""


def _required_text(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _position(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0 or value > MAX_POSITION:
        raise ValueError(f"{field_name} is outside NUMBER(38, 0) range")
    return value


def _count(value: int, field_name: str) -> int:
    result = _position(value, field_name)
    assert result is not None
    return result


def _metric_details(values: Mapping[str, int] | None) -> dict[str, int]:
    details: dict[str, int] = {}
    for key, value in (values or {}).items():
        normalized = _required_text(key, "metric detail key")
        if not normalized.replace("_", "").isalnum():
            raise ValueError("metric detail keys must be alphanumeric snake_case")
        details[normalized] = _count(value, f"metric_details[{normalized}]")
    return details


@dataclass(frozen=True, slots=True)
class RunIdentity:
    """Natural key for one pipeline execution."""

    pipeline_name: str
    run_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "pipeline_name", _required_text(self.pipeline_name, "pipeline_name")
        )
        object.__setattr__(self, "run_id", _required_text(self.run_id, "run_id"))


@dataclass(frozen=True, slots=True)
class RunMetrics:
    """Small generic metric set shared across production pipelines."""

    source_start_position: int | None = None
    source_end_position: int | None = None
    source_head_position: int | None = None
    safe_head_position: int | None = None
    watermark_before: int | None = None
    watermark_after: int | None = None
    records_read: int = 0
    records_written: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_quarantined: int = 0
    retry_count: int = 0
    details: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in (
            "source_start_position",
            "source_end_position",
            "source_head_position",
            "safe_head_position",
            "watermark_before",
            "watermark_after",
        ):
            _position(getattr(self, field_name), field_name)
        for field_name in (
            "records_read",
            "records_written",
            "records_inserted",
            "records_updated",
            "records_quarantined",
            "retry_count",
        ):
            _count(getattr(self, field_name), field_name)
        object.__setattr__(self, "details", _metric_details(self.details))

    @property
    def made_progress(self) -> bool:
        """Return whether a durable source position advanced."""
        return (
            self.watermark_before is not None
            and self.watermark_after is not None
            and self.watermark_after > self.watermark_before
        )


@dataclass(frozen=True, slots=True)
class PipelineRunRecord:
    """One durable pipeline execution without event data or secret material."""

    identity: RunIdentity
    run_type: RunType
    started_at: datetime
    status: RunStatus = RunStatus.RUNNING
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    metrics: RunMetrics = field(default_factory=RunMetrics)
    error_stage: ErrorStage | None = None
    error_class: str | None = None

    def __post_init__(self) -> None:
        if self.started_at.tzinfo is None:
            raise ValueError("started_at must be timezone-aware")
        if self.finished_at is not None and self.finished_at.tzinfo is None:
            raise ValueError("finished_at must be timezone-aware")
        if self.status is RunStatus.RUNNING:
            if self.finished_at is not None or self.duration_seconds is not None:
                raise ValueError("RUNNING records must not have terminal timestamps")
        elif self.finished_at is None or self.duration_seconds is None:
            raise ValueError("Terminal records require finish time and duration")
        if self.duration_seconds is not None and self.duration_seconds < 0:
            raise ValueError("duration_seconds must not be negative")
        if self.status is RunStatus.FAILED:
            if self.error_stage is None or not self.error_class:
                raise ValueError("FAILED records require sanitized error metadata")
        elif self.error_stage is not None or self.error_class is not None:
            raise ValueError("Only FAILED records may contain error metadata")


class PipelineRunStore(Protocol):
    """Persistence boundary for lifecycle-safe run records."""

    def start(self, record: PipelineRunRecord) -> PipelineRunRecord: ...

    def finish(self, record: PipelineRunRecord) -> PipelineRunRecord: ...


def terminal_record(
    running: PipelineRunRecord,
    status: RunStatus,
    metrics: RunMetrics,
    *,
    finished_at: datetime,
    duration_seconds: float,
    error_stage: ErrorStage | None = None,
    error: BaseException | None = None,
) -> PipelineRunRecord:
    """Build a valid terminal record with error class but never error text."""
    if running.status is not RunStatus.RUNNING:
        raise RunStateConflictError("Only RUNNING records may finish")
    if status is RunStatus.RUNNING:
        raise ValueError("Terminal status must not be RUNNING")
    error_class = type(error).__name__ if error is not None else None
    return replace(
        running,
        status=status,
        finished_at=finished_at.astimezone(UTC),
        duration_seconds=duration_seconds,
        metrics=metrics,
        error_stage=error_stage,
        error_class=error_class,
    )


class BestEffortRunRecorder:
    """Record metrics without allowing store failures to corrupt pipeline work."""

    def __init__(
        self,
        store: PipelineRunStore | None,
        identity: RunIdentity,
        run_type: RunType,
        *,
        clock: Callable[[], datetime] | None = None,
        timer: Callable[[], float] | None = None,
    ) -> None:
        self.store = store
        self.clock = clock or (lambda: datetime.now(UTC))
        self.timer = timer or monotonic
        self.started_tick = self.timer()
        self.record = PipelineRunRecord(identity, run_type, self.clock())
        self.stage = ErrorStage.VALIDATION
        self.finished = False
        if self.store is not None:
            try:
                self.store.start(self.record)
            except Exception as error:
                self._disable("start", error)

    def _disable(self, operation: str, error: BaseException) -> None:
        LOGGER.error(
            "Observability %s failed pipeline=%s run_id=%s error_class=%s",
            operation,
            self.record.identity.pipeline_name,
            self.record.identity.run_id,
            type(error).__name__,
        )
        self.store = None

    def set_stage(self, stage: ErrorStage) -> None:
        """Set the current high-level boundary for sanitized failure reporting."""
        self.stage = stage

    def finish(
        self,
        status: RunStatus,
        metrics: RunMetrics,
        *,
        error: BaseException | None = None,
    ) -> PipelineRunRecord:
        """Attempt one terminal transition and return it even if storage fails."""
        if self.finished:
            raise RunStateConflictError("Pipeline run was already finished")
        self.finished = True
        terminal = terminal_record(
            self.record,
            status,
            metrics,
            finished_at=self.clock(),
            duration_seconds=max(0.0, self.timer() - self.started_tick),
            error_stage=self.stage if status is RunStatus.FAILED else None,
            error=error,
        )
        if self.store is not None:
            try:
                self.store.finish(terminal)
            except Exception as store_error:
                self._disable("finish", store_error)
        return terminal

    def succeed(self, metrics: RunMetrics) -> PipelineRunRecord:
        return self.finish(RunStatus.SUCCEEDED, metrics)

    def noop(self, metrics: RunMetrics) -> PipelineRunRecord:
        return self.finish(RunStatus.NO_OP, metrics)

    def fail(
        self,
        error: BaseException,
        metrics: RunMetrics | None = None,
    ) -> PipelineRunRecord:
        return self.finish(RunStatus.FAILED, metrics or RunMetrics(), error=error)


@dataclass(frozen=True, slots=True)
class PipelineOutcome:
    """Terminal status and aggregate metrics returned by an instrumented runner."""

    status: RunStatus
    metrics: RunMetrics

    def __post_init__(self) -> None:
        if self.status not in {RunStatus.SUCCEEDED, RunStatus.NO_OP, RunStatus.SKIPPED}:
            raise ValueError("PipelineOutcome requires a non-failure terminal status")
