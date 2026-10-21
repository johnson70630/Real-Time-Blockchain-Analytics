"""Best-effort Snowflake observer used by finite production runners."""

from __future__ import annotations

import logging
import os
from typing import Any

from orchestration.observability import (
    BestEffortRunRecorder,
    ErrorStage,
    PipelineOutcome,
    RunIdentity,
    RunMetrics,
    RunStatus,
    RunType,
)
from warehouse.pipeline_runs import (
    SnowflakePipelineRunStore,
    connect_pipeline_run_store,
)

LOGGER = logging.getLogger(__name__)


class PipelineRunObserver:
    """Own a best-effort recorder and its optional Snowflake connection."""

    def __init__(self, pipeline_name: str, run_id: str, run_type: RunType) -> None:
        self.connection: Any | None = None
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self._prior_environment: dict[str, str | None] = {}
        try:
            self.connection = connect_pipeline_run_store()
            store = SnowflakePipelineRunStore(self.connection)
        except Exception as error:
            LOGGER.error(
                "Observability connection failed pipeline=%s run_id=%s "
                "error_class=%s",
                pipeline_name,
                run_id,
                type(error).__name__,
            )
            store = None
        self.recorder = BestEffortRunRecorder(
            store,
            RunIdentity(pipeline_name, run_id),
            run_type,
        )

    def __enter__(self) -> PipelineRunObserver:
        for key, value in (
            ("PIPELINE_OBSERVABILITY_NAME", self.pipeline_name),
            ("PIPELINE_OBSERVABILITY_RUN_ID", self.run_id),
        ):
            self._prior_environment[key] = os.environ.get(key)
            os.environ[key] = value
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        try:
            if exc_value is not None and not self.recorder.finished:
                self.recorder.fail(exc_value)
        finally:
            if self.connection is not None:
                self.connection.close()
            for key, value in self._prior_environment.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def set_stage(self, stage: ErrorStage) -> None:
        self.recorder.set_stage(stage)

    def finish(self, outcome: PipelineOutcome) -> None:
        metrics = self._combined_metrics(outcome.metrics)
        if outcome.status is RunStatus.NO_OP:
            self.recorder.noop(metrics)
        else:
            self.recorder.finish(outcome.status, metrics)

    def fail(self, error: BaseException, metrics: RunMetrics | None = None) -> None:
        self.recorder.fail(error, metrics)

    def _combined_metrics(self, metrics: RunMetrics) -> RunMetrics:
        """Merge child-process counters already added to the durable RUNNING row."""
        if not isinstance(self.recorder.store, SnowflakePipelineRunStore):
            return metrics
        try:
            persisted = self.recorder.store.get(self.recorder.record.identity)
        except Exception as error:
            LOGGER.error(
                "Observability metric read failed pipeline=%s run_id=%s "
                "error_class=%s",
                self.pipeline_name,
                self.run_id,
                type(error).__name__,
            )
            return metrics
        if persisted is None:
            return metrics
        child = persisted.metrics
        details = dict(child.details)
        for key, value in metrics.details.items():
            details[key] = details.get(key, 0) + value
        is_warehouse_run = self.recorder.record.run_type is RunType.WAREHOUSE
        return RunMetrics(
            source_start_position=metrics.source_start_position,
            source_end_position=metrics.source_end_position,
            source_head_position=metrics.source_head_position,
            safe_head_position=metrics.safe_head_position,
            watermark_before=metrics.watermark_before,
            watermark_after=metrics.watermark_after,
            # Forward runners already count source records. RAW child processes
            # contribute Snowflake work only; adding their COPY/MERGE rows here
            # would count the same logical records twice. Warehouse runs have no
            # separate source counter, so their child totals remain authoritative.
            records_read=(
                metrics.records_read + child.records_read
                if is_warehouse_run
                else metrics.records_read
            ),
            records_written=(
                metrics.records_written + child.records_written
                if is_warehouse_run
                else metrics.records_written
            ),
            records_inserted=metrics.records_inserted + child.records_inserted,
            records_updated=metrics.records_updated + child.records_updated,
            records_quarantined=(
                metrics.records_quarantined + child.records_quarantined
            ),
            retry_count=metrics.retry_count + child.retry_count,
            details=details,
        )
