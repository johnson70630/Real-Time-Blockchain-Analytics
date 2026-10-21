"""Best-effort CLI for multi-task pipeline run lifecycle transitions."""

from __future__ import annotations

import argparse
import json
import logging
import re
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from orchestration.observability import (
    ErrorStage,
    PipelineRunRecord,
    RunIdentity,
    RunMetrics,
    RunStatus,
    RunType,
    terminal_record,
)
from warehouse.pipeline_runs import (
    SnowflakePipelineRunStore,
    connect_pipeline_run_store,
)

LOGGER = logging.getLogger(__name__)
_SAFE_ERROR_CLASS = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,127}$")


def _dbt_counts(project_dir: Path) -> tuple[int, int]:
    manifest = project_dir / "target" / "manifest.json"
    if not manifest.is_file():
        return 0, 0
    document = json.loads(manifest.read_text())
    nodes = document.get("nodes", {}).values()
    models = sum(node.get("resource_type") == "model" for node in nodes)
    tests = sum(node.get("resource_type") == "test" for node in nodes)
    return models, tests


def start_run(pipeline_name: str, run_id: str, run_type: RunType) -> None:
    """Create RUNNING state, logging only sanitized store failure metadata."""
    try:
        with closing(connect_pipeline_run_store()) as connection:
            SnowflakePipelineRunStore(connection).start(
                PipelineRunRecord(
                    RunIdentity(pipeline_name, run_id),
                    run_type,
                    datetime.now(UTC),
                )
            )
    except Exception as error:
        LOGGER.error(
            "Observability start failed pipeline=%s run_id=%s error_class=%s",
            pipeline_name,
            run_id,
            type(error).__name__,
        )


def finish_run(
    pipeline_name: str,
    run_id: str,
    status: RunStatus,
    *,
    error_stage: ErrorStage | None = None,
    error_class: str | None = None,
    dbt_project_dir: Path | None = None,
) -> None:
    """Finish existing RUNNING state without propagating observability failures."""
    try:
        with closing(connect_pipeline_run_store()) as connection:
            store = SnowflakePipelineRunStore(connection)
            current = store.get(RunIdentity(pipeline_name, run_id))
            if current is None:
                raise RuntimeError("Pipeline run was not started")
            details = dict(current.metrics.details)
            if dbt_project_dir is not None:
                models, tests = _dbt_counts(dbt_project_dir)
                details.update(dbt_models=models, dbt_tests=tests)
            metrics = RunMetrics(
                records_read=current.metrics.records_read,
                records_written=current.metrics.records_written,
                records_inserted=current.metrics.records_inserted,
                records_updated=current.metrics.records_updated,
                records_quarantined=current.metrics.records_quarantined,
                retry_count=current.metrics.retry_count,
                details=details,
            )
            now = datetime.now(UTC)
            error = None
            if status is RunStatus.FAILED:
                safe_class = error_class or "PipelineTaskFailure"
                if not _SAFE_ERROR_CLASS.fullmatch(safe_class):
                    safe_class = "PipelineTaskFailure"
                error = type(safe_class, (RuntimeError,), {})()
            store.finish(
                terminal_record(
                    current,
                    status,
                    metrics,
                    finished_at=now,
                    duration_seconds=max(
                        0.0,
                        (now - current.started_at.astimezone(UTC)).total_seconds(),
                    ),
                    error_stage=error_stage,
                    error=error,
                )
            )
    except Exception as error:
        LOGGER.error(
            "Observability finish failed pipeline=%s run_id=%s error_class=%s",
            pipeline_name,
            run_id,
            type(error).__name__,
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "succeed", "noop", "fail"))
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--run-type",
        choices=tuple(item.value for item in RunType),
        default=RunType.WAREHOUSE.value,
    )
    parser.add_argument(
        "--error-stage",
        choices=tuple(item.value for item in ErrorStage),
    )
    parser.add_argument("--error-class")
    parser.add_argument("--dbt-project-dir", type=Path)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()
    if args.action == "start":
        start_run(args.pipeline_name, args.run_id, RunType(args.run_type))
        return
    statuses = {
        "succeed": RunStatus.SUCCEEDED,
        "noop": RunStatus.NO_OP,
        "fail": RunStatus.FAILED,
    }
    stage = ErrorStage(args.error_stage) if args.error_stage else None
    if args.action == "fail" and stage is None:
        stage = ErrorStage.RECONCILIATION
    finish_run(
        args.pipeline_name,
        args.run_id,
        statuses[args.action],
        error_stage=stage,
        error_class=args.error_class,
        dbt_project_dir=args.dbt_project_dir,
    )


if __name__ == "__main__":
    main()
