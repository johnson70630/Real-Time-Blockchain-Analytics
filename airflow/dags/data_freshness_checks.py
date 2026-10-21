"""Run read-only, provider-neutral data freshness and SLA checks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

PROJECT_PYTHON = "/opt/project-venv/bin/python"


with DAG(
    dag_id="data_freshness_checks",
    description="Evaluate source, feed, load, and transformation freshness",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 0},
    tags=["observability", "freshness", "read-only"],
) as data_freshness_checks:
    start = EmptyOperator(task_id="start")
    evaluate_freshness = BashOperator(
        task_id="evaluate_freshness",
        bash_command=(
            f"{PROJECT_PYTHON} -m orchestration.run_freshness_checks "
            "--fail-on-stale"
        ),
        execution_timeout=timedelta(minutes=8),
    )
    complete = EmptyOperator(task_id="complete")

    start >> evaluate_freshness >> complete
