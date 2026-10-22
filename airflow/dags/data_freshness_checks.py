"""Run read-only, provider-neutral data freshness and SLA checks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

PROJECT_PYTHON = "/opt/project-venv/bin/python"
FRESHNESS = f"{PROJECT_PYTHON} -m orchestration.run_freshness_checks"


with DAG(
    dag_id="data_freshness_checks",
    description="Evaluate freshness and persist deduplicated alert state",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 0},
    tags=["observability", "freshness", "alerts"],
) as data_freshness_checks:
    start = EmptyOperator(task_id="start")
    calculate_freshness = BashOperator(
        task_id="calculate_freshness",
        bash_command=FRESHNESS,
        execution_timeout=timedelta(minutes=8),
    )
    evaluate_alerts = BashOperator(
        task_id="evaluate_alerts",
        bash_command=f"{PROJECT_PYTHON} -m orchestration.run_alert_evaluation",
        execution_timeout=timedelta(minutes=8),
    )
    final_health_gate = BashOperator(
        task_id="final_health_gate",
        bash_command=f"{FRESHNESS} --fail-on-stale",
        execution_timeout=timedelta(minutes=8),
    )
    complete = EmptyOperator(task_id="complete")

    (
        start
        >> calculate_freshness
        >> evaluate_alerts
        >> final_health_gate
        >> complete
    )
