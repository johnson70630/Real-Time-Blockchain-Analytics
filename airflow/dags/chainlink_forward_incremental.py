"""Orchestrate one finite, commit-last Chainlink forward increment."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

PROJECT_PYTHON = "/opt/project-venv/bin/python"
WAREHOUSE_POOL = "warehouse_writes"
EXCLUSIVE_WAREHOUSE_SLOTS = 4
RUNNER = f"{PROJECT_PYTHON} -m orchestration.run_chainlink_forward"


with DAG(
    dag_id="chainlink_forward_incremental",
    description="Process one frozen Chainlink snapshot and commit state last",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args={"retries": 0},
    tags=["chainlink", "incremental", "warehouse"],
) as chainlink_forward_incremental:
    start = EmptyOperator(task_id="start")
    validate_environment = BashOperator(
        task_id="validate_environment",
        bash_command=f"{RUNNER} validate-environment",
        execution_timeout=timedelta(minutes=5),
    )
    process_increment = BashOperator(
        task_id="process_increment",
        bash_command=f'{RUNNER} run --run-id "{{{{ run_id }}}}"',
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=45),
        pool=WAREHOUSE_POOL,
        pool_slots=EXCLUSIVE_WAREHOUSE_SLOTS,
    )
    complete = EmptyOperator(task_id="complete")

    start >> validate_environment >> process_increment >> complete
