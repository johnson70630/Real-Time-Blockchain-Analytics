"""Run resumable Uniswap source-gap recovery outside the live stream."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

PROJECT_PYTHON = "/opt/project-venv/bin/python"
WAREHOUSE_POOL = "warehouse_writes"
EXCLUSIVE_WAREHOUSE_SLOTS = 4
RUNNER = f"{PROJECT_PYTHON} -m orchestration.run_uniswap_recovery"


with DAG(
    dag_id="uniswap_gap_recovery",
    description="Recover canonical Uniswap pool-event source gaps",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args={"retries": 0},
    tags=["uniswap", "recovery", "warehouse"],
) as uniswap_gap_recovery:
    start = EmptyOperator(task_id="start")
    validate_environment = BashOperator(
        task_id="validate_environment",
        bash_command=f"{RUNNER} validate-environment",
        execution_timeout=timedelta(minutes=5),
    )
    recover_source_ranges = BashOperator(
        task_id="recover_source_ranges",
        bash_command=f'{RUNNER} run --run-id "{{{{ run_id }}}}"',
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(hours=3),
        pool=WAREHOUSE_POOL,
        pool_slots=EXCLUSIVE_WAREHOUSE_SLOTS,
    )
    complete = EmptyOperator(task_id="complete")

    start >> validate_environment >> recover_source_ranges >> complete
