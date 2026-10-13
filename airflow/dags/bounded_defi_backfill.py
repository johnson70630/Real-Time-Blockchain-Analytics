"""Orchestrate finite, manually requested DeFi historical backfills."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param

PROJECT_ROOT = "/opt/project"
WAREHOUSE_PYTHON = "/opt/warehouse-venv/bin/python"
PROJECT_PYTHON = "/opt/project-venv/bin/python"
DBT = "/opt/warehouse-venv/bin/dbt"
DBT_PROJECT_ARGS = f"--project-dir {PROJECT_ROOT}/dbt"
WAREHOUSE_POOL = "warehouse_writes"
EXCLUSIVE_WAREHOUSE_SLOTS = 4

REQUEST_ARGS = (
    '--backfill-type "{{ params.backfill_type }}" '
    '--start-block "{{ params.start_block }}" '
    '--end-block "{{ params.end_block }}" '
    '--dag-run-id "{{ run_id }}" '
    '--logical-date "{{ dag_run.logical_date or \'manual\' }}"'
)
BACKFILL = f"{PROJECT_PYTHON} -m orchestration.backfill"


with DAG(
    dag_id="bounded_defi_backfill",
    description="Run a finite Aave or Chainlink backfill and refresh the warehouse",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={"retries": 0},
    params={
        "backfill_type": Param(
            "aave",
            type="string",
            enum=["aave", "chainlink"],
        ),
        "start_block": Param(1, type="integer", minimum=0),
        "end_block": Param(2, type="integer", minimum=0),
    },
    tags=["backfill", "warehouse", "bounded"],
) as bounded_defi_backfill:
    start = EmptyOperator(task_id="start")

    validate_parameters = BashOperator(
        task_id="validate_parameters",
        bash_command=f"{BACKFILL} validate {REQUEST_ARGS}",
        execution_timeout=timedelta(minutes=2),
    )
    validate_environment = BashOperator(
        task_id="validate_environment",
        bash_command=(
            f"{WAREHOUSE_PYTHON} -m orchestration.validate_environment && "
            f"{BACKFILL} validate-environment {REQUEST_ARGS}"
        ),
        execution_timeout=timedelta(minutes=5),
    )
    run_selected_backfill = BashOperator(
        task_id="run_selected_backfill",
        bash_command=f"{BACKFILL} run {REQUEST_ARGS}",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=60),
        pool=WAREHOUSE_POOL,
        pool_slots=EXCLUSIVE_WAREHOUSE_SLOTS,
    )
    validate_backfill_output = BashOperator(
        task_id="validate_backfill_output",
        bash_command=f"{BACKFILL} validate-output {REQUEST_ARGS}",
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=10),
    )
    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=f"{BACKFILL} load {REQUEST_ARGS}",
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=10),
        pool=WAREHOUSE_POOL,
        pool_slots=EXCLUSIVE_WAREHOUSE_SLOTS,
    )
    dbt_refresh = BashOperator(
        task_id="dbt_refresh",
        bash_command=(
            f"{DBT} seed {DBT_PROJECT_ARGS} && "
            f"{DBT} run {DBT_PROJECT_ARGS}"
        ),
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=20),
        pool=WAREHOUSE_POOL,
        pool_slots=EXCLUSIVE_WAREHOUSE_SLOTS,
    )
    reconciliation_checks = BashOperator(
        task_id="reconciliation_checks",
        bash_command=f"{DBT} test {DBT_PROJECT_ARGS}",
        execution_timeout=timedelta(minutes=20),
    )
    complete = EmptyOperator(task_id="complete")

    (
        start
        >> validate_parameters
        >> validate_environment
        >> run_selected_backfill
        >> validate_backfill_output
        >> load_warehouse
        >> dbt_refresh
        >> reconciliation_checks
        >> complete
    )
