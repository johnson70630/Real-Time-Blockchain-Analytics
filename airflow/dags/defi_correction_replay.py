"""Manually audit a bounded source interval for replay corrections."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param

PROJECT_PYTHON = "/opt/project-venv/bin/python"
WAREHOUSE_POOL = "warehouse_writes"
EXCLUSIVE_WAREHOUSE_SLOTS = 4
RUNNER = f"{PROJECT_PYTHON} -m orchestration.run_correction_replay"
REQUEST_ARGS = (
    '--source "{{ params.source }}" '
    '--start-block "{{ params.start_block }}" '
    '--end-block "{{ params.end_block }}" '
    '--mode "{{ params.replay_mode }}" '
    '--run-id "{{ run_id }}"'
)


with DAG(
    dag_id="defi_correction_replay",
    description="Audit a bounded source replay without advancing production state",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={"retries": 0},
    params={
        "source": Param(
            "aave",
            type="string",
            enum=["aave", "chainlink", "uniswap"],
        ),
        "start_block": Param(1, type="integer", minimum=0),
        "end_block": Param(1, type="integer", minimum=0),
        "replay_mode": Param(
            "correction",
            type="string",
            enum=["historical", "correction"],
        ),
    },
    tags=["replay", "correction", "bounded", "warehouse"],
) as defi_correction_replay:
    start = EmptyOperator(task_id="start")
    validate_parameters = BashOperator(
        task_id="validate_parameters",
        bash_command=f"{RUNNER} validate {REQUEST_ARGS}",
        execution_timeout=timedelta(minutes=2),
    )
    validate_environment = BashOperator(
        task_id="validate_environment",
        bash_command=f"{RUNNER} validate-environment {REQUEST_ARGS}",
        execution_timeout=timedelta(minutes=5),
    )
    reconcile_source = BashOperator(
        task_id="reconcile_source",
        bash_command=f"{RUNNER} audit {REQUEST_ARGS}",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=90),
        pool=WAREHOUSE_POOL,
        pool_slots=EXCLUSIVE_WAREHOUSE_SLOTS,
    )
    complete = EmptyOperator(task_id="complete")

    start >> validate_parameters >> validate_environment >> reconcile_source >> complete
