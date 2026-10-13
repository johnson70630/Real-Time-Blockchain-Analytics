"""Orchestrate the finite S3-to-Snowflake warehouse lifecycle."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

PROJECT_ROOT = "/opt/project"
PYTHON = "/opt/warehouse-venv/bin/python"
DBT = "/opt/warehouse-venv/bin/dbt"
DBT_PROJECT_ARGS = f"--project-dir {PROJECT_ROOT}/dbt"


with DAG(
    dag_id="daily_warehouse_pipeline",
    description="Load canonical RAW data and build the dbt warehouse layers",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"retries": 0},
    tags=["warehouse", "snowflake", "dbt"],
) as daily_warehouse_pipeline:
    start = EmptyOperator(task_id="start")

    validate_environment = BashOperator(
        task_id="validate_environment",
        bash_command=f"{PYTHON} -m orchestration.validate_environment",
    )
    validate_silver_inputs = BashOperator(
        task_id="validate_silver_inputs",
        bash_command=f"{PYTHON} -m warehouse.load_raw --validate-inputs",
        retries=1,
        retry_delay=timedelta(seconds=30),
    )

    load_uniswap_raw = BashOperator(
        task_id="load_uniswap_raw",
        bash_command=(
            f"{PYTHON} -m warehouse.load_raw "
            "--dataset silver_uniswap_swaps"
        ),
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    load_reference_raw = BashOperator(
        task_id="load_reference_raw",
        bash_command=(
            f"{PYTHON} -m warehouse.load_raw "
            "--dataset reference_uniswap_v3_pools "
            "--dataset reference_tokens"
        ),
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    load_aave_raw = BashOperator(
        task_id="load_aave_raw",
        bash_command=(
            f"{PYTHON} -m warehouse.load_raw "
            "--dataset silver_aave_borrows "
            "--dataset silver_aave_repays "
            "--dataset silver_aave_liquidations"
        ),
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    load_chainlink_raw = BashOperator(
        task_id="load_chainlink_raw",
        bash_command=(
            f"{PYTHON} -m warehouse.load_raw "
            "--dataset silver_chainlink_market_prices"
        ),
        retries=1,
        retry_delay=timedelta(seconds=30),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"{DBT} seed {DBT_PROJECT_ARGS} --select token_price_feed_mapping"
        ),
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            f"{DBT} run {DBT_PROJECT_ARGS} --select path:models/staging"
        ),
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"{DBT} test {DBT_PROJECT_ARGS} --select path:models/staging "
            "--exclude test_type:singular"
        ),
    )
    dbt_core = BashOperator(
        task_id="dbt_core",
        bash_command=(
            f"{DBT} run {DBT_PROJECT_ARGS} "
            "--select path:models/intermediate path:models/core"
        ),
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    dbt_test_core = BashOperator(
        task_id="dbt_test_core",
        bash_command=(
            f"{DBT} test {DBT_PROJECT_ARGS} "
            "--select path:models/intermediate path:models/core "
            "--exclude test_type:singular"
        ),
    )
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"{DBT} run {DBT_PROJECT_ARGS} --select path:models/marts",
        retries=1,
        retry_delay=timedelta(seconds=30),
    )
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"{DBT} test {DBT_PROJECT_ARGS} --select path:models/marts "
            "--exclude test_type:singular"
        ),
    )
    reconciliation_checks = BashOperator(
        task_id="reconciliation_checks",
        bash_command=f"{DBT} test {DBT_PROJECT_ARGS} --select test_type:singular",
    )
    complete = EmptyOperator(task_id="complete")

    raw_loads = [
        load_uniswap_raw,
        load_reference_raw,
        load_aave_raw,
        load_chainlink_raw,
    ]
    start >> validate_environment >> validate_silver_inputs >> raw_loads
    raw_loads >> dbt_seed
    (
        dbt_seed
        >> dbt_staging
        >> dbt_test_staging
        >> dbt_core
        >> dbt_test_core
        >> dbt_marts
        >> dbt_test_marts
        >> reconciliation_checks
        >> complete
    )
