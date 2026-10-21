"""Airflow callbacks for pipeline-level observability terminal state."""

from __future__ import annotations

import subprocess
from typing import Any

WAREHOUSE_PYTHON = "/opt/warehouse-venv/bin/python"

_TASK_STAGES = {
    "validate_environment": "validation",
    "validate_silver_inputs": "validation",
    "load_uniswap_raw": "snowflake",
    "load_reference_raw": "snowflake",
    "load_aave_raw": "snowflake",
    "load_chainlink_raw": "snowflake",
    "dbt_seed": "dbt",
    "dbt_staging": "dbt",
    "dbt_test_staging": "dbt",
    "dbt_core": "dbt",
    "dbt_test_core": "dbt",
    "dbt_marts": "dbt",
    "dbt_test_marts": "dbt",
    "reconciliation_checks": "reconciliation",
}


def record_daily_warehouse_failure(context: dict[str, Any]) -> None:
    """Mark a handled DAG failure without persisting exception text."""
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "")
    stage = _TASK_STAGES.get(task_id, "reconciliation")
    run_id = str(context.get("run_id") or "unknown")
    subprocess.run(
        (
            WAREHOUSE_PYTHON,
            "-m",
            "orchestration.run_observability",
            "fail",
            "--pipeline-name",
            "daily_warehouse_pipeline",
            "--run-id",
            run_id,
            "--error-stage",
            stage,
            "--error-class",
            "AirflowTaskFailure",
        ),
        check=False,
    )
