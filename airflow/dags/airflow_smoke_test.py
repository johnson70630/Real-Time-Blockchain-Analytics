"""Minimal manually triggered DAG proving the local Airflow runtime works."""

from __future__ import annotations

import logging
import platform
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="airflow_smoke_test",
    description="Validate DAG discovery, scheduling, task execution, and logs",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    tags=["development", "smoke"],
)
def build_smoke_dag():
    """Define a finite smoke workflow with no external dependencies."""

    @task
    def start() -> None:
        logger.info("Airflow smoke test started")

    @task
    def environment_check() -> None:
        logger.info("Airflow task is running on Python %s", platform.python_version())

    @task
    def complete() -> None:
        logger.info("Airflow smoke test completed")

    start() >> environment_check() >> complete()


airflow_smoke_test = build_smoke_dag()
