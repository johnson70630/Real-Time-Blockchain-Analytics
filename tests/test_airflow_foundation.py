"""Static safeguards for the containerized Airflow foundation."""

from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_ROOT = PROJECT_ROOT / "airflow"
COMPOSE_FILE = AIRFLOW_ROOT / "docker-compose.yaml"
DOCKERFILE = AIRFLOW_ROOT / "Dockerfile"
DAG_FILE = AIRFLOW_ROOT / "dags" / "airflow_smoke_test.py"


def _dag_decorator_keywords() -> dict[str, ast.expr]:
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "build_smoke_dag"
    )
    decorator = function.decorator_list[0]
    assert isinstance(decorator, ast.Call)
    return {keyword.arg: keyword.value for keyword in decorator.keywords if keyword.arg}


def test_airflow_runtime_is_pinned_and_isolated() -> None:
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    assert "name: blockchain-analytics-airflow" in compose
    assert "FROM apache/airflow:3.1.8-python3.12" in dockerfile
    assert "python -m venv /opt/warehouse-venv" in dockerfile
    assert "python -m venv /opt/project-venv" in dockerfile
    assert "openjdk-17-jre-headless" in dockerfile
    assert "AIRFLOW__CORE__EXECUTOR: LocalExecutor" in compose
    assert "AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8080/execution/" in compose
    assert "postgres:16-alpine" in compose
    assert '"127.0.0.1:${AIRFLOW_API_PORT:-8081}:8080"' in compose
    assert "redis" not in compose.lower()
    assert "celery" not in compose.lower()
    assert "airflow-ivy:/home/airflow/.ivy2.5.2" in compose


def test_operational_backfill_runtime_uses_existing_local_configuration() -> None:
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    assert "env_file:" in compose
    assert "path: ../.env" in compose
    assert "${HOME}/.aws:/home/airflow/.aws:ro" in compose
    assert "AAVE_V3_POOL_ADDRESS:" in compose
    assert "airflow pools set warehouse_writes 4" in compose


def test_smoke_dag_is_manual_and_disables_catchup() -> None:
    keywords = _dag_decorator_keywords()
    assert isinstance(keywords["schedule"], ast.Constant)
    assert keywords["schedule"].value is None
    assert isinstance(keywords["catchup"], ast.Constant)
    assert keywords["catchup"].value is False


def test_smoke_dag_declares_expected_tasks_and_retry() -> None:
    source = DAG_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    task_names = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(
            isinstance(decorator, ast.Name) and decorator.id == "task"
            for decorator in node.decorator_list
        )
    }
    assert task_names == {"start", "environment_check", "complete"}
    assert '"retries": 1' in source
    assert "start() >> environment_check() >> complete()" in source


def test_airflow_runtime_does_not_embed_project_credentials() -> None:
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    forbidden = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_PRIVATE_KEY",
    )
    assert not any(name in compose for name in forbidden)
    assert "POSTGRES_PASSWORD" not in compose
    assert "runtime_secret" in compose
