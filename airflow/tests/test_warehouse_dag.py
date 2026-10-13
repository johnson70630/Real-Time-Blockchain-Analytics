"""Runtime contract tests for the finite warehouse DAG."""

from __future__ import annotations

import unittest
from pathlib import Path

from airflow.models import DagBag


class WarehouseDagTests(unittest.TestCase):
    """Validate project-owned DAG structure and execution boundaries."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
        cls.dag = cls.dag_bag.get_dag("daily_warehouse_pipeline")

    def test_dag_imports_without_errors(self) -> None:
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(self.dag)

    def test_expected_tasks(self) -> None:
        self.assertEqual(
            set(self.dag.task_ids),
            {
                "start",
                "validate_environment",
                "validate_silver_inputs",
                "load_uniswap_raw",
                "load_reference_raw",
                "load_aave_raw",
                "load_chainlink_raw",
                "dbt_seed",
                "dbt_staging",
                "dbt_test_staging",
                "dbt_core",
                "dbt_test_core",
                "dbt_marts",
                "dbt_test_marts",
                "reconciliation_checks",
                "complete",
            },
        )

    def test_dependency_graph(self) -> None:
        self.assertEqual(
            self.dag.get_task("validate_silver_inputs").downstream_task_ids,
            {
                "load_uniswap_raw",
                "load_reference_raw",
                "load_aave_raw",
                "load_chainlink_raw",
            },
        )
        for task_id in (
            "load_uniswap_raw",
            "load_reference_raw",
            "load_aave_raw",
            "load_chainlink_raw",
        ):
            self.assertEqual(
                self.dag.get_task(task_id).downstream_task_ids,
                {"dbt_seed"},
            )
        ordered = (
            "dbt_seed",
            "dbt_staging",
            "dbt_test_staging",
            "dbt_core",
            "dbt_test_core",
            "dbt_marts",
            "dbt_test_marts",
            "reconciliation_checks",
            "complete",
        )
        for upstream, downstream in zip(ordered, ordered[1:]):
            self.assertEqual(
                self.dag.get_task(upstream).downstream_task_ids,
                {downstream},
            )

    def test_daily_schedule_and_run_limits(self) -> None:
        self.assertEqual(self.dag.schedule, "0 6 * * *")
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.dagrun_timeout.total_seconds(), 30 * 60)
        self.assertEqual(str(self.dag.timezone), "UTC")

    def test_retry_and_timeout_policy(self) -> None:
        retry_tasks = {
            "validate_silver_inputs",
            "load_uniswap_raw",
            "load_reference_raw",
            "load_aave_raw",
            "load_chainlink_raw",
            "dbt_seed",
            "dbt_staging",
            "dbt_core",
            "dbt_marts",
        }
        for task in self.dag.tasks:
            self.assertEqual(task.retries, int(task.task_id in retry_tasks))
            if task.task_id not in {"start", "complete"}:
                self.assertIsNotNone(task.execution_timeout)

    def test_shared_warehouse_pool(self) -> None:
        raw_tasks = {
            "load_uniswap_raw",
            "load_reference_raw",
            "load_aave_raw",
            "load_chainlink_raw",
        }
        exclusive_tasks = {"dbt_seed", "dbt_staging", "dbt_core", "dbt_marts"}
        for task_id in raw_tasks:
            task = self.dag.get_task(task_id)
            self.assertEqual(task.pool, "warehouse_writes")
            self.assertEqual(task.pool_slots, 1)
        for task_id in exclusive_tasks:
            task = self.dag.get_task(task_id)
            self.assertEqual(task.pool, "warehouse_writes")
            self.assertEqual(task.pool_slots, 4)

    def test_commands_keep_logic_outside_dag(self) -> None:
        commands = "\n".join(
            getattr(task, "bash_command", "") for task in self.dag.tasks
        )
        self.assertIn("warehouse.load_raw", commands)
        self.assertNotIn("/bin/dbt --project-dir", commands)
        for subcommand in ("seed", "run", "test"):
            self.assertIn(f"/bin/dbt {subcommand} --project-dir", commands)
        adapter_path = Path("/opt/project/warehouse/load_raw.py")
        adapter = adapter_path.read_text(encoding="utf-8")
        environment = Path(
            "/opt/project/orchestration/validate_environment.py"
        ).read_text(encoding="utf-8")
        self.assertIn('PIPELINE_ROLE = "BLOCKCHAIN_PIPELINE_ROLE"', adapter)
        self.assertIn('DBT_ROLE = "BLOCKCHAIN_DBT_ROLE"', environment)
        self.assertFalse(adapter_path.is_relative_to(Path("/opt/airflow/dags")))
        self.assertNotIn("MERGE INTO", commands.upper())
        self.assertNotIn("COPY INTO", commands.upper())
        for streaming_command in (
            "producer.run_producer",
            "market_data.run_poller",
            "spark.write_swaps_bronze",
            "spark.write_market_data_bronze",
        ):
            self.assertNotIn(streaming_command, commands)


if __name__ == "__main__":
    unittest.main()
