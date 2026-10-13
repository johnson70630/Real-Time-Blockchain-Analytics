"""Runtime contract tests for the bounded operational backfill DAG."""

from __future__ import annotations

import unittest
from pathlib import Path

from airflow.models import DagBag


class BackfillDagTests(unittest.TestCase):
    """Validate finite backfill structure and safety boundaries."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_bag = DagBag(
            dag_folder="/opt/airflow/dags",
            include_examples=False,
        )
        cls.dag = cls.dag_bag.get_dag("bounded_defi_backfill")

    def test_dag_imports_without_errors(self) -> None:
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(self.dag)

    def test_manual_bounded_configuration(self) -> None:
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.dagrun_timeout.total_seconds(), 2 * 60 * 60)

    def test_expected_linear_graph(self) -> None:
        ordered = (
            "start",
            "validate_parameters",
            "validate_environment",
            "run_selected_backfill",
            "validate_backfill_output",
            "load_warehouse",
            "dbt_refresh",
            "reconciliation_checks",
            "complete",
        )
        self.assertEqual(set(self.dag.task_ids), set(ordered))
        for upstream, downstream in zip(ordered, ordered[1:]):
            self.assertEqual(
                self.dag.get_task(upstream).downstream_task_ids,
                {downstream},
            )

    def test_retry_policy_and_shared_pool(self) -> None:
        retry_tasks = {
            "run_selected_backfill",
            "validate_backfill_output",
            "load_warehouse",
            "dbt_refresh",
        }
        for task in self.dag.tasks:
            self.assertEqual(task.retries, int(task.task_id in retry_tasks))
        for task_id in (
            "run_selected_backfill",
            "load_warehouse",
            "dbt_refresh",
        ):
            task = self.dag.get_task(task_id)
            self.assertEqual(task.pool, "warehouse_writes")
            self.assertEqual(task.pool_slots, 4)

    def test_dag_invokes_external_entrypoints_only(self) -> None:
        commands = "\n".join(
            getattr(task, "bash_command", "") for task in self.dag.tasks
        )
        self.assertIn("orchestration.backfill", commands)
        self.assertIn("orchestration.validate_environment", commands)
        adapter = Path("/opt/project/orchestration/backfill.py")
        self.assertTrue(adapter.is_file())
        self.assertFalse(adapter.is_relative_to(Path("/opt/airflow/dags")))
        for streaming_command in (
            "producer.run_producer",
            "market_data.run_poller",
            "spark.write_swaps_bronze",
            "spark.write_market_data_bronze",
        ):
            self.assertNotIn(streaming_command, commands)


if __name__ == "__main__":
    unittest.main()
