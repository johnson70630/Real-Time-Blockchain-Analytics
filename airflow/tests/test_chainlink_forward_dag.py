"""Runtime contract tests for the Chainlink forward DAG."""

from __future__ import annotations

import unittest

from airflow.models import DagBag


class ChainlinkForwardDagTests(unittest.TestCase):
    """Validate the finite manual DAG and exclusive mutation boundary."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_bag = DagBag(
            dag_folder="/opt/airflow/dags",
            include_examples=False,
        )
        cls.dag = cls.dag_bag.get_dag("chainlink_forward_incremental")

    def test_dag_imports_without_errors(self) -> None:
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(self.dag)

    def test_dag_is_manual_and_single_run(self) -> None:
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_graph_and_exclusive_pool(self) -> None:
        ordered = ("start", "validate_environment", "process_increment", "complete")
        self.assertEqual(set(self.dag.task_ids), set(ordered))
        for upstream, downstream in zip(ordered, ordered[1:]):
            self.assertEqual(
                self.dag.get_task(upstream).downstream_task_ids,
                {downstream},
            )
        task = self.dag.get_task("process_increment")
        self.assertEqual(task.pool, "warehouse_writes")
        self.assertEqual(task.pool_slots, 4)

    def test_dag_delegates_business_logic(self) -> None:
        command = self.dag.get_task("process_increment").bash_command
        self.assertIn("orchestration.run_chainlink_forward", command)
        self.assertNotIn("MERGE INTO", command.upper())


if __name__ == "__main__":
    unittest.main()
