"""Runtime contract tests for the Aave forward DAG."""

from __future__ import annotations

import unittest

from airflow.models import DagBag


class AaveForwardDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_bag = DagBag(
            dag_folder="/opt/airflow/dags",
            include_examples=False,
        )
        cls.dag = cls.dag_bag.get_dag("aave_forward_incremental")

    def test_dag_imports_without_errors(self) -> None:
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(self.dag)

    def test_dag_is_manual_single_run_and_exclusive(self) -> None:
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        task = self.dag.get_task("process_increment")
        self.assertEqual(task.pool, "warehouse_writes")
        self.assertEqual(task.pool_slots, 4)

    def test_dag_delegates_business_logic(self) -> None:
        task = self.dag.get_task("process_increment")
        self.assertIn("orchestration.run_aave_forward", task.bash_command)
        self.assertNotIn("MERGE INTO", task.bash_command.upper())


if __name__ == "__main__":
    unittest.main()
