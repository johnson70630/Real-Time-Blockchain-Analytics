"""Runtime contract tests for the Airflow smoke DAG."""

from __future__ import annotations

import unittest

from airflow.models import DagBag


class SmokeDagTests(unittest.TestCase):
    """Validate our DAG contract without testing Airflow internals."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
        cls.dag = cls.dag_bag.get_dag("airflow_smoke_test")

    def test_dag_imports_without_errors(self) -> None:
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(self.dag)

    def test_expected_tasks_and_dependencies(self) -> None:
        self.assertEqual(
            set(self.dag.task_ids),
            {"start", "environment_check", "complete"},
        )
        self.assertEqual(self.dag.get_task("start").downstream_task_ids, {"environment_check"})
        self.assertEqual(self.dag.get_task("environment_check").downstream_task_ids, {"complete"})
        self.assertEqual(self.dag.get_task("complete").downstream_task_ids, set())

    def test_dag_is_manual_without_catchup(self) -> None:
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)

    def test_retry_policy(self) -> None:
        for task in self.dag.tasks:
            self.assertEqual(task.retries, 1)


if __name__ == "__main__":
    unittest.main()
