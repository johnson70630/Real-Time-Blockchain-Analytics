"""Structural contract tests for the read-only freshness DAG."""

import unittest

from airflow.models import DagBag


class FreshnessDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_bag = DagBag(
            dag_folder="/opt/airflow/dags",
            include_examples=False,
        )
        cls.dag = cls.dag_bag.get_dag("data_freshness_checks")

    def test_dag_imports_and_is_manual_single_run(self) -> None:
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(self.dag)
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_dag_contains_only_read_only_freshness_flow(self) -> None:
        self.assertEqual(
            set(self.dag.task_ids),
            {"start", "evaluate_freshness", "complete"},
        )
        command = self.dag.get_task("evaluate_freshness").bash_command
        self.assertIn("orchestration.run_freshness_checks", command)
        self.assertIn("--fail-on-stale", command)
        for forbidden in ("load_raw", "dbt", "advance", "INSERT", "UPDATE"):
            self.assertNotIn(forbidden, command)


if __name__ == "__main__":
    unittest.main()
