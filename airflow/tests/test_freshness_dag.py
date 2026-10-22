"""Structural contracts for freshness checks and alert-state evaluation."""

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

    def test_dag_contains_only_health_and_alert_flow(self) -> None:
        self.assertEqual(
            set(self.dag.task_ids),
            {
                "start",
                "calculate_freshness",
                "evaluate_alerts",
                "final_health_gate",
                "complete",
            },
        )
        calculate = self.dag.get_task("calculate_freshness")
        evaluate = self.dag.get_task("evaluate_alerts")
        gate = self.dag.get_task("final_health_gate")
        self.assertIn("orchestration.run_freshness_checks", calculate.bash_command)
        self.assertNotIn("--fail-on-stale", calculate.bash_command)
        self.assertIn("orchestration.run_alert_evaluation", evaluate.bash_command)
        self.assertIn("--fail-on-stale", gate.bash_command)
        self.assertEqual(calculate.downstream_task_ids, {"evaluate_alerts"})
        self.assertEqual(evaluate.downstream_task_ids, {"final_health_gate"})
        for task in (calculate, evaluate, gate):
            for forbidden in ("load_raw", "dbt", "advance"):
                self.assertNotIn(forbidden, task.bash_command)


if __name__ == "__main__":
    unittest.main()
