"""Runtime contract tests for the manual Uniswap recovery DAG."""

from __future__ import annotations

import unittest

from airflow.models import DagBag


class UniswapRecoveryDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_bag = DagBag(
            dag_folder="/opt/airflow/dags",
            include_examples=False,
        )
        cls.dag = cls.dag_bag.get_dag("uniswap_gap_recovery")

    def test_dag_import_and_manual_single_run_contract(self) -> None:
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(self.dag)
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_recovery_is_exclusive_and_delegated(self) -> None:
        task = self.dag.get_task("recover_source_ranges")
        self.assertEqual(task.pool, "warehouse_writes")
        self.assertEqual(task.pool_slots, 4)
        self.assertIn("orchestration.run_uniswap_recovery", task.bash_command)
        self.assertEqual(
            self.dag.get_task("start").downstream_task_ids,
            {"validate_environment"},
        )
        self.assertEqual(
            self.dag.get_task("validate_environment").downstream_task_ids,
            {"recover_source_ranges"},
        )
        self.assertEqual(task.downstream_task_ids, {"complete"})


if __name__ == "__main__":
    unittest.main()
