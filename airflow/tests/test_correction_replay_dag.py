"""Runtime contract tests for the manual correction replay DAG."""

from __future__ import annotations

import unittest
from pathlib import Path

from airflow.models import DagBag


class CorrectionReplayDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_bag = DagBag(
            dag_folder="/opt/airflow/dags",
            include_examples=False,
        )
        cls.dag = cls.dag_bag.get_dag("defi_correction_replay")

    def test_dag_imports_without_errors(self) -> None:
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(self.dag)

    def test_manual_single_run_configuration(self) -> None:
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_graph_and_exclusive_pool(self) -> None:
        ordered = (
            "start",
            "validate_parameters",
            "validate_environment",
            "reconcile_source",
            "complete",
        )
        self.assertEqual(set(self.dag.task_ids), set(ordered))
        for upstream, downstream in zip(ordered, ordered[1:]):
            self.assertEqual(
                self.dag.get_task(upstream).downstream_task_ids,
                {downstream},
            )
        task = self.dag.get_task("reconcile_source")
        self.assertEqual(task.pool, "warehouse_writes")
        self.assertEqual(task.pool_slots, 4)

    def test_dag_delegates_without_forward_or_streaming_commands(self) -> None:
        commands = "\n".join(
            getattr(task, "bash_command", "") for task in self.dag.tasks
        )
        self.assertIn("orchestration.run_correction_replay", commands)
        self.assertNotIn("run_uniswap_recovery", commands)
        self.assertNotIn("run_chainlink_forward", commands)
        self.assertNotIn("run_aave_forward", commands)
        self.assertTrue(
            Path("/opt/project/orchestration/run_correction_replay.py").is_file()
        )


if __name__ == "__main__":
    unittest.main()
