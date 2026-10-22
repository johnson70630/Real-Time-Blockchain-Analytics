"""Evaluate live health and persist deduplicated alert lifecycle state."""

from __future__ import annotations

import json
from contextlib import closing
from datetime import UTC, datetime

from orchestration.alerts import (
    evaluate_alerts,
    freshness_conditions,
    pipeline_failure_conditions,
)
from orchestration.run_freshness_checks import collect_freshness
from warehouse.alert_state import SnowflakeAlertStore, latest_pipeline_statuses
from warehouse.pipeline_runs import connect_pipeline_run_store


def main() -> None:
    observed_at = datetime.now(UTC)
    report = collect_freshness(checked_at=observed_at)
    with closing(connect_pipeline_run_store()) as connection:
        conditions = (
            *freshness_conditions(report),
            *pipeline_failure_conditions(latest_pipeline_statuses(connection)),
        )
        decisions = evaluate_alerts(
            SnowflakeAlertStore(connection),
            conditions,
            observed_at,
        )
    payload = {
        "observed_at": observed_at.isoformat(),
        "alerts": [
            {
                "alert_key": decision.record.alert_key,
                "state": decision.record.state.value,
                "severity": (
                    decision.record.severity.value
                    if decision.record.severity
                    else None
                ),
                "occurrence_count": decision.record.occurrence_count,
                "incident_generation": decision.record.incident_generation,
                "event_type": (
                    decision.event.event_type.value
                    if decision.event
                    else None
                ),
            }
            for decision in decisions
        ],
    }
    print(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
