"""Readiness/drain acceptance, stale scope refusal and no terminal leakage."""

import importlib.util
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "pilot_relay",
    Path(__file__).resolve().parents[1] / "scripts/runtime_pilot_relay.py",
)
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def status():
    return {
        "unit": {"ActiveState": "active"},
        "stream": {
            "heartbeat_age_seconds": 1,
            "drained": False,
            "verifiedCheckpointBatchId": 0,
        },
        "containers": [
            {
                "service": name,
                "running": True,
                "healthy": name != "producer",
                "oom_killed": False,
                "exit_code": 0,
            }
            for name in ("kafka", "spark", "producer")
        ],
    }


def test_requested_start_is_not_confirmed_until_journal_and_health_match():
    value = status()
    assert not r.controller_confirmed(value, {})
    assert r.controller_confirmed(
        value, {"pilot_started_marker": True, "blocked_marker": False}
    )
    value["stream"]["heartbeat_age_seconds"] = 31
    assert not r.controller_confirmed(
        value, {"pilot_started_marker": True, "blocked_marker": False}
    )


def test_failed_or_missing_container_never_confirms_start():
    value = status()
    value["containers"][0]["oom_killed"] = True
    assert not r.controller_confirmed(
        value, {"pilot_started_marker": True, "blocked_marker": False}
    )
    value["containers"].pop()
    assert not r.controller_confirmed(
        value, {"pilot_started_marker": True, "blocked_marker": False}
    )


def test_drain_needs_checkpoint_marker_exit_codes_and_no_oom():
    value = status()
    journal = {"graceful_stop_verified_marker": True}
    assert not r.drain_confirmed(value, journal)
    value["stream"]["drained"] = True
    for item in value["containers"]:
        item["running"] = False
        item["exit_code"] = 143 if item["service"] == "kafka" else 0
    assert r.drain_confirmed(value, journal)
    value["containers"][1]["exit_code"] = 137
    assert not r.drain_confirmed(value, journal)


def test_format_only_or_expired_approval_never_contacts_sdk(tmp_path, monkeypatch):
    (tmp_path / "authorization.json").write_text(
        json.dumps(
            {
                "demo_approved": True,
                "max_controller_invocations": 0,
                "active_authorization": False,
            }
        )
    )
    monkeypatch.setattr(
        r.w,
        "clients",
        lambda *a: pytest.fail("SDK contacted without streaming approval"),
    )
    with pytest.raises(RuntimeError, match="STREAMING_SCOPE_APPROVAL_REQUIRED"):
        r.verify(tmp_path, tmp_path)


def test_empty_or_stale_reports_cannot_confirm_drain():
    assert not r.drain_confirmed({}, {})
    value = status()
    value["stream"]["verifiedCheckpointBatchId"] = None
    assert not r.drain_confirmed(value, {"graceful_stop_verified_marker": True})
