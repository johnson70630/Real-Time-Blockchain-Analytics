"""Fake EC2 and clocks; importing the watcher never loads credentials or clients."""

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "reader_window",
    Path(__file__).resolve().parents[1] / "scripts/runtime_pilot_window.py",
)
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)
ARM = 1795000000


class EC2:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.stops = []

    def describe_instances(self, **kwargs):
        assert kwargs == {"InstanceIds": [w.INSTANCE]}
        elapsed, state, launch = next(self.rows)
        self.elapsed = elapsed
        node = {
            "InstanceId": w.INSTANCE,
            "State": {"Name": state},
            "LaunchTime": datetime.fromtimestamp(ARM + launch, timezone.utc),
            "Placement": {"AvailabilityZone": "us-west-2a"},
            "Tags": [{"Key": k, "Value": v} for k, v in w.TAGS.items()],
        }
        return {"Reservations": [{"Instances": [node]}]}

    def stop_instances(self, **kwargs):
        self.stops.append(kwargs)
        return {
            "StoppingInstances": [
                {"InstanceId": w.INSTANCE, "CurrentState": {"Name": "stopping"}}
            ]
        }


def simulate(tmp_path, rows):
    ec2 = EC2(rows)
    times = iter(ARM + row[0] for row in rows)
    result = w.watch(
        ec2,
        tmp_path,
        ARM,
        clock=lambda: next(times),
        pause=lambda _: None,
        emit=lambda *a, **k: None,
    )
    return ec2, result, json.loads((tmp_path / "watch.json").read_text())


def test_no_start_expires_after_short_visibility_grace(tmp_path):
    ec2, result, _ = simulate(
        tmp_path, [(0, "stopped", -100), (300, "stopped", -100), (330, "stopped", -100)]
    )
    assert result["phase"] == "expired_without_start"
    assert ec2.stops == []


def test_late_start_is_never_armed_and_normal_stopped(tmp_path):
    # normal_stop re-reads ownership/state before issuing its request.
    ec2, result, _ = simulate(
        tmp_path, [(310, "running", 305), (310, "running", 305), (320, "stopped", 305)]
    )
    assert result["start_admission"] == "LATE_START"
    assert result["phase"] == "completed_stopped"
    assert ec2.stops == [
        {
            "InstanceIds": [w.INSTANCE],
            "Hibernate": False,
            "Force": False,
            "SkipOsShutdown": False,
        }
    ]


def test_late_pending_boot_remains_supervised_until_stop_possible(tmp_path):
    ec2, result, _ = simulate(
        tmp_path,
        [
            (310, "pending", 305),
            (340, "running", 305),
            (340, "running", 305),
            (350, "stopped", 305),
        ],
    )
    assert result["normal_stop_requested"] is True
    assert len(ec2.stops) == 1


def test_timely_start_observed_in_grace_retains_original_deadlines(tmp_path):
    _, result, _ = simulate(tmp_path, [(310, "running", 299), (320, "stopped", 299)])
    assert result["start_admission"] == "ACCEPTED"
    assert result["normal_stop_due_epoch"] == ARM + 299 + 1800


def test_missing_application_stops_after_setup_budget(tmp_path):
    ec2, result, _ = simulate(
        tmp_path, [(910, "running", 10), (910, "running", 10), (920, "stopped", 10)]
    )
    assert len(ec2.stops) == 1
    assert result["preflight_due_epoch"] == ARM + 910


def test_confirmation_from_another_boot_does_not_extend_setup(tmp_path):
    (tmp_path / "application-intent.json").write_text(
        json.dumps(
            {
                "controller_invocations": 1,
                "controller_start_confirmed": True,
                "launch_epoch_seconds": ARM - 10,
            }
        )
    )
    ec2, _, _ = simulate(
        tmp_path, [(910, "running", 10), (910, "running", 10), (920, "stopped", 10)]
    )
    assert len(ec2.stops) == 1


def test_session_gate_rejects_late_or_stale_watch_before_client_use(
    tmp_path, monkeypatch
):
    record = {"phase": "stop_due", "start_admission": "LATE_START"}
    (tmp_path / "watch.json").write_text(json.dumps(record))
    with pytest.raises(RuntimeError, match="WATCH_NOT_ADMITTED"):
        w.require_admitted_watch(tmp_path, object())
    record.update(
        phase="armed", start_admission="ACCEPTED", heartbeat_epoch_seconds=ARM
    )
    (tmp_path / "watch.json").write_text(json.dumps(record))
    monkeypatch.setattr(w.time, "time", lambda: ARM + 30)
    with pytest.raises(RuntimeError, match="WATCH_HEARTBEAT_STALE"):
        w.require_admitted_watch(tmp_path, object())


def test_session_gate_rejects_boot_change(tmp_path, monkeypatch):
    (tmp_path / "watch.json").write_text(
        json.dumps(
            {
                "phase": "armed",
                "start_admission": "ACCEPTED",
                "armed_epoch_seconds": ARM,
                "heartbeat_epoch_seconds": ARM + 100,
                "launch_epoch_seconds": ARM + 10,
            }
        )
    )
    monkeypatch.setattr(w.time, "time", lambda: ARM + 101)
    with pytest.raises(RuntimeError, match="BOOT_CHANGED"):
        w.require_admitted_watch(tmp_path, EC2([(101, "running", 20)]))


def test_preflight_receipt_cannot_be_from_another_boot_or_future(tmp_path):
    from datetime import timedelta

    path = tmp_path / "receipt.json"
    value = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "preflight_passed": True,
        "instance_state": "running",
        "launch_epoch_seconds": ARM,
    }
    path.write_text(json.dumps(value))
    w.fresh_receipt(tmp_path, path.name, "preflight_passed", "running", ARM)
    with pytest.raises(RuntimeError, match="PREFLIGHT_BOOT_MISMATCH"):
        w.fresh_receipt(tmp_path, path.name, "preflight_passed", "running", ARM + 1)
    value["checked_at_utc"] = (
        datetime.now(timezone.utc) + timedelta(minutes=1)
    ).isoformat()
    path.write_text(json.dumps(value))
    with pytest.raises(RuntimeError, match="FRESH_PREFLIGHT_RECEIPT_REQUIRED"):
        w.fresh_receipt(tmp_path, path.name, "preflight_passed", "running", ARM)
