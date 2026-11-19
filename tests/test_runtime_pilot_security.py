"""A consumed or unverifiable exception never permits another cloud operation."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest

from runtime import pilot_security as security

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def records(tmp_path):
    accepted_bytes = (ROOT / security.ACCEPTANCE).read_bytes()
    ledger = json.loads((ROOT / security.CONSUMPTION).read_bytes())
    target = tmp_path / security.CONSUMPTION
    target.parent.mkdir(parents=True)
    (tmp_path / security.ACCEPTANCE).write_bytes(accepted_bytes)
    target.write_text(json.dumps(ledger))
    return tmp_path, target, ledger


def test_real_exception_consumed_and_hash_bound():
    ledger = json.loads((ROOT / security.CONSUMPTION).read_bytes())
    assert ledger["acceptance_record_sha256"] == hashlib.sha256(
        (ROOT / security.ACCEPTANCE).read_bytes()
    ).hexdigest()
    with pytest.raises(RuntimeError, match="SECURITY_EXCEPTION_ALREADY_CONSUMED"):
        security.require_unused_acceptance(ROOT)


def test_hypothetical_unused_record_only(records):
    root, target, ledger = records
    ledger.update(
        demonstration_windows_consumed=0,
        streaming_runs_performed=0,
        controller_attempts=0,
        validation_container_executed=False,
        status="SINGLE_DEMONSTRATION_EXCEPTION_UNUSED",
    )
    target.write_text(json.dumps(ledger))
    security.require_unused_acceptance(root)


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", True),
        ("acceptance_record_path", "elsewhere"),
        ("acceptance_record_sha256", "0" * 64),
        ("reviewed_scope_sha256", "0" * 64),
        ("maximum_demonstration_windows", True),
        ("maximum_demonstration_windows", 2),
        ("demonstration_windows_consumed", False),
        ("demonstration_windows_consumed", -1),
        ("demonstration_windows_consumed", 2),
        ("streaming_runs_performed", True),
        ("streaming_runs_performed", 0),
        ("controller_attempts", 0),
        ("controller_attempts", True),
        ("validation_container_executed", False),
        ("new_start_authorized", True),
        ("status", "SINGLE_DEMONSTRATION_EXCEPTION_UNUSED"),
    ],
)
def test_invalid_history_is_never_approval(records, field, value):
    root, target, ledger = records
    ledger[field] = value
    target.write_text(json.dumps(ledger))
    with pytest.raises(RuntimeError, match="SECURITY_CONSUMPTION_RECORD_INVALID"):
        security.require_unused_acceptance(root)


@pytest.mark.parametrize(
    "case", ["missing", "symlink", "oversize", "json", "duplicate", "fifo", "directory"]
)
def test_unreadable_or_ambiguous_history_fails_closed(records, case):
    root, target, _ = records
    if case == "missing":
        target.unlink()
    elif case == "symlink":
        target.unlink()
        target.symlink_to(ROOT / security.CONSUMPTION)
    elif case in {"fifo", "directory"}:
        target.unlink()
        if case == "fifo":
            os.mkfifo(target)
        else:
            target.mkdir()
    else:
        target.write_bytes(
            b"x" * (security.MAX_BYTES + 1)
            if case == "oversize"
            else b"{"
            if case == "json"
            else b'{"demonstration_windows_consumed":1,"demonstration_windows_consumed":0}'
        )
    with pytest.raises(RuntimeError, match="SECURITY_CONSUMPTION_RECORD_INVALID"):
        security.require_unused_acceptance(root)


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / ("scripts/" + name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cloud_readiness_refuses_before_any_sdk_or_receipt_access(monkeypatch):
    module = load_script("check_runtime_cloud_readiness")
    monkeypatch.setattr(sys, "argv", ["check_runtime_cloud_readiness.py"])
    monkeypatch.setattr(
        module.w, "clients", lambda *args: pytest.fail("unexpected SDK access")
    )
    with pytest.raises(RuntimeError, match="SECURITY_EXCEPTION_ALREADY_CONSUMED"):
        module.main()


def test_even_new_streaming_scope_cannot_reuse_exception(tmp_path, monkeypatch):
    module = load_script("runtime_pilot_relay")
    tmp_path.chmod(0o700)
    (tmp_path / "authorization.json").write_text(
        json.dumps(
            dict(
                active_authorization=True,
                owner_approved=True,
                purpose="reader-compatible-pilot",
                instance_id=module.w.INSTANCE,
                window_minutes=45,
                normal_stop_due_minutes=30,
                manual_starts_consumed=1,
                demo_approved=True,
                normal_stop_approved=True,
                max_controller_invocations=1,
                pipeline_writes_approved=True,
                candidate_install_approved=True,
                keep_secret_unchanged=True,
            )
        )
    )
    monkeypatch.setattr(
        module.subprocess,
        "check_output",
        lambda *args, **kwargs: pytest.fail("consumption gate was bypassed"),
    )
    with pytest.raises(RuntimeError, match="SECURITY_EXCEPTION_ALREADY_CONSUMED"):
        module.verify(tmp_path, tmp_path)
