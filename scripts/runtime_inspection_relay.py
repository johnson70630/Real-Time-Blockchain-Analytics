"""Approved inspection-only SSM relay. Never starts EC2, containers or a stream."""

import argparse
import base64
import importlib.util
import json
import os
from pathlib import Path
import pty
import signal
import sys
import time
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runtime.inspection import validate_approval, INSTANCE, SAFE_REASONS  # noqa: E402
from runtime.recovery import _require  # noqa: E402
from runtime.recovery_metadata import canonical  # noqa: E402


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / file)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


window = module("inspection_window", "runtime_inspection_window.py")
terminal = module("inspection_terminal_helpers", "runtime_pilot_relay.py")


def guest_approval(owner, launch, boot, now):
    _require(
        type(boot) is dict
        and set(boot) == {"ok", "boot_id", "boot_epoch", "guest_epoch"}
        and boot["ok"] is True,
        "INSPECTION_BOOT_REPLY_INVALID",
    )
    _require(
        type(boot["boot_epoch"]) is int
        and launch - 5 <= boot["boot_epoch"] <= launch + 300
        and type(boot["guest_epoch"]) in (int, float)
        and abs(boot["guest_epoch"] - now) < 5,
        "INSPECTION_BOOT_REPLY_INVALID",
    )
    try:
        _require(
            str(UUID(boot["boot_id"])) == boot["boot_id"],
            "INSPECTION_BOOT_REPLY_INVALID",
        )
    except (ValueError, TypeError, AttributeError):
        raise RuntimeError("INSPECTION_BOOT_REPLY_INVALID") from None
    a = {
        "schema_version": 1,
        "purpose": "retained-metadata-inspection",
        "active_authorization": True,
        "owner_approved": True,
        "instance_id": INSTANCE,
        "source_commit": owner["source_commit"],
        "archive_sha256": owner["archive_sha256"],
        "manifest_sha256": owner["manifest_sha256"],
        "run_id": owner["run_id"],
        "boot_id": boot["boot_id"],
        "launch_epoch": launch,
        "setup_due_epoch": launch + 300,
        "normal_stop_epoch": launch + 600,
        "guest_poweroff_epoch": launch + 720,
        "window_end_epoch": launch + 900,
        "maximum_inspections": 1,
        "ec2_start_approved": True,
        "guest_shutdown_approved": True,
        "external_stop_approved": True,
        "container_starts": 0,
        "secret_reads": 0,
        "production_data_writes": 0,
        "terraform_calls": 0,
    }
    validate_approval(a, now)
    return a


def close_owned_session(pid, fd):
    os.close(fd)
    end = time.monotonic() + 5
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    while time.monotonic() < end:
        if os.waitpid(pid, os.WNOHANG)[0]:
            return
        time.sleep(0.1)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    os.waitpid(pid, 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    root = Path(window.MARKER.read_text().strip())
    owner, package = window.verify(root)
    window.local_dependencies()
    ec2, _ = window.old.clients(900)
    launch = window.admitted(root, ec2)
    window.receipt(root, "running-preflight.json", launch)
    intent = root / "relay.intent.json"
    fd = os.open(intent, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(canonical({"launch_epoch": launch, "maximum_inspections": 1}) + b"\n")
    pid = descriptor = None
    try:
        pid, descriptor = pty.fork()
        if pid == 0:
            os.execv(
                sys.executable,
                [
                    sys.executable,
                    str(ROOT / "scripts/runtime_inspection_window.py"),
                    "session",
                ],
            )
        terminal.read_until(descriptor, rb"[$#] ", 90, 262144)
        terminal.send(
            descriptor, "stty -echo; printf '\\nINSPECTION_NOECHO_READY\\n'\n"
        )
        terminal.read_until(descriptor, rb"INSPECTION_NOECHO_READY[\r\n]+", 20, 65536)
        boot_code = "import json,time;from pathlib import Path;print(json.dumps({'ok':True,'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'boot_epoch':int(next(x.split()[1] for x in Path('/proc/stat').read_text().splitlines() if x.startswith('btime '))),'guest_epoch':time.time()}),flush=True)"
        import shlex

        terminal.send(
            descriptor, "/usr/bin/python3 -B -c " + shlex.quote(boot_code) + "\n"
        )
        boot = terminal.read_json_reply(descriptor, 20, 65536)
        _require(window.admitted(root, ec2) == launch, "INSPECTION_BOOT_CHANGED")
        approval = guest_approval(owner, launch, boot, time.time())
        upload = (
            (package / "upload-command.template")
            .read_text()
            .replace(
                "__INSPECTION_APPROVAL_BASE64__",
                base64.b64encode(canonical(approval)).decode(),
            )
        )
        terminal.send(descriptor, upload)
        terminal.read_until(descriptor, rb"INSPECTION_FILES_STAGED[\r\n]+", 90, 262144)
        for action in ("controls", "inspect"):
            window.verify(root)
            _require(window.admitted(root, ec2) == launch, "INSPECTION_BOOT_CHANGED")
            terminal.send(
                descriptor, (package / (action + "-command.template")).read_text()
            )
            remaining = launch + 600 - time.time()
            _require(remaining > 0, "INSPECTION_WINDOW_CLOSED")
            result = terminal.read_json_reply(descriptor, min(remaining, 300), 65536)
            if result.get("ok") is False:
                reason = result.get("reason")
                window.old.write(
                    root / ("guest-" + action + "-refusal.json"),
                    {
                        "ok": False,
                        "phase": action,
                        "reason": reason
                        if type(reason) is str and reason in SAFE_REASONS
                        else "INSPECTION_GUEST_REFUSED",
                    },
                )
            _require(result.get("ok") is True, "INSPECTION_GUEST_REFUSED")
            if action == "controls":
                _require(
                    set(result) == {"ok", "controls_verified", "guest_poweroff_epoch"}
                    and result["controls_verified"] is True
                    and result["guest_poweroff_epoch"] == launch + 720,
                    "INSPECTION_CONTROLS_NOT_VERIFIED",
                )
                window.old.write(
                    root / "controls-confirmed.json",
                    {
                        "verified": True,
                        "launch_epoch": launch,
                        "guest_poweroff_epoch": launch + 720,
                    },
                )
            else:
                _require(
                    set(result)
                    == {
                        "ok",
                        "inspection_completed",
                        "candidate",
                        "identity",
                        "object_versions",
                        "container_starts",
                        "secret_reads",
                        "baseline_approved",
                    }
                    and result["inspection_completed"] is True
                    and result["baseline_approved"] is False
                    and type(result["container_starts"]) is int
                    and result["container_starts"] == 0
                    and type(result["secret_reads"]) is int
                    and result["secret_reads"] == 0,
                    "INSPECTION_RESULT_NOT_VERIFIED",
                )
                c = result["candidate"]
                _require(
                    c.get("status") == "RETAINED_METADATA_CANDIDATE_UNAPPROVED"
                    and c.get("baseline_approved") is False
                    and c.get("resume_authorized") is False
                    and c.get("broker_watermarks_verified") is False
                    and c.get("kafka_begin_offsets") is None
                    and c.get("kafka_end_offsets") is None,
                    "INSPECTION_RESULT_NOT_VERIFIED",
                )
            window.old.write(root / ("guest-" + action + "-result.json"), result)
            print(
                json.dumps(
                    {"action": action, "verified": True, "baseline_approved": False}
                ),
                flush=True,
            )
    finally:
        if pid not in (None, 0) and descriptor is not None:
            try:
                close_owned_session(pid, descriptor)
            except Exception:
                pass
        state = window.old.normal_stop(ec2)
        window.old.write(
            root / "relay-normal-stop.json",
            {"normal_stop_state": state, "checked_epoch": time.time()},
        )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(
            json.dumps(
                {
                    "ok": False,
                    "status": "INSPECTION_RELAY_BLOCKED",
                    "details_suppressed": True,
                }
            )
        )
        raise SystemExit(1)
