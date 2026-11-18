"""Scope-bound SSM relay for a new, explicitly approved streaming window.

Never starts EC2, prints terminal transcripts or forwards sample rows. The
completed format-only approval cannot satisfy this driver's scope requirements.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pty
import re
import select
import subprocess
import sys
import termios
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from runtime.pilot_acceptance import validate_sample  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "approved_window", REPO / "scripts/runtime_pilot_window.py"
)
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)
ACTIONS = {
    "controls",
    "install",
    "prepare",
    "launch",
    "status",
    "sample",
    "journal",
    "stop",
}


def controller_confirmed(status, journal):
    rows = status.get("containers", [])
    if len(rows) != 3 or {r.get("service") for r in rows} != {
        "kafka",
        "producer",
        "spark",
    }:
        return False
    heartbeat = status.get("stream", {}).get("heartbeat_age_seconds")
    return (
        status.get("unit", {}).get("ActiveState") == "active"
        and journal.get("pilot_started_marker") is True
        and journal.get("blocked_marker") is False
        and all(r.get("running") is True and r.get("oom_killed") is False for r in rows)
        and all(r.get("healthy") is True for r in rows if r["service"] != "producer")
        and type(heartbeat) in (int, float)
        and 0 <= heartbeat < 30
    )


def drain_confirmed(status, journal):
    rows = status.get("containers", [])
    stream = status.get("stream", {})
    return (
        len(rows) == 3
        and {r.get("service") for r in rows} == {"kafka", "producer", "spark"}
        and journal.get("graceful_stop_verified_marker") is True
        and stream.get("drained") is True
        and type(stream.get("verifiedCheckpointBatchId")) is int
        and stream["verifiedCheckpointBatchId"] >= 0
        and all(
            r.get("running") is False
            and r.get("oom_killed") is False
            and type(r.get("exit_code")) is int
            and r["exit_code"] in ((0, 143) if r["service"] == "kafka" else (0,))
            for r in rows
        )
    )


def verify(root, directory):
    w.require(
        not root.is_symlink()
        and root.is_dir()
        and root.stat().st_uid == os.geteuid()
        and not root.stat().st_mode & 0o077,
        "PRIVATE_EXECUTION_DIRECTORY_REQUIRED",
    )
    a = json.loads((root / "authorization.json").read_text())
    w.require(
        a.get("active_authorization") is True
        and a.get("owner_approved") is True
        and a.get("purpose") == "reader-compatible-pilot"
        and a.get("instance_id") == w.INSTANCE
        and a.get("window_minutes") == 45
        and a.get("normal_stop_due_minutes") == 30
        and a.get("manual_starts_consumed") == 1
        and a.get("demo_approved") is True
        and a.get("normal_stop_approved") is True
        and a.get("max_controller_invocations") == 1
        and a.get("pipeline_writes_approved") is True
        and a.get("candidate_install_approved") is True
        and a.get("keep_secret_unchanged") is True,
        "STREAMING_SCOPE_APPROVAL_REQUIRED",
    )
    w.require(
        subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
        ).strip()
        == a["source_commit"],
        "APPROVED_HEAD_CHANGED",
    )
    w.require(
        not subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=REPO,
            text=True,
        ).strip(),
        "TRACKED_SOURCE_CHANGED",
    )
    required = {
        "scripts/runtime_pilot_relay.py",
        "scripts/runtime_pilot_window.py",
        "scripts/runtime_demo_window_policy.py",
        "scripts/check_runtime_cloud_readiness.py",
        "runtime/pilot_operator.py",
        "runtime/pilot_sample.py",
        "runtime/pilot_acceptance.py",
        "runtime/deployment.py",
        "scripts/check_runtime_chain.py",
        "scripts/prepare_runtime_pilot_package.py",
    }
    w.require(
        required <= set(a["approved_artifact_sha256"]),
        "APPROVED_SOURCE_INVENTORY_INCOMPLETE",
    )
    for name, digest in a["approved_artifact_sha256"].items():
        w.require(
            not Path(name).is_absolute() and ".." not in Path(name).parts,
            "SOURCE_PATH_FORBIDDEN",
        )
        w.require(
            hashlib.sha256((REPO / name).read_bytes()).hexdigest() == digest,
            "APPROVED_SOURCE_CHANGED",
        )
    w.require(
        str(directory) == a["pilot_package_directory"]
        and directory.is_dir()
        and not directory.is_symlink(),
        "PACKAGE_DIRECTORY_CHANGED",
    )
    expected = {"operator.tar", "proposal.json", "upload-command.sh"} | {
        action + "-command.template" for action in ACTIONS
    }
    w.require(
        set(a["pilot_package_file_sha256"]) == expected, "PACKAGE_INVENTORY_CHANGED"
    )
    for name, digest in a["pilot_package_file_sha256"].items():
        path = directory / name
        w.require(
            not path.is_symlink()
            and path.is_file()
            and path.stat().st_uid == os.geteuid()
            and not path.stat().st_mode & 0o022
            and hashlib.sha256(path.read_bytes()).hexdigest() == digest,
            "PACKAGE_CHANGED",
        )
    proposal = json.loads((directory / "proposal.json").read_text())
    w.require(
        proposal["source_commit"] == a["source_commit"]
        and proposal["archive_sha256"] == a["pilot_package_archive_sha256"],
        "PACKAGE_SOURCE_CHANGED",
    )
    return a


def read_until(fd, pattern, timeout, limit):
    end, buffer = time.monotonic() + timeout, b""
    while time.monotonic() < end:
        if not select.select([fd], [], [], 1)[0]:
            continue
        try:
            data = os.read(fd, 65536)
        except OSError:
            raise RuntimeError("SSM_TERMINAL_ENDED") from None
        w.require(bool(data), "SSM_TERMINAL_ENDED")
        buffer += data
        w.require(len(buffer) <= limit, "TERMINAL_OUTPUT_LIMIT")
        found = re.search(pattern, buffer)
        if found:
            return found
    raise RuntimeError("SSM_REPLY_TIMEOUT")


def send(fd, command):
    data, offset = command.encode(), 0
    while offset < len(data):
        offset += os.write(fd, data[offset : offset + 2048])
        time.sleep(0.025)


def checkpoint(root, launch):
    status_path, journal_path = (
        root / "guest-status-result.json",
        root / "guest-journal-result.json",
    )
    if not status_path.is_file() or not journal_path.is_file():
        return
    status, journal = (
        json.loads(status_path.read_text()),
        json.loads(journal_path.read_text()),
    )
    if controller_confirmed(status, journal):
        intent = json.loads((root / "application-intent.json").read_text())
        intent.update(controller_start_confirmed=True, launch_epoch_seconds=launch)
        w.write(root / "application-intent.json", intent)
    if drain_confirmed(status, journal):
        w.write(
            root / "drain-acceptance.json",
            {
                "verified": True,
                "launch_epoch_seconds": launch,
                "checked_at_utc": w.now(),
            },
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-directory", type=Path, required=True)
    args = parser.parse_args()
    root = Path(w.MARKER.read_text().strip())
    verify(root, args.package_directory)
    ec2, _ssm = w.clients(3000)
    watch = w.require_admitted_watch(root, ec2)
    pid, fd = pty.fork()
    if pid == 0:
        os.execv(
            sys.executable,
            [sys.executable, str(REPO / "scripts/runtime_pilot_window.py"), "session"],
        )
    if sys.stdin.isatty():
        attrs = termios.tcgetattr(sys.stdin)
        attrs[3] &= ~termios.ECHO
        termios.tcsetattr(sys.stdin, termios.TCSANOW, attrs)
    try:
        read_until(fd, rb"sh-[0-9.]+[#$] ", 60, 1024 * 1024)
        os.write(fd, b"stty -echo\n")
        read_until(fd, rb"sh-[0-9.]+[#$] ", 15, 1024 * 1024)
        print(
            json.dumps({"ssm_session_ready": True, "transcripts_suppressed": True}),
            flush=True,
        )
        for line in sys.stdin:
            action = line.strip()
            if action == "exit":
                return
            w.require(action == "upload" or action in ACTIONS, "ACTION_FORBIDDEN")
            verify(root, args.package_directory)
            setup = action in {"upload", "controls", "install", "prepare", "launch"}
            watch = w.require_admitted_watch(root, ec2, before_preflight=setup)
            launch = watch["launch_epoch_seconds"]
            if action == "launch":
                from runtime.pilot_operator import exclusive

                exclusive(
                    root / "application-intent.json",
                    {
                        "controller_invocations": 1,
                        "controller_start_confirmed": False,
                        "launch_epoch_seconds": launch,
                    },
                )
            if action == "prepare":
                from runtime.pilot_operator import exclusive

                exclusive(
                    root / "image-validation-attempt.json",
                    {"attempts": 1, "one_run_exception_reserved": True},
                )
            command = (
                (
                    args.package_directory
                    / (
                        "upload-command.sh"
                        if action == "upload"
                        else action + "-command.template"
                    )
                )
                .read_text()
                .replace("__APPROVED_LAUNCH_EPOCH__", repr(launch))
            )
            print(json.dumps({"action_started": action}), flush=True)
            send(fd, command)
            if action == "upload":
                read_until(fd, rb"PILOT_OPERATOR_FILES_STAGED", 120, 2 * 1024 * 1024)
                result = {"ok": True, "upload_verified": True}
            else:
                match = read_until(
                    fd,
                    rb'(\{"[^\r\n]+\})\r?\n',
                    930 if action == "prepare" else 300,
                    100 * 1024 * 1024 if action == "sample" else 2 * 1024 * 1024,
                )
                result = json.loads(match.group(1))
                if action == "sample" and result.get("ok") is True:
                    try:
                        result = validate_sample(result)
                    except Exception as error:
                        result = {
                            "ok": False,
                            "stage": "PARQUET_ACCEPTANCE",
                            "category": type(error).__name__,
                            "details_suppressed": True,
                        }
            w.write(root / ("guest-" + action + "-result.json"), result)
            print(json.dumps({"action": action, "result": result}), flush=True)
            if result.get("ok") is not True:
                print(
                    json.dumps(
                        {
                            "normal_stop_state": w.normal_stop(ec2),
                            "reason": "GATE_FAILED",
                        }
                    ),
                    flush=True,
                )
                return
            if action in {"status", "journal"}:
                checkpoint(root, launch)
    finally:
        try:
            os.write(fd, b"exit\n")
        except OSError:
            pass
        os.close(fd)
        try:
            os.waitpid(pid, os.WNOHANG)
        except OSError:
            pass
        try:
            state = w.normal_stop(ec2)
            w.write(
                root / "relay-normal-stop.json",
                {
                    "checked_at_utc": w.now(),
                    "state": state,
                    "normal_stop_requested": True,
                },
            )
            print(json.dumps({"normal_stop_state": state}), flush=True)
        except Exception:
            print(
                json.dumps({"console_normal_stop_verification_required": True}),
                flush=True,
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(
            json.dumps(
                {
                    "STOP": "PILOT_RELAY_ENDED",
                    "category": type(error).__name__,
                    "details_suppressed": True,
                }
            ),
            flush=True,
        )
        raise SystemExit(1)
