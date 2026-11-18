"""Explicitly invoked, bounded guest actions for a separately approved pilot.

Import is inert. The CLI requires root on the exact EC2 host and a verified
operator package. Never edits secrets, legacy files, Kafka identity or checkpoints.
"""

from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone
import importlib.util
import json
import math
import os
from pathlib import Path
import resource
import re
import subprocess
import tarfile
import time

from runtime import bundle, deployment as d, host
from runtime.contracts import BUCKET, RUN, validate_release, writer_lock

ROOT = Path(__file__).resolve().parents[1]
LEGACY_UNITS = Path("/etc/systemd/system")
LEGACY_BUNDLE_SHA = "669a806f2370cc33c43296b769b9e8eddb39d939eaeb624591f637782242a627"
LEGACY_CHAIN_SHA = "36cdf29df027706eafabeac7adfd772448e19c154f41c067d139265aad067b26"
MANIFEST = "operator.manifest.json"
FILES = (
    *bundle.FILES,
    bundle.MANIFEST,
    "host-candidate.tar",
    "legacy-host.tar",
    "release.json",
    "runtime/deployment.py",
    "runtime/pilot_operator.py",
    "runtime/pilot_sample.py",
    "scripts/runtime_demo_window_policy.py",
    "scripts/check_runtime_chain.py",
    "security-acceptance.json",
)


class ChainGateFailure(RuntimeError):
    def __init__(self, safe):
        self.stage, self.code = safe.stage, safe.code
        super().__init__("CHAIN_GATE_FAILED")


def require(ok, code):
    if not ok:
        raise RuntimeError(code)


def exclusive(path, value):
    body = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    descriptor = os.open(
        path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(body)


def application_stop_command(launch, now):
    require(
        type(launch) in (int, float)
        and type(now) in (int, float)
        and math.isfinite(launch)
        and math.isfinite(now)
        and launch > 0
        and 0 <= now - launch < 900,
        "SETUP_WINDOW_CLOSED",
    )
    calendar = datetime.fromtimestamp(launch + 1200, timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    return [
        "/usr/bin/systemd-run",
        "--unit=blockchain-pilot-application-stop",
        "--on-calendar=" + calendar,
        "--timer-property=AccuracySec=1s",
        "--timer-property=Persistent=false",
        "/usr/bin/systemctl",
        "stop",
        "--no-block",
        d.UNIT,
    ]


def package(root=ROOT):
    """External tar digest is checked by upload; internal hashes reject later edits."""
    d._directory(root)
    raw = (root / MANIFEST).read_bytes()
    require(len(raw) < 32768, "MANIFEST_TOO_LARGE")
    value = json.loads(raw)
    require(
        value.get("schema_version") == 1
        and set(value.get("file_sha256", {})) == set(FILES)
        and isinstance(value.get("source_commit"), str)
        and re.fullmatch("[0-9a-f]{40}", value["source_commit"])
        and value.get("instance_id") == d.INSTANCE
        and value.get("frozen_host_source_commit") == d.SOURCE_COMMIT
        and value.get("owner_approved") is False,
        "PACKAGE_INVENTORY_CHANGED",
    )
    for name, digest in value["file_sha256"].items():
        path = root / name
        require(
            not path.is_symlink()
            and path.is_file()
            and path.stat().st_uid == os.geteuid()
            and not path.stat().st_mode & 0o022,
            "PACKAGE_FILE_UNSAFE",
        )
        require(
            path.stat().st_size <= bundle.MAX_BUNDLE_BYTES, "PACKAGE_FILE_TOO_LARGE"
        )
        require(
            hashlib.sha256(path.read_bytes()).hexdigest() == digest,
            "PACKAGE_FILE_CHANGED",
        )
        for parent in path.relative_to(root).parents:
            if parent != Path("."):
                d._directory(root / parent)
    require(
        bundle.verify_bundle(root / "host-candidate.tar", d.BUNDLE_SHA)["git_commit"]
        == d.SOURCE_COMMIT,
        "HOST_SOURCE_CHANGED",
    )
    bundle.verify_bundle(root / "legacy-host.tar", LEGACY_BUNDLE_SHA)
    d.inspect_inputs(root / "host-candidate.tar", root / "release.json")
    return value


class Operator:
    def __init__(self, launch_epoch, root=ROOT):
        require(
            type(launch_epoch) in (int, float)
            and math.isfinite(launch_epoch)
            and launch_epoch > 0,
            "INVALID_LAUNCH",
        )
        self.launch_epoch, self.root = launch_epoch, Path(root)
        self.state = RUN / "reader-pilot-operator"
        self.release = json.loads((self.root / "release.json").read_text())
        validate_release(self.release)
        self.env = host.compose_environment(self.release)
        self.env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")

    def call(self, args, *, data=None, timeout=45, setup=False):
        if setup:
            remaining = self.launch_epoch + 900 - time.time()
            require(remaining > 0, "SETUP_WINDOW_CLOSED")
            timeout = min(timeout, remaining)
        result = subprocess.run(
            args,
            input=data,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout,
            env=self.env,
        )
        require(result.returncode == 0, "OPERATOR_COMMAND_FAILED")
        return result.stdout.strip()

    def legacy(self):
        require(
            d.LEGACY.is_dir() and d.LEGACY.resolve() == d.LEGACY, "LEGACY_ROOT_CHANGED"
        )
        d._directory(d.LEGACY)
        with tarfile.open(self.root / "legacy-host.tar", "r:") as archive:
            for name in bundle.FILES:
                path = d.LEGACY / name
                require(
                    not path.is_symlink()
                    and path.is_file()
                    and path.stat().st_uid == os.geteuid()
                    and not path.stat().st_mode & 0o022,
                    "LEGACY_FILE_REDIRECTED",
                )
                for parent in Path(name).parents:
                    if parent != Path("."):
                        d._directory(d.LEGACY / parent)
                require(
                    path.read_bytes() == archive.extractfile(name).read(),
                    "LEGACY_SOURCE_CHANGED",
                )
        for name, digest in (
            ("release.json", d.RELEASE_SHA),
            ("check_runtime_chain.py", LEGACY_CHAIN_SHA),
        ):
            path = d.LEGACY / name
            require(
                not path.is_symlink()
                and hashlib.sha256(path.read_bytes()).hexdigest() == digest,
                "LEGACY_AUXILIARY_CHANGED",
            )
        for name in (
            "blockchain-pilot.service",
            "blockchain-pilot-deadline.timer",
            "blockchain-pilot-poweroff.service",
        ):
            require(
                (LEGACY_UNITS / name).read_bytes()
                == (d.LEGACY / "deploy/runtime" / name).read_bytes(),
                "LEGACY_UNIT_CHANGED",
            )

    def guard(self, *, setup=False, controls=False, shutdown=False):
        d.require_host()
        package(self.root)
        self.legacy()
        require(
            self.call(["findmnt", "-n", "-o", "FSTYPE", "-T", "/run"]) == "tmpfs",
            "TMPFS_REQUIRED",
        )
        boot = int(
            next(
                line.split()[1]
                for line in Path("/proc/stat").read_text().splitlines()
                if line.startswith("btime ")
            )
        )
        require(
            self.launch_epoch - 5 <= boot <= self.launch_epoch + 300,
            "BOOT_TIMESTAMP_CHANGED",
        )
        require(time.time() >= self.launch_epoch, "CLOCK_BEFORE_LAUNCH")
        if setup:
            require(time.time() < self.launch_epoch + 900, "SETUP_WINDOW_CLOSED")
        if not controls:
            d._directory(self.state)
            receipt = json.loads((self.state / "controls.json").read_text())
            require(
                receipt["launch_epoch_seconds"] == self.launch_epoch
                and receipt["boot_id"]
                == Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                "CONTROL_BOOT_CHANGED",
            )
            if not shutdown:
                self.call(
                    [
                        "systemctl",
                        "is-active",
                        "--quiet",
                        "blockchain-pilot-window-deadline.timer",
                    ]
                )
        if setup and not controls:
            self.call(
                [
                    "systemctl",
                    "is-active",
                    "--quiet",
                    "blockchain-pilot-application-stop.timer",
                ]
            )
            host.preflight(self.release)

    def no_containers(self):
        require(
            self.call(["docker", "ps", "--all", "--quiet"]) == "",
            "EXISTING_CONTAINERS_REQUIRE_REVIEW",
        )

    def controls(self):
        self.guard(setup=True, controls=True)
        self.no_containers()
        RUN.mkdir(mode=0o755, exist_ok=True)
        d._directory(RUN)
        self.state.mkdir(mode=0o700)
        path = self.root / "scripts/runtime_demo_window_policy.py"
        spec = importlib.util.spec_from_file_location("approved_window_policy", path)
        policy = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(policy)
        self.call(
            policy.guest_deadline_command(self.launch_epoch, time.time()), setup=True
        )
        timer = policy.TRANSIENT_UNIT + ".timer"
        self.call(["systemctl", "is-active", "--quiet", timer])
        next_time = self.call(
            ["systemctl", "show", timer, "--property=NextElapseUSecRealtime", "--value"]
        )
        actual = int(self.call(["/usr/bin/date", "-u", "--date=" + next_time, "+%s"]))
        require(actual == int(self.launch_epoch + 2100), "ABSOLUTE_TIMER_CHANGED")
        self.call(application_stop_command(self.launch_epoch, time.time()), setup=True)
        app_timer = "blockchain-pilot-application-stop.timer"
        self.call(["systemctl", "is-active", "--quiet", app_timer])
        app_time = self.call(
            [
                "systemctl",
                "show",
                app_timer,
                "--property=NextElapseUSecRealtime",
                "--value",
            ]
        )
        app_epoch = int(self.call(["/usr/bin/date", "-u", "--date=" + app_time, "+%s"]))
        require(
            app_epoch == int(self.launch_epoch + 1200), "APPLICATION_STOP_TIMER_CHANGED"
        )
        # Existing manual fallback satisfies the frozen host preflight. The
        # earlier absolute timer and external watcher remain the active limits.
        self.call(["systemctl", "start", "blockchain-pilot-deadline.timer"])
        host.preflight(self.release)
        value = {
            "launch_epoch_seconds": self.launch_epoch,
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "guest_shutdown_epoch": actual,
            "application_stop_epoch": app_epoch,
        }
        exclusive(self.state / "controls.json", value)
        return {
            "controls_verified": True,
            "guest_shutdown_epoch": actual,
            "application_stop_epoch": app_epoch,
        }

    def install(self):
        self.guard(setup=True)
        self.no_containers()
        result = d.install_candidate(
            self.root / "host-candidate.tar", self.root / "release.json"
        )
        result["unit_sha256"] = d.stage_unit()
        self.call(["systemctl", "daemon-reload"])
        d.verify_unit()
        self.legacy()
        return {**result, "pilot_started": False}

    def accepted_images(self):
        value = json.loads((self.root / "security-acceptance.json").read_text())
        expiry = datetime.fromisoformat(
            value["expires_at_utc"].replace("Z", "+00:00")
        ).timestamp()
        require(
            value.get("accepted") is True
            and value.get("maximum_runs") == 1
            and value.get("streaming_runs_performed") == 0
            and self.launch_epoch + 2700 < expiry,
            "SECURITY_EXCEPTION_INVALID",
        )
        require(
            self.release["max_runtime_seconds"]
            <= value["maximum_streaming_duration_seconds"],
            "SECURITY_DURATION_CHANGED",
        )
        rows = {r["role"]: r for r in value["artifact_scope"]}
        require(set(rows) == {"producer", "spark", "kafka"}, "IMAGE_SCOPE_CHANGED")
        for role, row in rows.items():
            require(
                row["quarantined_digest_reference"] == self.release[role + "_image"],
                "IMAGE_SCOPE_CHANGED",
            )
        return rows

    def prepare(self):
        self.guard(setup=True)
        d.verify_install()
        d.verify_unit()
        self.no_containers()
        rows = self.accepted_images()
        exclusive(self.state / "prepare.intent.json", {"attempts": 1})
        listing = json.loads(
            self.call(
                [
                    "aws",
                    "s3api",
                    "list-objects-v2",
                    "--bucket",
                    BUCKET,
                    "--prefix",
                    self.release["pilot_prefix"] + "/",
                    "--max-keys",
                    "1",
                    "--output",
                    "json",
                ],
                setup=True,
            )
        )
        require(
            listing.get("KeyCount", 0) == 0 and not listing.get("Contents"),
            "PILOT_PREFIX_NOT_EMPTY",
        )
        config = RUN / "docker"
        if config.exists():
            d._directory(config)
            require(not any(config.iterdir()), "STALE_REGISTRY_AUTH")
        else:
            config.mkdir(mode=0o700)
        config.chmod(0o700)
        try:
            password = self.call(
                ["aws", "ecr", "get-login-password", "--region", "us-west-2"],
                setup=True,
            )
            self.call(
                [
                    "docker",
                    "login",
                    "--username",
                    "AWS",
                    "--password-stdin",
                    "209479274979.dkr.ecr.us-west-2.amazonaws.com",
                ],
                data=password,
                setup=True,
            )
            password = None
            for role in ("producer", "spark", "kafka"):
                image = self.release[role + "_image"]
                self.call(["docker", "pull", image], timeout=600, setup=True)
                actual = json.loads(
                    self.call(
                        ["docker", "image", "inspect", "--format", "{{json .}}", image],
                        setup=True,
                    )
                )
                require(
                    actual["Architecture"] == "amd64"
                    and actual["Id"] == rows[role]["config_digest"]
                    and image in actual["RepoDigests"],
                    "IMAGE_CONFIG_CHANGED",
                )
            self.chain_gate()
        finally:
            auth = config / "config.json"
            if os.path.lexists(auth):
                require(
                    not auth.is_symlink()
                    and auth.is_file()
                    and auth.stat().st_uid == os.geteuid(),
                    "REGISTRY_AUTH_PATH_UNSAFE",
                )
                auth.unlink()
        value = {
            "launch_epoch_seconds": self.launch_epoch,
            "chain_id": 42161,
            "private_chain_check_passed": True,
            "pilot_prefix_initially_empty": True,
            "three_image_identities_verified": True,
        }
        exclusive(self.state / "prepared.json", value)
        return value

    def chain_gate(self):
        remaining = self.launch_epoch + 900 - time.time()
        require(remaining > 0, "SETUP_WINDOW_CLOSED")
        path = self.root / "scripts/check_runtime_chain.py"
        spec = importlib.util.spec_from_file_location("approved_chain_gate", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        result = subprocess.run(
            [
                "/usr/bin/python3",
                "-B",
                str(path),
                "--execute-on-approved-host",
                str(self.root / "release.json"),
            ],
            env=self.env,
            text=True,
            capture_output=True,
            check=False,
            timeout=min(60, remaining),
        )
        if (
            result.returncode == 0
            and result.stdout.strip() == "ARBITRUM_CHAIN_ID_VERIFIED"
        ):
            return
        safe = module.parse_failure(result.stdout.strip()) or module.PreflightFailure(
            "HOST_PRECHECK", "HOST_ERROR"
        )
        raise ChainGateFailure(safe)

    def launch(self):
        self.guard(setup=True)
        d.verify_install()
        d.verify_unit()
        self.accepted_images()
        self.no_containers()
        value = json.loads((self.state / "prepared.json").read_text())
        require(
            value.get("private_chain_check_passed") is True
            and value.get("launch_epoch_seconds") == self.launch_epoch,
            "CHAIN_GATE_REQUIRED",
        )
        require(
            not (RUN / "secrets/alchemy_websocket_url").exists(),
            "STALE_SECRET_REQUIRES_REVIEW",
        )
        # Probe the same lock; the service independently holds it for its lifetime.
        with writer_lock(RUN / "host.lock"):
            exclusive(
                self.state / "controller.intent.json",
                {"attempts": 1, "launch_epoch_seconds": self.launch_epoch},
            )
        self.call(["systemctl", "start", "--no-block", d.UNIT], setup=True)
        return {
            "controller_start_requested": True,
            "controller_start_confirmed": False,
            "maximum_controller_invocations": 1,
        }

    def journal(self):
        self.guard()
        body = self.call(
            [
                "journalctl",
                "--boot=0",
                "--unit",
                d.UNIT,
                "--output",
                "cat",
                "--no-pager",
                "--lines",
                "100",
            ]
        )
        return {
            "pilot_started_marker": "PILOT_STARTED" in body.splitlines(),
            "graceful_stop_verified_marker": "PILOT_GRACEFUL_STOP_VERIFIED"
            in body.splitlines(),
            "blocked_marker": any(
                line.startswith("PILOT_BLOCKED") for line in body.splitlines()
            ),
        }

    def status(self):
        self.guard()
        result = {"unit": {}, "containers": []}
        for name in (
            "ActiveState",
            "SubState",
            "ExecMainCode",
            "ExecMainStatus",
            "Result",
        ):
            text = self.call(
                ["systemctl", "show", d.UNIT, "--property=" + name, "--value"]
            )
            require(
                len(text) < 80 and all(c.isalnum() or c in "-_" for c in text),
                "UNSAFE_STATUS",
            )
            result["unit"][name] = text
        pilot = host.Pilot(self.release)
        for role in ("kafka", "producer", "spark"):
            value = pilot.state(role)
            result["containers"].append(
                {
                    "service": role,
                    "running": value.get("Running") is True,
                    "healthy": value.get("Health", {}).get("Status") == "healthy",
                    "oom_killed": value.get("OOMKilled") is True,
                    "exit_code": value.get("ExitCode")
                    if type(value.get("ExitCode")) is int
                    else None,
                }
            )
        path = RUN / "pilot/status.json"
        if path.is_file():
            value = json.loads(path.read_text())
            progress = value.get("progress", {})
            counters = {
                name: progress.get(name)
                for name in ("batchId", "numInputRows", "verifiedCheckpointBatchId")
            }
            require(
                all(x is None or type(x) is int for x in counters.values()),
                "UNSAFE_PROGRESS",
            )
            heartbeat = value.get("heartbeat")
            require(
                type(heartbeat) in (int, float) and math.isfinite(heartbeat),
                "UNSAFE_HEARTBEAT",
            )
            result["stream"] = {
                **counters,
                "heartbeat_age_seconds": round(time.time() - heartbeat, 1),
                "drained": value.get("drained") is True,
            }
        return result

    def stop(self):
        self.guard(shutdown=True)
        require(
            (self.state / "controller.intent.json").is_file(), "NO_CONTROLLER_ATTEMPT"
        )
        self.call(["systemctl", "stop", "--no-block", d.UNIT])
        return {"graceful_stop_requested": True}

    def sample(self):
        from runtime.pilot_sample import sample

        return sample(self)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=[
            "controls",
            "install",
            "prepare",
            "launch",
            "status",
            "journal",
            "sample",
            "stop",
        ],
    )
    parser.add_argument("--launch-epoch", type=float, required=True)
    parser.add_argument(
        "--execute-on-approved-host", action="store_true", required=True
    )
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    try:
        d.require_host()
        package()
        result = getattr(Operator(args.launch_epoch), args.action)()
        print(json.dumps({"ok": True, **result}, sort_keys=True))
        return 0
    except ChainGateFailure as error:
        print(json.dumps({"ok": False, "stage": error.stage, "code": error.code}))
        return 1
    except Exception as error:
        # Subprocess output and exception messages can contain provider data.
        print(
            json.dumps(
                {
                    "ok": False,
                    "category": type(error).__name__,
                    "details_suppressed": True,
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
