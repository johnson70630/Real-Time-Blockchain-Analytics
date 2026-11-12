"""Operator-invoked host controller. Never run this against AWS during preparation.

    python3 -m runtime.host validate deploy/runtime/release.example.json

Only ``run`` retrieves secrets or starts containers. It must be invoked on the
approved EC2 host after deployment approval, using a reviewed release manifest.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
import urllib.request
from pathlib import Path
from threading import Event

from runtime.contracts import (
    MOUNT,
    REGION,
    REGISTRY,
    RUN,
    validate_release,
    writer_lock,
)

ROOT = Path(__file__).resolve().parents[1]


def command(args, *, env=None, data=None, timeout=60) -> str:
    result = subprocess.run(
        args,
        input=data,
        text=True,
        capture_output=True,
        env=env,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        # CLI errors can contain provider responses. Never echo arguments,
        # stdout, stderr, exception chains, or the secret payload.
        raise RuntimeError(f"{Path(args[0]).name.upper()}_COMMAND_FAILED")
    return result.stdout.strip()


def metadata_instance() -> str:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    base = "http://169.254.169.254/latest/"
    request = urllib.request.Request(
        base + "api/token",
        method="PUT",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
    )
    with opener.open(request, timeout=5) as response:
        token = response.read().decode()
    request = urllib.request.Request(
        base + "meta-data/instance-id", headers={"X-aws-ec2-metadata-token": token}
    )
    with opener.open(request, timeout=5) as response:
        return response.read().decode()


def verify_mount(release, mount: dict, serial: str) -> None:
    if (
        mount.get("target") != str(MOUNT)
        or mount.get("fstype") != "xfs"
        or mount.get("uuid") != release["kafka_filesystem_uuid"]
        or serial.replace("-", "").strip()
        != release["kafka_volume_id"].replace("-", "")
    ):
        raise RuntimeError("EBS_MOUNT_IDENTITY_MISMATCH")


def preflight(release) -> None:
    if os.geteuid() != 0 or metadata_instance() != release["instance_id"]:
        raise RuntimeError("APPROVED_EC2_HOST_REQUIRED")
    if len(Path("/proc/swaps").read_text().splitlines()) != 1:
        raise RuntimeError("DISABLE_SWAP_BEFORE_SECRET_INJECTION")
    command(["systemctl", "is-active", "--quiet", "blockchain-pilot-deadline.timer"])
    mount = json.loads(
        command(
            [
                "findmnt",
                "--json",
                "--mountpoint",
                str(MOUNT),
                "--output",
                "TARGET,SOURCE,FSTYPE,UUID",
            ]
        )
    )["filesystems"][0]
    serial = command(["lsblk", "-ndo", "SERIAL", mount["source"]])
    verify_mount(release, mount, serial)
    if command(["findmnt", "-n", "-o", "FSTYPE", "-T", "/run"]) != "tmpfs":
        raise RuntimeError("SECRET_STORAGE_MUST_BE_TMPFS")
    if os.statvfs(MOUNT).f_bavail * os.statvfs(MOUNT).f_frsize < 10 * 1024**3:
        raise RuntimeError("KAFKA_FREE_SPACE_BELOW_10_GIB")
    meta = MOUNT / "data/meta.properties"
    data = MOUNT / "data"
    if data.exists() and not meta.exists() and any(data.iterdir()):
        raise RuntimeError("KAFKA_METADATA_MISSING_RECOVERY_REQUIRED")
    if meta.exists():
        properties = dict(
            line.split("=", 1)
            for line in meta.read_text().splitlines()
            if "=" in line and not line.startswith("#")
        )
        if (
            properties.get("cluster.id") != release["cluster_id"]
            or properties.get("node.id") != "1"
        ):
            raise RuntimeError("KAFKA_CLUSTER_ID_MISMATCH")


def compose_environment(release) -> dict[str, str]:
    # Do not inherit .env, AWS profiles, static credentials, proxies, or Compose
    # overrides. Docker and the AWS CLI use the instance role on this host.
    return {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME": "/root",
        "AWS_REGION": REGION,
        "AWS_DEFAULT_REGION": REGION,
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_PAGER": "",
        "COMPOSE_DISABLE_ENV_FILE": "1",
        "DOCKER_CONFIG": str(RUN / "docker"),
        "KAFKA_IMAGE": release["kafka_image"],
        "PRODUCER_IMAGE": release["producer_image"],
        "SPARK_IMAGE": release["spark_image"],
        "KAFKA_CLUSTER_ID": release["cluster_id"],
    }


class Pilot:
    def __init__(self, release):
        self.release = release
        self.env = compose_environment(release)
        self.deadline = None
        self.stopping = False
        self.base = [
            "docker",
            "compose",
            "-f",
            str(ROOT / "deploy/runtime/compose.yaml"),
        ]

    def execute(self, args, *, data=None, timeout=60):
        if self.deadline is not None and not self.stopping:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("PILOT_DURATION_EXPIRED")
            timeout = min(timeout, remaining)
        return command(args, env=self.env, data=data, timeout=timeout)

    def compose(self, *args, timeout=240):
        return self.execute([*self.base, *args], timeout=timeout)

    def state(self, service):
        identifier = self.compose("ps", "--all", "--quiet", service)
        if not identifier:
            return {}
        return json.loads(
            self.execute(
                ["docker", "inspect", "--format", "{{json .State}}", identifier],
            )
        )

    def prepare(self):
        for directory, mode, uid in (
            (RUN, 0o755, 0),
            (RUN / "secrets", 0o711, 0),
            (RUN / "pilot", 0o755, 10001),
            (RUN / "docker", 0o700, 0),
            (MOUNT / "data", 0o750, 1000),
        ):
            if directory.is_symlink():
                raise RuntimeError("RUNTIME_DIRECTORY_SYMLINK_FORBIDDEN")
            directory.mkdir(exist_ok=True)
            directory.chmod(mode)
            os.chown(directory, uid, uid)
        payload = self.execute(
            [
                "aws",
                "secretsmanager",
                "get-secret-value",
                "--region",
                REGION,
                "--secret-id",
                self.release["secret_arn"],
                "--output",
                "json",
            ],
        )
        value = json.loads(payload).get("SecretString")
        if (
            not isinstance(value, str)
            or not value.startswith("wss://")
            or "\n" in value
        ):
            raise ValueError("SECRET_MUST_CONTAIN_ONLY_WSS_URL")
        secret = RUN / "secrets/alchemy_websocket_url"
        # Refuse stale paths/symlinks. The controller removes its own tmpfs
        # files on shutdown; an interrupted run requires operator inspection.
        with secret.open("x") as handle:
            os.chmod(secret, 0o400)
            os.chown(secret, 10001, 10001)
            handle.write(value)
        password = self.execute(
            ["aws", "ecr", "get-login-password", "--region", REGION]
        )
        self.execute(
            ["docker", "login", "--username", "AWS", "--password-stdin", REGISTRY],
            data=password,
        )
        self.compose("pull", timeout=600)
        for field in ("kafka_image", "producer_image", "spark_image"):
            architecture = self.execute(
                [
                    "docker",
                    "image",
                    "inspect",
                    "--format",
                    "{{.Architecture}}",
                    self.release[field],
                ],
            )
            if architecture != "amd64":
                raise RuntimeError("AMD64_IMAGE_REQUIRED")

    def start(self):
        self.compose("up", "-d", "--wait", "--wait-timeout", "180", "kafka")
        self.compose("up", "--no-deps", "--exit-code-from", "topic-init", "topic-init")
        # Start the source first so Spark can report an initial committed batch.
        self.compose("up", "-d", "--no-deps", "producer")
        self.compose(
            "up",
            "-d",
            "--no-deps",
            "--wait",
            "--wait-timeout",
            "240",
            "spark",
            timeout=300,
        )

    def stop(self):
        self.stopping = True  # Graceful drain has its own bounded timeouts.
        self.compose("stop", "-t", "75", "producer", timeout=90)
        producer = self.state("producer")
        if producer and producer.get("ExitCode") != 0:
            raise RuntimeError("PRODUCER_STOP_FAILED_RECOVERY_REQUIRED")
        spark = self.state("spark")
        if spark.get("Running"):
            self.compose("stop", "-t", "180", "spark", timeout=195)
        status = RUN / "pilot/status.json"
        state = self.state("spark")
        if (
            not state
            or state.get("ExitCode") != 0
            or not status.exists()
            or not json.loads(status.read_text()).get("drained")
        ):
            raise RuntimeError("DRAIN_NOT_VERIFIED_PRESERVE_KAFKA_FOR_RECOVERY")
        self.compose("stop", "-t", "90", "kafka", timeout=105)
        kafka = self.state("kafka")
        # JVMs may report 128 + SIGTERM after running shutdown hooks.
        if kafka.get("ExitCode") not in (0, 143) or kafka.get("OOMKilled"):
            raise RuntimeError("KAFKA_STOP_FAILED_RECOVERY_REQUIRED")
        print("PILOT_GRACEFUL_STOP_VERIFIED", flush=True)

    def clear_secrets(self):
        (RUN / "secrets/alchemy_websocket_url").unlink(missing_ok=True)
        (RUN / "docker/config.json").unlink(missing_ok=True)


def run(release):
    preflight(release)
    RUN.mkdir(exist_ok=True, mode=0o755)
    stop = Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    with writer_lock(RUN / "host.lock"):
        pilot = Pilot(release)
        if any(
            pilot.state(service).get("Running")
            for service in ("producer", "spark", "kafka")
        ):
            raise RuntimeError("EXISTING_RUNTIME_REQUIRES_OPERATOR_RECOVERY")
        # Duration includes image preparation and startup, not just ingestion.
        deadline = time.monotonic() + release["max_runtime_seconds"]
        pilot.deadline = deadline
        try:
            pilot.prepare()
            pilot.start()
            print("PILOT_STARTED", flush=True)
            while time.monotonic() < deadline and not stop.wait(10):
                for service in ("kafka", "spark", "producer"):
                    state = pilot.state(service)
                    if (
                        not state.get("Running")
                        or state.get("Health", {}).get("Status") == "unhealthy"
                    ):
                        raise RuntimeError("PILOT_SERVICE_FAILED")
                if (
                    os.statvfs(MOUNT).f_bavail * os.statvfs(MOUNT).f_frsize
                    < 10 * 1024**3
                ):
                    raise RuntimeError("KAFKA_LOW_DISK_STOP_REQUIRED")
        finally:
            try:
                pilot.stop()
            finally:
                pilot.clear_secrets()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["validate", "run"])
    parser.add_argument("release", type=Path)
    args = parser.parse_args()
    try:
        release = json.loads(args.release.read_text())
        validate_release(release)
        if args.action == "run":
            run(release)
        else:
            print("OFFLINE_RELEASE_CONTRACT_VALID")
        return 0
    except Exception as error:
        print(f"PILOT_BLOCKED type={type(error).__name__}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
