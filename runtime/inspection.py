"""Guarded retained-metadata inspection; never starts an application container.

Import is inert. Production CLI requires a separately approved, hash-bound boot
authorization. Results are unapproved candidates, never recovery permission.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import fcntl
import io
import json
import math
import os
from pathlib import Path
import re
import resource
import subprocess
import tarfile
import tempfile
import time
import urllib.request
from uuid import UUID

from runtime.contracts import TOPIC
from runtime.recovery import RecoveryBlocked, _require, _topic_identity
from runtime.recovery_metadata import (
    S3MetadataSource,
    LIST_KEYS,
    MAX_OBJECT,
    MAX_TOTAL,
    _numbers,
    _log,
    parse_json,
    canonical,
)
from runtime.inspection_transport import (
    AwsCliMetadataClient,
    retained_disk_identity,
    private_directory,
    safe_binary,
    child_limits,
)
from runtime.recovery_metadata import SAFE_CODES as METADATA_REASONS

SAFE_REASONS = METADATA_REASONS | frozenset(
    """
INSPECTION_APPROVAL_REQUIRED INSPECTION_WINDOW_CLOSED INSPECTION_APPROVAL_CHANGED
INSPECTION_PACKAGE_CHANGED INSPECTION_FILE_UNSAFE INSPECTION_FILE_TOO_LARGE
INSPECTION_ROOT_REQUIRED INSPECTION_SWAP_ENABLED INSPECTION_TMPFS_REQUIRED
INSPECTION_BOOT_CHANGED INSPECTION_HOST_CHANGED INSPECTION_VOLUME_CHANGED
INSPECTION_RELEASE_CHANGED INSPECTION_APPLICATION_ACTIVE INSPECTION_CONTAINER_ACTIVE
INSPECTION_TIMER_REQUIRED INSPECTION_TIMER_CHANGED INSPECTION_SETUP_CLOSED
INSPECTION_LOCK_UNSAFE INSPECTION_WRITER_ACTIVE INSPECTION_AWS_CLI_REQUIRED
INSPECTION_BINARY_UNSAFE INSPECTION_AWS_CLI_FAILED INSPECTION_OPERATION_FORBIDDEN
INSPECTION_REPLY_TOO_LARGE INSPECTION_OUTPUT_UNSAFE INSPECTION_METADATA_INVALID
INSPECTION_METADATA_CHANGED INSPECTION_EXPECTED_BATCH_CHANGED
INSPECTION_CLUSTER_IDENTITY_CHANGED INSPECTION_DISK_PATH_UNSAFE
INSPECTION_DISK_METADATA_INVALID INSPECTION_WORKLOAD_ROLE_CHANGED
INSPECTION_HOST_CHECK_FAILED INSPECTION_SCRATCH_UNSAFE
""".split()
)

INSTANCE = "i-0ca1671a59751acc5"
VOLUME = "vol-02c9fb14a9c568fb5"
FILESYSTEM = "da1454d8-1bea-41a3-89d7-87bd570a47bd"
CLUSTER = "vLEW3m8cQNyTdif0Nm5U2g"
MOUNT = Path("/var/lib/blockchain-runtime/kafka")
RELEASE = Path("/opt/blockchain-runtime/release.json")
RELEASE_SHA = "7f948e94cec7f7aebaf2aa4dab4ddd8e92a60f083f17bb9cefcff4db89cd200f"
TIMER = "blockchain-retained-inspection-deadline.timer"
FILES = (
    "runtime/__init__.py",
    "runtime/contracts.py",
    "runtime/recovery.py",
    "runtime/recovery_metadata.py",
    "runtime/inspection_transport.py",
    "runtime/inspection.py",
)
ROOT = Path(__file__).resolve().parents[1]
MANIFEST = "inspection.manifest.json"
MAX_ARCHIVE = 512 * 1024


def read_regular(path, limit, private=False):
    path = Path(path)
    for parent in path.parents:
        _require(not parent.is_symlink() and parent.is_dir(), "INSPECTION_FILE_UNSAFE")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        import stat

        info = os.fstat(handle.fileno())
        _require(stat.S_ISREG(info.st_mode), "INSPECTION_FILE_UNSAFE")
        if private:
            _require(
                info.st_uid == os.geteuid() and not info.st_mode & 0o077,
                "INSPECTION_FILE_UNSAFE",
            )
        value = handle.read(limit + 1)
    _require(len(value) <= limit, "INSPECTION_FILE_TOO_LARGE")
    return value


def validate_approval(value, now):
    expected = {
        "schema_version",
        "purpose",
        "active_authorization",
        "owner_approved",
        "instance_id",
        "source_commit",
        "archive_sha256",
        "manifest_sha256",
        "run_id",
        "boot_id",
        "launch_epoch",
        "setup_due_epoch",
        "normal_stop_epoch",
        "guest_poweroff_epoch",
        "window_end_epoch",
        "maximum_inspections",
        "ec2_start_approved",
        "guest_shutdown_approved",
        "external_stop_approved",
        "container_starts",
        "secret_reads",
        "production_data_writes",
        "terraform_calls",
    }
    _require(
        type(value) is dict and set(value) == expected, "INSPECTION_APPROVAL_REQUIRED"
    )
    _require(
        type(value["schema_version"]) is int
        and value["schema_version"] == 1
        and value["purpose"] == "retained-metadata-inspection"
        and value["instance_id"] == INSTANCE,
        "INSPECTION_APPROVAL_REQUIRED",
    )
    for flag in (
        "active_authorization",
        "owner_approved",
        "ec2_start_approved",
        "guest_shutdown_approved",
        "external_stop_approved",
    ):
        _require(value[flag] is True, "INSPECTION_APPROVAL_REQUIRED")
    for field in (
        "container_starts",
        "secret_reads",
        "production_data_writes",
        "terraform_calls",
    ):
        _require(
            type(value[field]) is int and value[field] == 0,
            "INSPECTION_APPROVAL_REQUIRED",
        )
    _require(
        type(value["maximum_inspections"]) is int and value["maximum_inspections"] == 1,
        "INSPECTION_APPROVAL_REQUIRED",
    )
    for field in ("source_commit", "archive_sha256", "manifest_sha256"):
        size = 40 if field == "source_commit" else 64
        _require(
            type(value[field]) is str
            and re.fullmatch("[0-9a-f]{" + str(size) + "}", value[field]),
            "INSPECTION_APPROVAL_REQUIRED",
        )
    for field in ("run_id", "boot_id"):
        try:
            _require(
                str(UUID(value[field])) == value[field] and UUID(value[field]).int != 0,
                "INSPECTION_APPROVAL_REQUIRED",
            )
        except (ValueError, TypeError, AttributeError):
            raise RecoveryBlocked("INSPECTION_APPROVAL_REQUIRED") from None
    launch = value["launch_epoch"]
    _require(
        type(launch) in (int, float)
        and math.isfinite(launch)
        and launch > 0
        and launch == int(launch)
        and type(now) in (int, float)
        and math.isfinite(now),
        "INSPECTION_APPROVAL_REQUIRED",
    )
    for field, offset in (
        ("setup_due_epoch", 300),
        ("normal_stop_epoch", 600),
        ("guest_poweroff_epoch", 720),
        ("window_end_epoch", 900),
    ):
        _require(
            type(value[field]) in (int, float)
            and math.isfinite(value[field])
            and value[field] == launch + offset,
            "INSPECTION_APPROVAL_REQUIRED",
        )
    _require(launch <= now < value["normal_stop_epoch"], "INSPECTION_WINDOW_CLOSED")


def verify_package(root, approval):
    root = Path(root)
    private_directory(root)
    archive = read_regular(root / "inspection.tar", MAX_ARCHIVE, private=True)
    manifest_bytes = read_regular(root / MANIFEST, 16384, private=True)
    _require(
        hashlib.sha256(archive).hexdigest() == approval["archive_sha256"]
        and hashlib.sha256(manifest_bytes).hexdigest() == approval["manifest_sha256"],
        "INSPECTION_PACKAGE_CHANGED",
    )
    manifest = parse_json(manifest_bytes)
    _require(
        type(manifest.get("schema_version")) is int
        and manifest["schema_version"] == 1
        and manifest.get("owner_approved") is False
        and manifest.get("source_commit") == approval["source_commit"]
        and manifest.get("instance_id") == INSTANCE
        and set(manifest.get("file_sha256", {})) == set(FILES),
        "INSPECTION_PACKAGE_CHANGED",
    )
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
        members = tar.getmembers()
        _require(
            len(members) == len(FILES) + 1
            and {m.name for m in members} == {*FILES, MANIFEST}
            and all(m.isfile() and m.size <= 262144 for m in members),
            "INSPECTION_PACKAGE_CHANGED",
        )
        _require(
            tar.extractfile(MANIFEST).read() == manifest_bytes,
            "INSPECTION_PACKAGE_CHANGED",
        )
        for name in FILES:
            data = read_regular(root / name, 262144, private=True)
            _require(
                data == tar.extractfile(name).read()
                and hashlib.sha256(data).hexdigest() == manifest["file_sha256"][name],
                "INSPECTION_PACKAGE_CHANGED",
            )
    # Extra importable files cannot shadow reviewed modules. State is not code.
    actual = {
        p.relative_to(root).as_posix()
        for p in root.rglob("*")
        if not p.is_dir() and p.relative_to(root).parts[0] != "state"
    }
    _require(
        actual == {*FILES, MANIFEST, "inspection.tar", "authorization.json"},
        "INSPECTION_PACKAGE_CHANGED",
    )
    for path in root.rglob("*"):
        _require(not path.is_symlink(), "INSPECTION_FILE_UNSAFE")
    return manifest


def collect_candidate(source, disk_reader):
    """No broker process/watermarks: deliberately not a RecoveryState envelope."""
    disk = disk_reader()
    listings = {key: source.list(key) for key in sorted(LIST_KEYS)}
    commits = _numbers(listings["checkpoint/commits"])
    offsets = _numbers(listings["checkpoint/offsets"])
    sinks = _numbers(listings["bronze/_spark_metadata"], compact=True)
    _require(
        max(commits) == max(offsets) == max(sinks) == 13,
        "INSPECTION_EXPECTED_BATCH_CHANGED",
    )
    keys = [
        "checkpoint/metadata",
        "checkpoint/commits/" + commits[13],
        "checkpoint/offsets/" + offsets[13],
        "bronze/_spark_metadata/" + sinks[13],
    ]
    raw, total = {}, 0
    for key in keys:
        value = source.read(key)
        _require(
            type(value) is bytes and 0 < len(value) <= MAX_OBJECT,
            "INSPECTION_METADATA_INVALID",
        )
        total += len(value)
        _require(total <= MAX_TOTAL, "INSPECTION_METADATA_INVALID")
        raw[key] = value
    query = parse_json(raw[keys[0]])
    _require(
        type(query) is dict and set(query) == {"id"} and type(query["id"]) is str,
        "INSPECTION_METADATA_INVALID",
    )
    try:
        _require(
            str(UUID(query["id"])) == query["id"] and UUID(query["id"]).int != 0,
            "INSPECTION_METADATA_INVALID",
        )
    except ValueError:
        raise RecoveryBlocked("INSPECTION_METADATA_INVALID") from None
    commit, offset = _log(raw[keys[1]]), _log(raw[keys[2]])
    _require(
        len(commit) == 1
        and type(commit[0]) is dict
        and len(offset) == 2
        and type(offset[0]) is dict
        and type(offset[1]) is dict
        and set(offset[1]) == {TOPIC},
        "INSPECTION_METADATA_INVALID",
    )
    ends = offset[1][TOPIC]
    _require(
        type(ends) is dict
        and set(ends) == {"0"}
        and type(ends["0"]) is int
        and ends["0"] >= 0,
        "INSPECTION_METADATA_INVALID",
    )
    from urllib.parse import unquote, urlsplit

    expected = urlsplit(source.output)
    for row in _log(raw[keys[3]], allow_empty=True):
        _require(
            type(row) is dict
            and row.get("action") == "add"
            and type(row.get("path")) is str,
            "INSPECTION_METADATA_INVALID",
        )
        uri = urlsplit(unquote(row["path"]))
        _require(
            uri.scheme == expected.scheme
            and uri.netloc == expected.netloc
            and not uri.query
            and not uri.fragment
            and ".." not in uri.path.split("/")
            and uri.path.startswith(expected.path + "/"),
            "INSPECTION_METADATA_INVALID",
        )
    for key, value in raw.items():
        other = source.read(key)
        total += len(other)
        _require(total <= MAX_TOTAL and other == value, "INSPECTION_METADATA_CHANGED")
    _require(
        all(source.list(key) == value for key, value in listings.items())
        and disk_reader() == disk,
        "INSPECTION_METADATA_CHANGED",
    )
    _require(
        disk["cluster_id"] == CLUSTER
        and disk["partition"] == 0
        and disk["node_id"] == 1,
        "INSPECTION_CLUSTER_IDENTITY_CHANGED",
    )
    return {
        "status": "RETAINED_METADATA_CANDIDATE_UNAPPROVED",
        "topic_id": _topic_identity(disk["topic_id"]),
        "cluster_id": disk["cluster_id"],
        "query_id": query["id"],
        "checkpoint_batch": 13,
        "checkpoint_end_offsets": ends,
        "kafka_begin_offsets": None,
        "kafka_end_offsets": None,
        "broker_watermarks_verified": False,
        "historical_identity_confirmed": False,
        "baseline_approved": False,
        "resume_authorized": False,
        "metadata_sha256": {
            key: hashlib.sha256(value).hexdigest() for key, value in raw.items()
        },
        "disk_metadata_sha256": disk["metadata_sha256"],
        "stable_read_verified": True,
    }


class Inspector:
    def __init__(self, root=ROOT):
        self.root = Path(root)
        self.approval_bytes = read_regular(
            self.root / "authorization.json", 16384, private=True
        )
        self.approval = parse_json(self.approval_bytes)
        validate_approval(self.approval, time.time())
        verify_package(self.root, self.approval)
        self.state = self.root / "state"
        self.env = {
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "HOME": "/root",
            "LC_ALL": "C.UTF-8",
        }

    def call(self, args, allowed=(0,)):
        _require(
            args[0]
            in {
                "/usr/bin/findmnt",
                "/usr/bin/lsblk",
                "/usr/bin/systemctl",
                "/usr/bin/systemd-run",
                "/usr/bin/docker",
                "/usr/bin/date",
            },
            "INSPECTION_OPERATION_FORBIDDEN",
        )
        try:
            with (
                tempfile.TemporaryFile(dir="/run") as stdout,
                tempfile.TemporaryFile(dir="/run") as stderr,
            ):
                result = subprocess.run(
                    args,
                    env=self.env,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    timeout=15,
                    check=False,
                    preexec_fn=child_limits,
                )
                _require(result.returncode in allowed, "INSPECTION_HOST_CHECK_FAILED")
                stdout.seek(0)
                value = stdout.read(16385)
                _require(len(value) <= 16384, "INSPECTION_HOST_CHECK_FAILED")
            return value.decode().strip()
        except RecoveryBlocked:
            raise
        except Exception:
            raise RecoveryBlocked("INSPECTION_HOST_CHECK_FAILED") from None

    def fresh(self, timer=True):
        validate_approval(self.approval, time.time())
        _require(
            read_regular(self.root / "authorization.json", 16384, private=True)
            == self.approval_bytes,
            "INSPECTION_APPROVAL_CHANGED",
        )
        _require(
            Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            == self.approval["boot_id"],
            "INSPECTION_BOOT_CHANGED",
        )
        if timer:
            _require(
                self.call(
                    [
                        "/usr/bin/systemctl",
                        "show",
                        TIMER,
                        "--property=ActiveState",
                        "--value",
                    ]
                )
                == "active",
                "INSPECTION_TIMER_REQUIRED",
            )
            stamp = self.call(
                [
                    "/usr/bin/systemctl",
                    "show",
                    TIMER,
                    "--property=NextElapseUSecRealtime",
                    "--value",
                ]
            )
            actual = int(self.call(["/usr/bin/date", "-u", "--date=" + stamp, "+%s"]))
            _require(
                actual == self.approval["guest_poweroff_epoch"],
                "INSPECTION_TIMER_CHANGED",
            )

    def host(self, timer=True):
        _require(os.geteuid() == 0, "INSPECTION_ROOT_REQUIRED")
        self.fresh(timer=timer)
        _require(
            len(Path("/proc/swaps").read_text().splitlines()) == 1,
            "INSPECTION_SWAP_ENABLED",
        )
        _require(
            self.call(["/usr/bin/findmnt", "-n", "-o", "FSTYPE", "-T", "/run"])
            == "tmpfs",
            "INSPECTION_TMPFS_REQUIRED",
        )
        boot = int(
            next(
                line.split()[1]
                for line in Path("/proc/stat").read_text().splitlines()
                if line.startswith("btime ")
            )
        )
        _require(
            self.approval["launch_epoch"] - 5
            <= boot
            <= self.approval["launch_epoch"] + 300,
            "INSPECTION_BOOT_CHANGED",
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        base = "http://169.254.169.254/latest/"
        req = urllib.request.Request(
            base + "api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        )
        with opener.open(req, timeout=3) as response:
            token = response.read(2048).decode()
        req = urllib.request.Request(
            base + "meta-data/instance-id", headers={"X-aws-ec2-metadata-token": token}
        )
        with opener.open(req, timeout=3) as response:
            _require(response.read(128).decode() == INSTANCE, "INSPECTION_HOST_CHANGED")
        mount = parse_json(
            self.call(
                [
                    "/usr/bin/findmnt",
                    "--json",
                    "--mountpoint",
                    str(MOUNT),
                    "--output",
                    "TARGET,SOURCE,FSTYPE,UUID",
                ]
            )
        )["filesystems"][0]
        _require(
            mount["target"] == str(MOUNT)
            and mount["fstype"] == "xfs"
            and mount["uuid"] == FILESYSTEM
            and re.fullmatch(r"/dev/nvme[0-9]+n[0-9]+", mount["source"]),
            "INSPECTION_VOLUME_CHANGED",
        )
        serial = self.call(["/usr/bin/lsblk", "-ndo", "SERIAL", mount["source"]])
        _require(
            serial.replace("-", "") == VOLUME.replace("-", ""),
            "INSPECTION_VOLUME_CHANGED",
        )
        _require(
            hashlib.sha256(read_regular(RELEASE, 16384)).hexdigest() == RELEASE_SHA,
            "INSPECTION_RELEASE_CHANGED",
        )
        for unit in (
            "blockchain-pilot.service",
            "blockchain-pilot-reader-candidate.service",
        ):
            _require(
                self.call(["/usr/bin/systemctl", "is-active", unit], allowed=(0, 3, 4))
                in {"inactive", "failed", "unknown"},
                "INSPECTION_APPLICATION_ACTIVE",
            )
        _require(
            self.call(["/usr/bin/docker", "ps", "--quiet"]) == "",
            "INSPECTION_CONTAINER_ACTIVE",
        )

    def controls(self):
        self.host(timer=False)
        _require(
            time.time() < self.approval["setup_due_epoch"], "INSPECTION_SETUP_CLOSED"
        )
        self.state.mkdir(mode=0o700)
        private_directory(self.state)
        # Exclusive intent precedes the only timer mutation; never retry/rearm it.
        self.exclusive("controls.intent.json", {"boot_id": self.approval["boot_id"]})
        stamp = datetime.fromtimestamp(
            self.approval["guest_poweroff_epoch"], timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        self.call(
            [
                "/usr/bin/systemd-run",
                "--unit=" + TIMER.removesuffix(".timer"),
                "--on-calendar=" + stamp,
                "--timer-property=AccuracySec=1s",
                "--timer-property=Persistent=false",
                "/usr/bin/systemctl",
                "--no-block",
                "poweroff",
            ]
        )
        self.fresh()
        self.exclusive(
            "controls.json",
            {
                "boot_id": self.approval["boot_id"],
                "guest_poweroff_epoch": self.approval["guest_poweroff_epoch"],
            },
        )
        return {
            "ok": True,
            "controls_verified": True,
            "guest_poweroff_epoch": self.approval["guest_poweroff_epoch"],
        }

    def exclusive(self, name, value):
        fd = os.open(
            self.state / name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        with os.fdopen(fd, "wb") as handle:
            handle.write(canonical(value) + b"\n")

    def inspect(self):
        self.host()
        with self.host_lock():
            return self._inspect()

    @contextmanager
    def host_lock(self):
        run = Path("/run/blockchain-runtime")
        if not os.path.lexists(run):
            run.mkdir(mode=0o755)
        _require(
            not run.is_symlink()
            and run.is_dir()
            and run.stat().st_uid == 0
            and not run.stat().st_mode & 0o022,
            "INSPECTION_LOCK_UNSAFE",
        )
        fd = os.open(run / "host.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "a") as handle:
            import stat

            info = os.fstat(handle.fileno())
            _require(
                stat.S_ISREG(info.st_mode)
                and info.st_uid == 0
                and info.st_nlink == 1
                and not info.st_mode & 0o022,
                "INSPECTION_LOCK_UNSAFE",
            )
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RecoveryBlocked("INSPECTION_WRITER_ACTIVE") from None
            yield

    def _inspect(self):
        self.host()
        private_directory(self.state)
        controls = parse_json(
            read_regular(self.state / "controls.json", 4096, private=True)
        )
        _require(
            controls
            == {
                "boot_id": self.approval["boot_id"],
                "guest_poweroff_epoch": self.approval["guest_poweroff_epoch"],
            },
            "INSPECTION_TIMER_REQUIRED",
        )
        self.exclusive("inspection.intent.json", {"run_id": self.approval["run_id"]})
        aws = next(
            (
                path
                for path in ("/usr/bin/aws", "/usr/local/bin/aws")
                if Path(path).exists()
            ),
            None,
        )
        _require(aws is not None, "INSPECTION_AWS_CLI_REQUIRED")
        client = AwsCliMetadataClient(
            self.state, binary=safe_binary(aws), guard=self.fresh
        )
        identity = client.identity()
        candidate = collect_candidate(
            S3MetadataSource(client),
            lambda: retained_disk_identity(MOUNT / "data", CLUSTER),
        )
        self.host()
        result = {
            "ok": True,
            "inspection_completed": True,
            "candidate": candidate,
            "identity": identity,
            "object_versions": client.observations,
            "container_starts": 0,
            "secret_reads": 0,
            "baseline_approved": False,
        }
        self.exclusive("inspection.result.json", result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["controls", "inspect"])
    parser.add_argument(
        "--execute-on-approved-host", action="store_true", required=True
    )
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    try:
        _require(os.geteuid() == 0, "INSPECTION_ROOT_REQUIRED")
        inspector = Inspector()
        print(json.dumps(getattr(inspector, args.action)(), sort_keys=True), flush=True)
    except Exception as error:
        # No provider/CLI/path/payload text or traceback can cross SSM.
        print(
            json.dumps(
                {
                    "ok": False,
                    "status": "INSPECTION_BLOCKED",
                    "phase": args.action,
                    "reason": str(error)
                    if isinstance(error, RecoveryBlocked) and str(error) in SAFE_REASONS
                    else "INSPECTION_BLOCKED",
                }
            ),
            flush=True,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
