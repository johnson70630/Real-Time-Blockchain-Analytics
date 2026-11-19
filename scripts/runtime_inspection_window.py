"""New inspection-only authorization/window guard. Never starts EC2 or images."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import UUID

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from runtime.recovery import _require  # noqa: E402
from runtime.recovery_metadata import parse_json  # noqa: E402
from runtime.inspection import INSTANCE, FILES  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "existing_readonly_guards", Path(__file__).with_name("runtime_pilot_window.py")
)
old = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old)
MARKER = Path("/tmp/blockchain-inspection-execution-location")
ARTIFACTS = {
    "inspection.tar",
    "proposal.json",
    "upload-command.template",
    "controls-command.template",
    "inspect-command.template",
}
LOCAL_SOURCES = {
    "scripts/runtime_inspection_window.py",
    "scripts/runtime_inspection_relay.py",
    "scripts/prepare_runtime_inspection.py",
    "scripts/runtime_pilot_window.py",
    "scripts/runtime_demo_window_policy.py",
    "scripts/runtime_pilot_relay.py",
    *FILES,
}


def private(path, directory=False):
    _require(
        not path.is_symlink() and (path.is_dir() if directory else path.is_file()),
        "INSPECTION_PRIVATE_PATH_REQUIRED",
    )
    info = path.stat()
    _require(
        info.st_uid == os.geteuid() and not info.st_mode & 0o077,
        "INSPECTION_PRIVATE_PATH_REQUIRED",
    )


def read(path, limit=32768):
    private(path)
    with path.open("rb") as handle:
        value = handle.read(limit + 1)
    _require(len(value) <= limit, "INSPECTION_LOCAL_FILE_TOO_LARGE")
    return value


def verify(root):
    """Every failure here precedes cached-credential inspection or SDK creation."""
    private(root, directory=True)
    a = parse_json(read(root / "authorization.json"))
    _require(
        a.get("purpose") == "retained-metadata-inspection"
        and a.get("instance_id") == INSTANCE,
        "INSPECTION_SCOPE_APPROVAL_REQUIRED",
    )
    for flag in (
        "active_authorization",
        "owner_approved",
        "ec2_start_approved",
        "normal_stop_approved",
        "guest_shutdown_approved",
    ):
        _require(a.get(flag) is True, "INSPECTION_SCOPE_APPROVAL_REQUIRED")
    for key, expected in (
        ("maximum_inspections", 1),
        ("maximum_manual_starts", 1),
        ("window_minutes", 15),
        ("normal_stop_due_minutes", 10),
        ("guest_poweroff_minutes", 12),
        ("container_starts", 0),
        ("secret_reads", 0),
        ("production_data_writes", 0),
        ("terraform_calls", 0),
    ):
        _require(
            type(a.get(key)) is int and a[key] == expected,
            "INSPECTION_SCOPE_APPROVAL_REQUIRED",
        )
    try:
        run_id = a.get("run_id")
        _require(
            str(UUID(run_id)) == run_id and UUID(run_id).int != 0,
            "INSPECTION_SCOPE_APPROVAL_REQUIRED",
        )
    except (ValueError, TypeError, AttributeError):
        raise RecoveryError("INSPECTION_SCOPE_APPROVAL_REQUIRED") from None
    from runtime.bundle import _git

    _require(
        _git(REPO, "branch", "--show-current").decode().strip() == "develop"
        and _git(REPO, "rev-parse", "HEAD").decode().strip() == a["source_commit"]
        and not _git(REPO, "status", "--porcelain", "--untracked-files=no").strip(),
        "INSPECTION_SOURCE_CHANGED",
    )
    _require(
        set(a.get("approved_source_sha256", {})) == LOCAL_SOURCES,
        "INSPECTION_SOURCE_CHANGED",
    )
    for name, sha in a["approved_source_sha256"].items():
        _require(
            hashlib.sha256((REPO / name).read_bytes()).hexdigest() == sha,
            "INSPECTION_SOURCE_CHANGED",
        )
    package = Path(a["package_directory"])
    private(package, directory=True)
    _require(
        {p.name for p in package.iterdir()} == ARTIFACTS
        and set(a.get("approved_package_sha256", {})) == ARTIFACTS,
        "INSPECTION_PACKAGE_CHANGED",
    )
    for name, sha in a["approved_package_sha256"].items():
        _require(
            hashlib.sha256(read(package / name, 2 * 1024 * 1024)).hexdigest() == sha,
            "INSPECTION_PACKAGE_CHANGED",
        )
    proposal = parse_json(read(package / "proposal.json"))
    _require(
        proposal.get("source_commit") == a["source_commit"]
        and proposal.get("owner_approved") is False
        and proposal.get("archive_sha256") == a["archive_sha256"]
        and proposal.get("manifest_sha256") == a["manifest_sha256"]
        and proposal.get("purpose") == "retained-metadata-inspection",
        "INSPECTION_PACKAGE_CHANGED",
    )
    _require(
        hashlib.sha256(read(package / "inspection.tar", 512 * 1024)).hexdigest()
        == a["archive_sha256"],
        "INSPECTION_PACKAGE_CHANGED",
    )
    return a, package


def receipt(root, name, launch=None):
    value = parse_json(read(root / name))
    age = time.time() - value["checked_epoch"]
    _require(
        0 <= age < 60
        and value.get("verified") is True
        and (launch is None or value.get("launch_epoch") == launch),
        "INSPECTION_PREFLIGHT_REQUIRED",
    )
    return value


def cloud_preflight(ec2, ssm, expected):
    node = old.instance(ec2)
    _require(node["State"]["Name"] == expected, "INSPECTION_INSTANCE_STATE_CHANGED")
    _require(
        node.get("InstanceType") == "t3.xlarge"
        and node.get("MetadataOptions", {}).get("HttpTokens") == "required"
        and not node.get("KeyName"),
        "INSPECTION_INSTANCE_CONFIGURATION_CHANGED",
    )
    _require(
        node.get("IamInstanceProfile", {}).get("Arn")
        == f"arn:aws:iam::{old.ACCOUNT}:instance-profile/blockchain-runtime-ec2",
        "INSPECTION_INSTANCE_PROFILE_CHANGED",
    )
    blocks = {item["Ebs"]["VolumeId"]: item for item in node["BlockDeviceMappings"]}
    _require(
        set(blocks) == {old.ROOT_VOLUME, old.KAFKA}
        and blocks[old.KAFKA]["DeviceName"] == "/dev/sdf"
        and blocks[old.KAFKA]["Ebs"]["DeleteOnTermination"] is False,
        "INSPECTION_DISK_MAPPING_CHANGED",
    )
    volumes = ec2.describe_volumes(VolumeIds=[old.ROOT_VOLUME, old.KAFKA])["Volumes"]
    _require(
        {v["VolumeId"] for v in volumes} == set(blocks), "INSPECTION_VOLUMES_CHANGED"
    )
    for v in volumes:
        _require(
            v["Encrypted"] is True
            and v["VolumeType"] == "gp3"
            and v["AvailabilityZone"] == "us-west-2a"
            and v["Size"] == (100 if v["VolumeId"] == old.KAFKA else 30)
            and len(v["Attachments"]) == 1
            and v["Attachments"][0]["InstanceId"] == INSTANCE
            and v["Attachments"][0]["State"] == "attached",
            "INSPECTION_VOLUMES_CHANGED",
        )
    groups = node["SecurityGroups"]
    _require(len(groups) == 1, "INSPECTION_INGRESS_CHANGED")
    sg = ec2.describe_security_groups(GroupIds=[groups[0]["GroupId"]])["SecurityGroups"]
    _require(len(sg) == 1 and not sg[0]["IpPermissions"], "INSPECTION_INGRESS_CHANGED")
    credits = ec2.describe_instance_credit_specifications(InstanceIds=[INSTANCE])[
        "InstanceCreditSpecifications"
    ]
    _require(
        len(credits) == 1 and credits[0]["CpuCredits"] == "standard",
        "INSPECTION_CREDITS_CHANGED",
    )
    _require(
        ec2.describe_instance_attribute(
            InstanceId=INSTANCE, Attribute="instanceInitiatedShutdownBehavior"
        )["InstanceInitiatedShutdownBehavior"]["Value"]
        == "stop",
        "INSPECTION_SHUTDOWN_CHANGED",
    )
    old.document(ssm)
    from botocore.exceptions import ClientError

    try:
        ec2.stop_instances(
            InstanceIds=[INSTANCE],
            Hibernate=False,
            Force=False,
            SkipOsShutdown=False,
            DryRun=True,
        )
        raise RecoveryError()
    except ClientError as error:
        _require(
            error.response.get("Error", {}).get("Code") == "DryRunOperation",
            "INSPECTION_NORMAL_STOP_NOT_VERIFIED",
        )
    _require(
        old.instance(ec2)["State"]["Name"] == expected,
        "INSPECTION_INSTANCE_STATE_CHANGED",
    )
    return node


class RecoveryError(RuntimeError):
    pass


def watch(ec2, root, armed, clock=time.time, pause=time.sleep, emit=print):
    launch = None
    rejected = False
    stop_requested = False
    last_report = -100
    while True:
        now = clock()
        node = old.instance(ec2)
        state = node["State"]["Name"]
        if state in {"pending", "running", "stopping"}:
            value = node["LaunchTime"].timestamp()
            if launch is None:
                launch = value
            if launch != value:
                rejected = True
        try:
            admission = old.policy.start_admission(armed, now, launch)
        except ValueError:
            admission = "INVALID_BOOT"
        if admission in {"LATE_START", "INVALID_BOOT"}:
            rejected = True
        if rejected:
            admission = "INVALID_BOOT"
        elapsed = now - launch if launch is not None else 0
        controls = False
        control_file = root / "controls-confirmed.json"
        if control_file.exists():
            c = parse_json(read(control_file))
            controls = (
                c.get("verified") is True
                and c.get("launch_epoch") == launch
                and c.get("guest_poweroff_epoch") == launch + 720
            )
        due = admission in {"LATE_START", "INVALID_BOOT"} or (
            launch is not None and (elapsed >= 600 or (elapsed >= 300 and not controls))
        )
        if (
            parse_json(read(root / "authorization.json")).get("active_authorization")
            is not True
        ):
            due = True
        record = {
            "checked_epoch": now,
            "armed_epoch": armed,
            "start_admission": admission,
            "launch_epoch": launch,
            "state": state,
            "phase": "admitted" if admission == "ACCEPTED" else "waiting",
            "normal_stop_requested": stop_requested,
            "controls_verified": controls,
        }
        if admission == "EXPIRED":
            record["phase"] = "expired_without_start"
            old.write(root / "watch.json", record)
            consume(root, "EXPIRED_WITHOUT_START")
            return record
        if launch is not None and state == "stopped":
            record["phase"] = "completed_stopped"
            old.write(root / "watch.json", record)
            consume(root, "INSPECTION_WINDOW_STOPPED_CONSUMED")
            return record
        if due and state == "running" and not stop_requested:
            record["stop_state"] = old.normal_stop(ec2)
            stop_requested = True
        if due:
            record["phase"] = "stop_due"
        record["normal_stop_requested"] = stop_requested
        old.write(root / "watch.json", record)
        if now - last_report >= 45:
            emit(json.dumps(record), flush=True)
            last_report = now
        _require(launch is None or elapsed < 900, "INSPECTION_WINDOW_ENDED_NOT_STOPPED")
        pause(10)


def consume(root, status):
    a = parse_json(read(root / "authorization.json"))
    a.update(
        active_authorization=False,
        consumed_status=status,
        consumed_at_epoch=time.time(),
    )
    old.write(root / "authorization.json", a)


def admitted(root, ec2):
    record = parse_json(read(root / "watch.json"))
    _require(
        record.get("phase") == "admitted"
        and record.get("start_admission") == "ACCEPTED"
        and 0 <= time.time() - record["checked_epoch"] < 30,
        "INSPECTION_FRESH_WATCH_REQUIRED",
    )
    launch = record["launch_epoch"]
    node = old.instance(ec2)
    _require(
        node["State"]["Name"] == "running"
        and node["LaunchTime"].timestamp() == launch
        and launch <= time.time() < launch + 600,
        "INSPECTION_BOOT_CHANGED",
    )
    return launch


def local_dependencies():
    """Verify installed local tools before any credential or account access."""
    for binary in (
        "/opt/homebrew/bin/aws",
        "/usr/local/bin/session-manager-plugin",
    ):
        _require(
            Path(binary).is_file() and os.access(binary, os.X_OK),
            "INSPECTION_LOCAL_TOOL_REQUIRED",
        )
        result = subprocess.run(
            [binary, "--version"],
            env={"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(Path.home())},
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=10,
            check=False,
        )
        _require(
            result.returncode == 0
            and len(result.stdout) + len(result.stderr) < 8192
            and (
                b"aws-cli/2." in result.stdout + result.stderr
                if binary.endswith("/aws")
                else result.stdout.strip() == b"1.2.896.0"
            ),
            "INSPECTION_LOCAL_TOOL_CHANGED",
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["preflight", "watch", "session", "stop"])
    parser.add_argument(
        "--expected-state", choices=["stopped", "running"], default="stopped"
    )
    args = parser.parse_args()
    root = Path(MARKER.read_text().strip())
    a, _ = verify(root)
    if args.mode != "stop":
        local_dependencies()
    ec2, ssm = old.clients(2700 if args.mode in {"preflight", "watch"} else 900)
    if args.mode == "stop":
        print(json.dumps({"normal_stop_state": old.normal_stop(ec2)}))
        return
    if args.mode == "preflight":
        launch = admitted(root, ec2) if args.expected_state == "running" else None
        cloud_preflight(ec2, ssm, args.expected_state)
        old.write(
            root
            / ("running-preflight.json" if launch is not None else "preflight.json"),
            {
                "verified": True,
                "checked_epoch": time.time(),
                "launch_epoch": launch,
                "instance_state": args.expected_state,
            },
        )
        print(
            json.dumps(
                {"preflight_verified": True, "instance_state": args.expected_state}
            )
        )
        return
    if args.mode == "watch":
        _require(not (root / "watch.json").exists(), "INSPECTION_WATCH_ALREADY_ARMED")
        receipt(root, "preflight.json")
        _require(
            old.instance(ec2)["State"]["Name"] == "stopped",
            "INSPECTION_ARM_REQUIRES_STOPPED",
        )
        try:
            watch(ec2, root, time.time())
        except Exception:
            try:
                state = old.normal_stop(ec2)
                old.write(
                    root / "watch-failure-stop.json",
                    {"normal_stop_state": state, "checked_epoch": time.time()},
                )
            except Exception:
                old.write(
                    root / "watch-failure-stop.json",
                    {
                        "console_normal_stop_required": True,
                        "checked_epoch": time.time(),
                    },
                )
            raise
        return
    launch = admitted(root, ec2)
    receipt(root, "running-preflight.json", launch)
    old.document(ssm)
    command = [
        "/opt/homebrew/bin/aws",
        "ssm",
        "start-session",
        "--profile",
        old.PROFILE,
        "--region",
        old.REGION,
        "--target",
        INSTANCE,
        "--document-name",
        old.DOCUMENT,
        "--reason",
        "Approved retained metadata inspection only; no container or secret execution",
    ]
    raise SystemExit(subprocess.call(command))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(
            json.dumps(
                {
                    "ok": False,
                    "status": "INSPECTION_WINDOW_BLOCKED",
                    "details_suppressed": True,
                }
            )
        )
        raise SystemExit(1)
