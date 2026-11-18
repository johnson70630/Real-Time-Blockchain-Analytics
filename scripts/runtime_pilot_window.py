"""Reviewed operator guard for a separately approved pilot; never starts EC2."""

import argparse
import hashlib
import importlib.util as _policy_import
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tempfile
from datetime import datetime, timezone

ACCOUNT = "209479274979"
REGION = "us-west-2"
INSTANCE = "i-0ca1671a59751acc5"
KAFKA = "vol-02c9fb14a9c568fb5"
ROOT_VOLUME = "vol-03bd5d26947719f98"
PROFILE = "blockchain-demo-deploy"
ARN = f"arn:aws:sts::{ACCOUNT}:assumed-role/blockchain-runtime-deployer/blockchain-runtime-deploy"
DOCUMENT = "blockchain-runtime-demo-shell"


_policy_spec = _policy_import.spec_from_file_location(
    "short_policy", str(Path(__file__).with_name("runtime_demo_window_policy.py"))
)
policy = _policy_import.module_from_spec(_policy_spec)
_policy_spec.loader.exec_module(policy)

DOCUMENT_SHA = "a09eb89d2c2d9e879f60d89372405996c72aae040909b78607d98560db443d33"
TAGS = {
    "Project": "real-time-blockchain-analytics",
    "Environment": "production-runtime",
    "Workload": "blockchain-runtime",
}
MARKER = Path("/tmp/blockchain-reader-pilot-execution-location")


def now():
    return datetime.now(timezone.utc).isoformat()


def require(condition, code):
    if not condition:
        raise RuntimeError(code)


def write(path, data):
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as handle:
            json.dump(data, handle, indent=2, default=str)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def fresh_cache(required_seconds):
    for path in (Path.home() / ".aws/cli/cache").glob("*.json"):
        try:
            data = json.loads(path.read_text())
            if data.get("AssumedRoleUser", {}).get("Arn") == ARN:
                expiry = datetime.fromisoformat(
                    data["Credentials"]["Expiration"].replace("Z", "+00:00")
                )
                if (
                    expiry - datetime.now(timezone.utc)
                ).total_seconds() > required_seconds:
                    return
        except (OSError, ValueError, KeyError, TypeError):
            pass
    raise RuntimeError("PRIVATE_MFA_RENEWAL_REQUIRED")


def clients(required_seconds):
    fresh_cache(required_seconds)
    for key in list(os.environ):
        if key.startswith("AWS_"):
            del os.environ[key]
    os.environ.update(
        AWS_EC2_METADATA_DISABLED="true", AWS_PAGER="", AWS_DEFAULT_REGION=REGION
    )
    logging.disable(logging.CRITICAL)
    import boto3
    from botocore.config import Config

    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    config = Config(
        connect_timeout=8, read_timeout=15, retries={"total_max_attempts": 1}
    )
    identity = session.client("sts", config=config).get_caller_identity()
    require(
        identity.get("Account") == ACCOUNT and identity.get("Arn") == ARN,
        "IDENTITY_MISMATCH",
    )
    return session.client("ec2", config=config), session.client("ssm", config=config)


def instance(ec2):
    response = ec2.describe_instances(InstanceIds=[INSTANCE])
    nodes = [node for group in response["Reservations"] for node in group["Instances"]]
    require(len(nodes) == 1 and nodes[0]["InstanceId"] == INSTANCE, "INSTANCE_MISMATCH")
    node = nodes[0]
    tags = {tag["Key"]: tag["Value"] for tag in node.get("Tags", [])}
    require(all(tags.get(k) == v for k, v in TAGS.items()), "OWNERSHIP_TAG_MISMATCH")
    require(node["Placement"]["AvailabilityZone"] == "us-west-2a", "AZ_MISMATCH")
    return node


def document(ssm):
    meta = ssm.describe_document(Name=DOCUMENT, DocumentVersion="$DEFAULT")["Document"]
    require(
        meta.get("Owner") == ACCOUNT and meta.get("Status") == "Active",
        "DOCUMENT_OWNER_STATUS_MISMATCH",
    )
    require(
        meta.get("DocumentType") == "Session"
        and meta.get("TargetType") == "/AWS::EC2::Instance",
        "DOCUMENT_TYPE_MISMATCH",
    )
    require(
        meta.get("DefaultVersion") == "1" and meta.get("LatestVersion") == "1",
        "DOCUMENT_VERSION_MISMATCH",
    )
    value = json.loads(
        ssm.get_document(Name=DOCUMENT, DocumentVersion="1", DocumentFormat="JSON")[
            "Content"
        ]
    )
    digest = hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    require(digest == DOCUMENT_SHA, "DOCUMENT_CONTENT_MISMATCH")
    return digest


def normal_stop(ec2):
    # No force, hibernate, skip-OS-shutdown, detach, terminate, start or formatting.
    node = instance(ec2)
    state = node["State"]["Name"]
    if state in {"stopped", "stopping"}:
        return state
    require(state == "running", "STOP_REQUIRES_RUNNING_STATE")
    response = ec2.stop_instances(
        InstanceIds=[INSTANCE], Hibernate=False, Force=False, SkipOsShutdown=False
    )
    changes = response.get("StoppingInstances", [])
    require(
        len(changes) == 1 and changes[0].get("InstanceId") == INSTANCE,
        "STOP_RESPONSE_MISMATCH",
    )
    return changes[0]["CurrentState"]["Name"]


def stop_due(elapsed, application_invoked):
    return elapsed >= policy.STOP_DUE_SECONDS or (
        elapsed >= policy.PREFLIGHT_SECONDS and not application_invoked
    )


def fresh_receipt(window, name, field, expected_state, launch=None):
    value = json.loads((window / name).read_text())
    age = (
        datetime.now(timezone.utc) - datetime.fromisoformat(value["checked_at_utc"])
    ).total_seconds()
    require(
        value.get(field) is True
        and value.get("instance_state") == expected_state
        and 0 <= age < 300,
        "FRESH_PREFLIGHT_RECEIPT_REQUIRED",
    )
    if launch is not None:
        require(value.get("launch_epoch_seconds") == launch, "PREFLIGHT_BOOT_MISMATCH")
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["preflight", "watch", "session", "stop"])
    parser.add_argument(
        "--expected-state", choices=["stopped", "running"], default="stopped"
    )
    args = parser.parse_args()
    window = Path(MARKER.read_text().strip())
    require(
        not window.is_symlink()
        and window.is_dir()
        and window.stat().st_uid == os.geteuid()
        and (window.stat().st_mode & 0o077) == 0,
        "PRIVATE_DIRECTORY_REQUIRED",
    )
    approval = json.loads((window / "authorization.json").read_text())
    require(
        approval.get("instance_id") == INSTANCE
        and approval.get("window_minutes") == 45
        and approval.get("normal_stop_due_minutes") == 30
        and approval.get("demo_approved") is True
        and approval.get("normal_stop_approved") is True,
        "APPROVAL_SCOPE_MISMATCH",
    )
    require(
        approval.get("purpose") == "reader-compatible-pilot"
        and approval.get("keep_secret_unchanged") is True,
        "READER_APPROVAL_REQUIRED",
    )
    if args.mode != "stop":
        repo = Path(__file__).resolve().parents[1]
        require(
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=repo, text=True
            ).strip()
            == approval["source_commit"],
            "APPROVED_HEAD_CHANGED",
        )
        require(
            subprocess.check_output(
                ["git", "branch", "--show-current"], cwd=repo, text=True
            ).strip()
            == "develop",
            "DEVELOP_REQUIRED",
        )
        require(
            not subprocess.check_output(
                ["git", "status", "--porcelain", "--untracked-files=no"],
                cwd=repo,
                text=True,
            ).strip(),
            "TRACKED_WORKTREE_CHANGED",
        )
        for name, digest in approval["approved_artifact_sha256"].items():
            require(
                hashlib.sha256((repo / name).read_bytes()).hexdigest() == digest,
                "APPROVED_ARTIFACT_CHANGED",
            )
    ec2, ssm = clients(
        5400
        if args.mode in {"preflight", "watch"}
        else (1 if args.mode == "stop" else 900)
    )
    if args.mode == "preflight":
        node = instance(ec2)
        require(node["State"]["Name"] == args.expected_state, "INSTANCE_STATE_MISMATCH")
        admitted = (
            require_admitted_watch(window, ec2)
            if args.expected_state == "running"
            else None
        )
        if args.expected_state == "stopped":
            require(not node.get("PublicIpAddress"), "STOPPED_IPV4_STILL_ALLOCATED")
        require(node["InstanceType"] == "t3.xlarge", "TYPE_MISMATCH")
        require(
            node["IamInstanceProfile"]["Arn"]
            == f"arn:aws:iam::{ACCOUNT}:instance-profile/blockchain-runtime-ec2",
            "INSTANCE_PROFILE_MISMATCH",
        )
        require(
            node.get("MetadataOptions", {}).get("HttpTokens") == "required",
            "IMDSV2_REQUIRED",
        )
        require(not node.get("KeyName"), "UNEXPECTED_SSH_KEY")
        mappings = {
            item["Ebs"]["VolumeId"]: item for item in node["BlockDeviceMappings"]
        }
        require(set(mappings) == {ROOT_VOLUME, KAFKA}, "DISK_MAPPING_MISMATCH")
        require(mappings[KAFKA]["DeviceName"] == "/dev/sdf", "KAFKA_DEVICE_MISMATCH")
        require(
            mappings[KAFKA]["Ebs"].get("DeleteOnTermination") is False,
            "KAFKA_RETENTION_CHANGED",
        )
        volumes = ec2.describe_volumes(VolumeIds=[ROOT_VOLUME, KAFKA])["Volumes"]
        require(
            {v["VolumeId"] for v in volumes} == {ROOT_VOLUME, KAFKA},
            "VOLUME_IDS_MISMATCH",
        )
        for volume in volumes:
            require(
                volume["Encrypted"] and volume["VolumeType"] == "gp3",
                "VOLUME_ENCRYPTION_TYPE_MISMATCH",
            )
            require(
                volume["Size"] == (100 if volume["VolumeId"] == KAFKA else 30),
                "VOLUME_SIZE_MISMATCH",
            )
            require(volume["AvailabilityZone"] == "us-west-2a", "VOLUME_AZ_MISMATCH")
            attachments = volume["Attachments"]
            require(
                len(attachments) == 1
                and attachments[0]["InstanceId"] == INSTANCE
                and attachments[0]["State"] == "attached",
                "VOLUME_ATTACHMENT_MISMATCH",
            )
        groups = node["SecurityGroups"]
        require(len(groups) == 1, "SECURITY_GROUP_COUNT_MISMATCH")
        sg = ec2.describe_security_groups(GroupIds=[groups[0]["GroupId"]])[
            "SecurityGroups"
        ]
        require(len(sg) == 1 and not sg[0]["IpPermissions"], "UNEXPECTED_INGRESS")
        credits = ec2.describe_instance_credit_specifications(InstanceIds=[INSTANCE])[
            "InstanceCreditSpecifications"
        ]
        require(
            len(credits) == 1 and credits[0]["CpuCredits"] == "standard",
            "CPU_CREDITS_MISMATCH",
        )
        digest = document(ssm)
        require(
            ec2.describe_instance_attribute(
                InstanceId=INSTANCE, Attribute="instanceInitiatedShutdownBehavior"
            )["InstanceInitiatedShutdownBehavior"]["Value"]
            == "stop",
            "SHUTDOWN_BEHAVIOR_MISMATCH",
        )
        from botocore.exceptions import ClientError

        try:
            ec2.stop_instances(
                InstanceIds=[INSTANCE],
                Hibernate=False,
                Force=False,
                SkipOsShutdown=False,
                DryRun=True,
            )
            raise RuntimeError("UNEXPECTED_DRY_RUN_SUCCESS")
        except ClientError as error:
            require(
                error.response.get("Error", {}).get("Code") == "DryRunOperation",
                "NORMAL_STOP_AUTHORIZATION_NOT_VERIFIED",
            )
        result = {
            "checked_at_utc": now(),
            "preflight_passed": True,
            "instance_state": args.expected_state,
            "instance_id": INSTANCE,
            "encrypted_gp3_disks_match": True,
            "no_ingress": True,
            "standard_cpu_credits": True,
            "normal_stop_dry_run_authorized": True,
            "document_canonical_sha256": digest,
            "aws_mutations_performed": False,
        }
        if admitted is not None:
            require_admitted_watch(window, ec2)
            result["launch_epoch_seconds"] = admitted["launch_epoch_seconds"]
        else:
            require(
                instance(ec2)["State"]["Name"] == "stopped", "INSTANCE_STATE_CHANGED"
            )
        write(
            window
            / ("preflight.json" if admitted is None else "running-preflight.json"),
            result,
        )
        print(json.dumps(result), flush=True)
        return
    if args.mode == "stop":
        print(
            json.dumps(
                {"normal_stop_state": normal_stop(ec2), "checked_at_utc": now()}
            ),
            flush=True,
        )
        return
    if args.mode == "session":
        admitted = require_admitted_watch(window, ec2)
        fresh_receipt(
            window,
            "running-preflight.json",
            "preflight_passed",
            "running",
            admitted["launch_epoch_seconds"],
        )
        fresh_receipt(
            window,
            "running-cloud-readiness.json",
            "all_cloud_readiness_checks_passed",
            "running",
            admitted["launch_epoch_seconds"],
        )
        document(ssm)
        command = [
            "/opt/homebrew/bin/aws",
            "ssm",
            "start-session",
            "--profile",
            PROFILE,
            "--region",
            REGION,
            "--target",
            INSTANCE,
            "--document-name",
            DOCUMENT,
            "--reason",
            "Approved 11.6C exact one-run streaming demo",
        ]
        raise SystemExit(subprocess.call(command))
    existing = window / "watch.json"
    require(not existing.exists(), "WATCH_ALREADY_ARMED")
    fresh_receipt(window, "preflight.json", "preflight_passed", "stopped")
    fresh_receipt(
        window, "cloud-readiness.json", "all_cloud_readiness_checks_passed", "stopped"
    )
    require(
        instance(ec2)["State"]["Name"] == "stopped", "ARM_REQUIRES_STOPPED_INSTANCE"
    )
    require(
        subprocess.check_output(
            ["/usr/local/bin/session-manager-plugin", "--version"], text=True
        ).strip()
        == "1.2.896.0",
        "PLUGIN_VERSION_MISMATCH",
    )
    watch(ec2, window, time.time())


def require_admitted_watch(window, ec2, before_preflight=True):
    record = json.loads((window / "watch.json").read_text())
    epoch = time.time()
    require(
        record["phase"] == "armed" and record.get("start_admission") == "ACCEPTED",
        "WATCH_NOT_ADMITTED",
    )
    require(
        0 <= epoch - record["heartbeat_epoch_seconds"] < 30, "WATCH_HEARTBEAT_STALE"
    )
    launch = record["launch_epoch_seconds"]
    require(
        policy.start_admission(record["armed_epoch_seconds"], epoch, launch)
        == "ACCEPTED",
        "START_CUTOFF_MISSED",
    )
    require(
        0
        <= epoch - launch
        < (policy.PREFLIGHT_SECONDS if before_preflight else policy.STOP_DUE_SECONDS),
        "SESSION_WINDOW_TOO_LATE",
    )
    node = instance(ec2)
    require(node["State"]["Name"] == "running", "INSTANCE_NOT_RUNNING")
    require(node["LaunchTime"].timestamp() == launch, "BOOT_CHANGED")
    return record


def watch(ec2, window, armed_epoch, clock=time.time, pause=time.sleep, emit=print):
    """Only accepted current boots can be handed to SSM/application tooling.

    Late boots are never marked armed, and receive a normal stop as soon as
    running. A pending late boot stays supervised while AWS finishes starting.
    """
    launch_epoch = None
    last_report = -100
    stop_requested = False
    existing = window / "watch.json"
    while True:
        epoch = clock()
        node = instance(ec2)
        state = node["State"]["Name"]
        if state in {"pending", "running", "stopping"}:
            observed = node["LaunchTime"].timestamp()
            if launch_epoch is None:
                launch_epoch = observed
            require(observed == launch_epoch, "BOOT_CHANGED_DURING_WATCH")
        try:
            admission = policy.start_admission(armed_epoch, epoch, launch_epoch)
        except ValueError:
            admission = "INVALID_BOOT"
        elapsed = max(0, epoch - launch_epoch) if launch_epoch is not None else 0
        record = {
            "phase": "armed" if admission == "ACCEPTED" else "waiting_start",
            "armed_epoch_seconds": armed_epoch,
            "start_cutoff_epoch_seconds": armed_epoch + policy.START_HANDOFF_SECONDS,
            "start_admission": admission,
            "heartbeat_epoch_seconds": epoch,
            "elapsed_seconds": int(elapsed),
            "state": state,
            "observed_start": launch_epoch is not None,
            "normal_stop_requested": stop_requested,
            "launch_epoch_seconds": launch_epoch,
        }
        if launch_epoch is not None:
            record.update(policy.deadlines(launch_epoch))
        if admission == "EXPIRED":
            record["phase"] = "expired_without_start"
            write(existing, record)
            emit(json.dumps(record), flush=True)
            return record
        if launch_epoch is not None and state == "stopped":
            record["phase"] = "completed_stopped"
            record["completed_at_utc"] = now()
            write(existing, record)
            emit(json.dumps(record), flush=True)
            return record
        app = window / "application-intent.json"
        application_invoked = False
        if app.is_file():
            value = json.loads(app.read_text())
            application_invoked = (
                value.get("controller_invocations") == 1
                and value.get("controller_start_confirmed") is True
                and value.get("launch_epoch_seconds") == launch_epoch
            )
        deadline_due = launch_epoch is not None and stop_due(
            elapsed, application_invoked
        )
        must_stop = admission in {"LATE_START", "INVALID_BOOT"} or deadline_due
        if must_stop and state == "running" and not stop_requested:
            record["stop_api_state"] = normal_stop(ec2)
            stop_requested = True
        if stop_requested or must_stop:
            record["phase"] = "stop_due"
        record["normal_stop_requested"] = stop_requested
        write(existing, record)
        if epoch - last_report >= 45:
            emit(json.dumps(record), flush=True)
            last_report = epoch
        if launch_epoch is not None and elapsed >= policy.WINDOW_SECONDS:
            emit(
                json.dumps(
                    {
                        "STOP": "WINDOW_ENDED_INSTANCE_NOT_STOPPED",
                        "state": state,
                        "manual_console_normal_stop_required": True,
                    }
                ),
                flush=True,
            )
            raise RuntimeError("WINDOW_ENDED_INSTANCE_NOT_STOPPED")
        pause(10)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        try:
            failure_root = Path(MARKER.read_text().strip())
            write(
                failure_root / "watch-failure.json",
                {
                    "checked_at_utc": now(),
                    "category": type(error).__name__,
                    "console_normal_stop_required": True,
                },
            )
        except Exception:
            pass
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        safe_code = re.sub(r"[^A-Za-z0-9_.-]", "", str(code or type(error).__name__))[
            :100
        ]
        print(
            json.dumps(
                {
                    "STOP": "OPERATOR_REVIEW_REQUIRED",
                    "category": safe_code,
                    "guard": str(error) if type(error) is RuntimeError else None,
                    "details_suppressed": True,
                }
            ),
            flush=True,
        )
        sys.exit(1)
