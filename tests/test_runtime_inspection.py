"""Inspection-only package, disk and S3 transport contracts; no real AWS calls."""

import ast
from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shlex
import subprocess
import tarfile
from types import SimpleNamespace
from uuid import uuid4

import pytest

from runtime import inspection as ins, inspection_transport as transport, bundle
from runtime.contracts import BUCKET, PILOT_PREFIX, TOPIC
from runtime.recovery import RecoveryBlocked
from runtime.recovery_metadata import canonical

ROOT = Path(__file__).resolve().parents[1]
NOW = 1795068000


def load(name):
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "scripts" / (name + ".py")
    )
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


@pytest.fixture
def approval():
    return {
        "schema_version": 1,
        "purpose": "retained-metadata-inspection",
        "active_authorization": True,
        "owner_approved": True,
        "instance_id": ins.INSTANCE,
        "source_commit": "a" * 40,
        "archive_sha256": "b" * 64,
        "manifest_sha256": "c" * 64,
        "run_id": str(uuid4()),
        "boot_id": str(uuid4()),
        "launch_epoch": NOW - 10,
        "setup_due_epoch": NOW + 290,
        "normal_stop_epoch": NOW + 590,
        "guest_poweroff_epoch": NOW + 710,
        "window_end_epoch": NOW + 890,
        "maximum_inspections": 1,
        "ec2_start_approved": True,
        "guest_shutdown_approved": True,
        "external_stop_approved": True,
        "container_starts": 0,
        "secret_reads": 0,
        "production_data_writes": 0,
        "terraform_calls": 0,
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_approved", False),
        ("active_authorization", False),
        ("purpose", "reader-compatible-pilot"),
        ("instance_id", "i-wrong"),
        ("maximum_inspections", True),
        ("maximum_inspections", 2),
        ("container_starts", False),
        ("container_starts", 1),
        ("secret_reads", 1),
        ("production_data_writes", 1),
        ("terraform_calls", 1),
        ("normal_stop_epoch", NOW + 999),
        ("guest_poweroff_epoch", float("nan")),
        ("run_id", "../../other"),
        ("boot_id", "00000000-0000-0000-0000-000000000000"),
    ],
)
def test_wrong_approval_never_becomes_inspection_permission(approval, field, value):
    approval[field] = value
    with pytest.raises(RecoveryBlocked):
        ins.validate_approval(approval, NOW)


def test_approval_deadlines_are_absolute(approval):
    ins.validate_approval(approval, NOW)
    with pytest.raises(RecoveryBlocked, match="INSPECTION_WINDOW_CLOSED"):
        ins.validate_approval(approval, approval["normal_stop_epoch"])


@pytest.fixture
def disk(tmp_path):
    root = tmp_path / "kafka"
    root.mkdir()
    (root / "meta.properties").write_text(
        "#generated\nversion=1\ncluster.id="
        + ins.CLUSTER
        + "\nnode.id=1\ndirectory.id=AAAAAAAAAAEAAAAAAAAAAg\n"
    )
    part = root / (TOPIC + "-0")
    part.mkdir()
    (part / "partition.metadata").write_text(
        "version: 0\ntopic_id: NH7b0fLSTnKFKjVemmxT_A"
    )
    return root


def test_real_format_disk_identity_reads_two_metadata_files_only(disk):
    (disk / "00000000000000000000.log").write_text("must not read a Kafka segment")
    result = transport.retained_disk_identity(disk, ins.CLUSTER)
    assert (
        result["cluster_id"] == ins.CLUSTER
        and result["topic_id"] == "NH7b0fLSTnKFKjVemmxT_A"
    )
    assert len(result["metadata_sha256"]) == 2


@pytest.mark.parametrize(
    "file,value,code",
    [
        (
            "meta.properties",
            "version=1\ncluster.id=changed\nnode.id=1",
            "INSPECTION_CLUSTER_IDENTITY_CHANGED",
        ),
        (
            "meta.properties",
            "version=1\ncluster.id=x\ncluster.id=y\nnode.id=1",
            "INSPECTION_DISK_METADATA_INVALID",
        ),
        (
            TOPIC + "-0/partition.metadata",
            "version: 1\ntopic_id: NH7b0fLSTnKFKjVemmxT_A",
            "INSPECTION_DISK_METADATA_INVALID",
        ),
        (
            TOPIC + "-0/partition.metadata",
            "version: 0\ntopic_id: AAAAAAAAAAAAAAAAAAAAAA",
            "KAFKA_IDENTITY_REQUIRED",
        ),
    ],
)
def test_changed_or_unknown_disk_metadata_fails_closed(disk, file, value, code):
    (disk / file).write_text(value)
    with pytest.raises(RecoveryBlocked, match=code):
        transport.retained_disk_identity(disk, ins.CLUSTER)


def test_disk_symlink_is_not_followed(disk):
    target = disk / (TOPIC + "-0/partition.metadata")
    target.unlink()
    target.symlink_to(disk / "meta.properties")
    with pytest.raises((RecoveryBlocked, OSError)):
        transport.retained_disk_identity(disk, ins.CLUSTER)


def test_cli_executes_verified_system_target_instead_of_replaceable_link(tmp_path):
    target = Path("/usr/bin/python3").resolve(strict=True)
    link = tmp_path / "aws-link"
    link.symlink_to(target)
    assert transport.safe_binary(link) == str(target)


@pytest.fixture
def client(tmp_path, monkeypatch):
    scratch = tmp_path / "scratch"
    scratch.mkdir(mode=0o700)
    c = transport.AwsCliMetadataClient(scratch)
    c.calls = []
    key = PILOT_PREFIX + "/checkpoints/swaps_bronze/metadata"
    value = b'{"id":"12345678-1234-1234-1234-123456789abc"}'
    etag = '"' + "a" * 32 + '"'

    def run(command, **kwargs):
        c.calls.append(command)
        assert kwargs["env"]["AWS_CONFIG_FILE"] == "/dev/null"
        assert kwargs["env"]["AWS_SHARED_CREDENTIALS_FILE"] == "/dev/null"
        assert kwargs["env"]["AWS_MAX_ATTEMPTS"] == "1"
        assert not any(
            k in kwargs["env"]
            for k in [
                "AWS_PROFILE",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "HTTPS_PROXY",
            ]
        )
        assert kwargs["timeout"] == 20
        operation = command[2]
        if operation == "get-caller-identity":
            reply = {"Account": transport.ACCOUNT, "Arn": transport.ROLE_ARN}
        elif operation == "head-object":
            reply = {
                "ContentLength": len(value),
                "ETag": etag,
                "LastModified": "2026-11-19T01:20:00+00:00",
                "VersionId": "opaque-version",
            }
        elif operation == "get-object":
            path = Path(command[command.index("--region") - 1])
            path.write_bytes(value)
            reply = {"ETag": etag}
        elif operation == "list-objects-v2":
            reply = {"Contents": [], "IsTruncated": False}
        else:
            pytest.fail("Unexpected AWS CLI verb")
        kwargs["stdout"].write(canonical(reply))
        kwargs["stdout"].flush()
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(transport.subprocess, "run", run)
    return c, key, value, etag


def test_cli_bridge_is_conditional_scoped_and_cleans_temporary_bodies(client):
    c, key, value, etag = client
    assert c.identity() == {"account_matches": True, "workload_role_matches": True}
    head = c.head_object(Bucket=BUCKET, Key=key)
    result = c.get_object(
        Bucket=BUCKET, Key=key, IfMatch=etag, Range=f"bytes=0-{len(value) - 1}"
    )
    assert result["Body"].read() == value and head["ETag"] == etag
    assert (
        c.observations[key]["version_id_sha256"]
        == hashlib.sha256(b"opaque-version").hexdigest()
    )
    assert not list(c.scratch.iterdir())
    assert all(
        "secretsmanager" not in cmd and "put-object" not in cmd for cmd in c.calls
    )


@pytest.mark.parametrize(
    "key",
    [
        "production/platform/terraform.tfstate",
        PILOT_PREFIX + "/bronze/swaps/part.parquet",
        PILOT_PREFIX + "/checkpoints/swaps_bronze/../secret",
    ],
)
def test_cli_bridge_blocks_nonmetadata_before_subprocess(client, key):
    c, *_ = client
    with pytest.raises(RecoveryBlocked, match="INSPECTION_OPERATION_FORBIDDEN"):
        c.head_object(Bucket=BUCKET, Key=key)
    assert not c.calls


def test_cli_failure_never_echoes_provider_output(client, monkeypatch, capsys):
    c, key, *_ = client

    def failed(command, **kw):
        kw["stderr"].write(b"private-provider-response-not-for-chat")
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(transport.subprocess, "run", failed)
    with pytest.raises(RecoveryBlocked, match="^INSPECTION_AWS_CLI_FAILED$"):
        c.head_object(Bucket=BUCKET, Key=key)
    assert not capsys.readouterr().out and not list(c.scratch.iterdir())


def test_candidate_does_not_invent_broker_offsets(disk):
    topic = TOPIC

    class Source:
        output = f"s3a://{BUCKET}/{PILOT_PREFIX}/bronze/swaps"
        data = {
            "checkpoint/metadata": b'{"id":"12345678-1234-1234-1234-123456789abc"}',
            "checkpoint/commits/13": b"v1\n{}",
            "checkpoint/offsets/13": (
                "v1\n{}\n" + json.dumps({topic: {"0": 123}})
            ).encode(),
            "bronze/_spark_metadata/13.compact": b"v1\n",
        }

        def list(self, key):
            return sorted(
                k[len(key) + 1 :] for k in self.data if k.startswith(key + "/")
            )

        def read(self, key):
            return self.data[key]

    source = Source()
    result = ins.collect_candidate(
        source, lambda: transport.retained_disk_identity(disk, ins.CLUSTER)
    )
    assert (
        result["checkpoint_end_offsets"] == {"0": 123}
        and result["checkpoint_batch"] == 13
    )
    assert result["kafka_end_offsets"] is None and result["kafka_begin_offsets"] is None
    assert result["baseline_approved"] is False and result["resume_authorized"] is False
    assert result["historical_identity_confirmed"] is False
    source.data["checkpoint/commits/14"] = b"v1\n{}"
    with pytest.raises(RecoveryBlocked, match="INSPECTION_EXPECTED_BATCH_CHANGED"):
        ins.collect_candidate(
            source, lambda: transport.retained_disk_identity(disk, ins.CLUSTER)
        )


@pytest.fixture
def package(tmp_path, monkeypatch, approval):
    builder = load("prepare_runtime_inspection")

    def git(repo, *args):
        if args == ("branch", "--show-current"):
            return b"develop\n"
        if args == ("status", "--porcelain", "--untracked-files=no"):
            return b""
        if args == ("rev-parse", "HEAD"):
            return b"a" * 40 + b"\n"
        if args[0] == "ls-tree":
            return b"100644 blob " + b"b" * 40 + b"\t" + args[-1].encode() + b"\0"
        assert args[:2] == ("cat-file", "blob")
        return (ROOT / args[-1].split(":", 1)[1]).read_bytes()

    monkeypatch.setattr(bundle, "_git", git)
    folder = tmp_path / "package"
    result = builder.build(ROOT, folder)
    approval.update(
        archive_sha256=result["archive_sha256"],
        manifest_sha256=result["manifest_sha256"],
    )
    stage = tmp_path / "stage"
    stage.mkdir(mode=0o700)
    archive = (folder / "inspection.tar").read_bytes()
    (stage / "inspection.tar").write_bytes(archive)
    (stage / "inspection.tar").chmod(0o600)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
        for item in tar:
            path = stage / item.name
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            path.write_bytes(tar.extractfile(item).read())
            path.chmod(0o600)
    (stage / "authorization.json").write_bytes(canonical(approval))
    (stage / "authorization.json").chmod(0o600)
    return builder, folder, stage, result, approval


def test_inspection_package_is_reproducible_pending_and_nonsecret(package, tmp_path):
    builder, folder, stage, result, approval = package
    assert builder.build(ROOT, tmp_path / "again") == result
    assert ins.verify_package(stage, approval)["source_commit"] == "a" * 40
    proposal = json.loads((folder / "proposal.json").read_text())
    assert proposal["owner_approved"] is False and proposal["container_starts"] == 0
    upload = (folder / "upload-command.template").read_text()
    assert "sudo -n /usr/bin/env -i " in upload.splitlines()[0]
    assert "/usr/bin/python3 -I -B - " in upload.splitlines()[0]
    ast.parse("\n".join(upload.splitlines()[1:-1]), feature_version=(3, 9))
    assert max(map(len, upload.splitlines())) < 2048
    for name in ["upload", "controls", "inspect"]:
        subprocess.run(
            ["/bin/sh", "-n"],
            input=(folder / (name + "-command.template")).read_text(),
            text=True,
            check=True,
        )
    for name in ["controls", "inspect"]:
        args = shlex.split((folder / (name + "-command.template")).read_text())
        assert args[:4] == ["sudo", "-n", "/usr/bin/env", "-i"]
        assert "runtime.inspection " + name in args[-1]
    with pytest.raises(FileExistsError):
        builder.build(ROOT, folder)


def test_modified_package_is_refused_before_host_checks(package):
    _, _, stage, _, approval = package
    (stage / "runtime/inspection_transport.py").write_bytes(b"changed")
    with pytest.raises(RecoveryBlocked, match="INSPECTION_PACKAGE_CHANGED"):
        ins.verify_package(stage, approval)


def test_local_package_fingerprint_gate_accepts_only_exact_reviewed_bytes(
    package, tmp_path, monkeypatch
):
    _, folder, _, result, guest = package
    w = load("runtime_inspection_window")
    root = tmp_path / "execution"
    root.mkdir(mode=0o700)
    owner = {
        "purpose": "retained-metadata-inspection",
        "instance_id": ins.INSTANCE,
        "active_authorization": True,
        "owner_approved": True,
        "ec2_start_approved": True,
        "normal_stop_approved": True,
        "guest_shutdown_approved": True,
        "maximum_inspections": 1,
        "maximum_manual_starts": 1,
        "window_minutes": 15,
        "normal_stop_due_minutes": 10,
        "guest_poweroff_minutes": 12,
        "container_starts": 0,
        "secret_reads": 0,
        "production_data_writes": 0,
        "terraform_calls": 0,
        "source_commit": "a" * 40,
        "run_id": guest["run_id"],
        "archive_sha256": result["archive_sha256"],
        "manifest_sha256": result["manifest_sha256"],
        "package_directory": str(folder),
        "approved_source_sha256": {
            name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
            for name in w.LOCAL_SOURCES
        },
        "approved_package_sha256": {
            name: hashlib.sha256((folder / name).read_bytes()).hexdigest()
            for name in w.ARTIFACTS
        },
    }
    path = root / "authorization.json"
    path.write_bytes(canonical(owner))
    path.chmod(0o600)
    assert w.verify(root)[0]["owner_approved"] is True
    (folder / "inspect-command.template").write_text("changed")
    with pytest.raises(RecoveryBlocked, match="INSPECTION_PACKAGE_CHANGED"):
        w.verify(root)


def test_real_child_output_is_bounded_without_invoking_aws(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir(mode=0o700)
    script = tmp_path / "fake-client"
    script.write_text(
        '#!/usr/bin/python3\nimport os\nos.write(1,b"x"*(3*1024*1024))\nraise SystemExit(1)\n'
    )
    script.chmod(0o700)
    client = transport.AwsCliMetadataClient(scratch, binary=str(script))
    with pytest.raises(RecoveryBlocked, match="INSPECTION_AWS_CLI_FAILED"):
        client.identity()
    assert not list(scratch.iterdir())


def test_changed_object_version_is_refused_even_with_identical_payload(
    client, monkeypatch
):
    c, key, *_ = client
    original = c._invoke
    count = [0]

    def invoke(operation, options, output=None):
        result = original(operation, options, output)
        if operation == ("s3api", "head-object"):
            count[0] += 1
            result["VersionId"] = "first" if count[0] == 1 else "second"
        return result

    monkeypatch.setattr(c, "_invoke", invoke)
    c.head_object(Bucket=BUCKET, Key=key)
    with pytest.raises(RecoveryBlocked, match="INSPECTION_METADATA_CHANGED"):
        c.head_object(Bucket=BUCKET, Key=key)


def test_controls_install_once_and_inspect_reserves_before_aws(package, monkeypatch):
    _, _, stage, _, approval = package
    monkeypatch.setattr(ins.time, "time", lambda: NOW)
    inspector = ins.Inspector(stage)
    calls = []
    monkeypatch.setattr(inspector, "host", lambda **kw: None)
    monkeypatch.setattr(inspector, "fresh", lambda **kw: None)
    monkeypatch.setattr(inspector, "call", lambda args, **kw: calls.append(args) or "")
    assert inspector.controls()["controls_verified"] is True
    assert len(calls) == 1 and calls[0][0] == "/usr/bin/systemd-run"
    with pytest.raises(FileExistsError):
        inspector.controls()
    assert len(calls) == 1
    # Call the body only: the live lock/root/instance path is deliberately not
    # used in offline tests. Intent persists even if the first client step fails.
    monkeypatch.setattr(
        ins,
        "safe_binary",
        lambda path: (_ for _ in ()).throw(RecoveryBlocked("INSPECTION_BINARY_UNSAFE")),
    )
    monkeypatch.setattr(ins.Path, "exists", lambda path: True)
    with pytest.raises(RecoveryBlocked):
        inspector._inspect()
    assert (inspector.state / "inspection.intent.json").is_file()
    with pytest.raises(FileExistsError):
        inspector._inspect()


def test_missing_execution_flag_is_refused_without_network():
    result = subprocess.run(
        [str(ROOT / ".venv/bin/python"), "-m", "runtime.inspection", "inspect"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0 and "--execute-on-approved-host" in result.stderr


def test_local_window_refuses_old_or_missing_scope_before_sdk(tmp_path, monkeypatch):
    w = load("runtime_inspection_window")
    tmp_path.chmod(0o700)
    p = tmp_path / "authorization.json"
    p.write_text('{"purpose":"reader-compatible-pilot"}')
    p.chmod(0o600)
    monkeypatch.setattr(
        w.old, "clients", lambda *args: pytest.fail("SDK called before approval")
    )
    with pytest.raises(RecoveryBlocked, match="INSPECTION_SCOPE_APPROVAL_REQUIRED"):
        w.verify(tmp_path)


class FakeEc2:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.current = None
        self.stops = []

    def row(self):
        self.current = next(self.rows)
        state, launch = self.current
        return {
            "State": {"Name": state},
            "LaunchTime": datetime.fromtimestamp(launch, timezone.utc),
        }


@pytest.mark.parametrize(
    "case",
    [
        "no_start",
        "late_pending",
        "setup_timeout",
        "normal_deadline",
        "reboot",
        "future_boot",
        "revoked",
    ],
)
def test_inspection_watch_never_extends_or_reuses_a_window(tmp_path, monkeypatch, case):
    w = load("runtime_inspection_window")
    (tmp_path / "authorization.json").write_text('{"active_authorization":true}')
    (tmp_path / "authorization.json").chmod(0o600)
    arm = 1000
    if case == "no_start":
        times = [1000, 1331]
        rows = [("stopped", 900), ("stopped", 900)]
    elif case == "late_pending":
        times = [1305, 1310, 1320]
        rows = [("pending", 1301), ("running", 1301), ("stopped", 1301)]
    elif case == "setup_timeout":
        times = [1000, 1300, 1310]
        rows = [("running", 1000), ("running", 1000), ("stopped", 1000)]
    elif case == "normal_deadline":
        times = [1000, 1600, 1610]
        rows = [("running", 1000), ("running", 1000), ("stopped", 1000)]
        p = tmp_path / "controls-confirmed.json"
        p.write_text(
            '{"verified":true,"launch_epoch":1000,"guest_poweroff_epoch":1720}'
        )
        p.chmod(0o600)
    elif case == "reboot":
        times = [1000, 1050, 1060]
        rows = [("running", 1000), ("running", 1040), ("stopped", 1040)]
    elif case == "revoked":
        times = [1000, 1010]
        rows = [("running", 1000), ("stopped", 1000)]
        (tmp_path / "authorization.json").write_text('{"active_authorization":false}')
    else:
        times = [1000, 1020, 1030]
        rows = [("pending", 1010), ("running", 1010), ("stopped", 1010)]
    ec2 = FakeEc2(rows)
    clock = iter(times)
    monkeypatch.setattr(w.old, "instance", lambda client: client.row())
    monkeypatch.setattr(
        w.old,
        "normal_stop",
        lambda client: client.stops.append(client.current) or "stopping",
    )
    result = w.watch(
        ec2,
        tmp_path,
        arm,
        clock=lambda: next(clock),
        pause=lambda _: None,
        emit=lambda *a, **k: None,
    )
    assert result["phase"] == (
        "expired_without_start" if case == "no_start" else "completed_stopped"
    )
    assert len(ec2.stops) == (0 if case == "no_start" else 1)
    assert (
        json.loads((tmp_path / "authorization.json").read_text())[
            "active_authorization"
        ]
        is False
    )


def test_guest_approval_binds_actual_boot_without_new_app_scope(approval):
    relay = load("runtime_inspection_relay")
    owner = {
        key: approval[key]
        for key in ["source_commit", "archive_sha256", "manifest_sha256", "run_id"]
    }
    boot = {
        "ok": True,
        "boot_id": approval["boot_id"],
        "boot_epoch": NOW - 9,
        "guest_epoch": NOW,
    }
    result = relay.guest_approval(owner, NOW - 10, boot, NOW)
    ins.validate_approval(result, NOW)
    assert result["container_starts"] == 0 and result["secret_reads"] == 0
    boot["guest_epoch"] = NOW + 20
    with pytest.raises(RecoveryBlocked, match="INSPECTION_BOOT_REPLY_INVALID"):
        relay.guest_approval(owner, NOW - 10, boot, NOW)


@pytest.mark.parametrize("changed", [False, True])
def test_local_ssm_dependencies_are_checked_without_account_access(
    monkeypatch, changed
):
    w = load("runtime_inspection_window")
    calls = []
    monkeypatch.setattr(w.Path, "is_file", lambda _: True)
    monkeypatch.setattr(w.os, "access", lambda *a: True)

    def run(command, **kwargs):
        calls.append(command)
        assert command[-1] == "--version" and kwargs["timeout"] == 10
        assert not any(k.startswith("AWS_") for k in kwargs["env"])
        value = b"aws-cli/2.34.0" if command[0].endswith("/aws") else b"1.2.896.0"
        return SimpleNamespace(
            returncode=0, stdout=b"changed" if changed else value, stderr=b""
        )

    monkeypatch.setattr(w.subprocess, "run", run)
    if changed:
        with pytest.raises(RecoveryBlocked, match="INSPECTION_LOCAL_TOOL_CHANGED"):
            w.local_dependencies()
    else:
        w.local_dependencies()
        assert len(calls) == 2


@pytest.mark.parametrize("bad", [None, "ingress", "volume", "credits", "stop_denied"])
def test_exact_cloud_preflight_uses_fake_reads_and_stop_dry_run(monkeypatch, bad):
    from botocore.exceptions import ClientError

    w = load("runtime_inspection_window")
    node = {
        "State": {"Name": "stopped"},
        "InstanceType": "t3.xlarge",
        "MetadataOptions": {"HttpTokens": "required"},
        "IamInstanceProfile": {
            "Arn": "arn:aws:iam::209479274979:instance-profile/blockchain-runtime-ec2"
        },
        "SecurityGroups": [{"GroupId": "sg-synthetic"}],
        "BlockDeviceMappings": [
            {"DeviceName": "/dev/xvda", "Ebs": {"VolumeId": w.old.ROOT_VOLUME}},
            {
                "DeviceName": "/dev/sdf",
                "Ebs": {"VolumeId": w.old.KAFKA, "DeleteOnTermination": False},
            },
        ],
    }
    calls = []

    class Ec2:
        def describe_volumes(self, **kwargs):
            return {
                "Volumes": [
                    {
                        "VolumeId": v,
                        "Encrypted": bad != "volume",
                        "VolumeType": "gp3",
                        "AvailabilityZone": "us-west-2a",
                        "Size": 100 if v == w.old.KAFKA else 30,
                        "Attachments": [
                            {"InstanceId": ins.INSTANCE, "State": "attached"}
                        ],
                    }
                    for v in [w.old.ROOT_VOLUME, w.old.KAFKA]
                ]
            }

        def describe_security_groups(self, **kwargs):
            return {
                "SecurityGroups": [
                    {"IpPermissions": ["unexpected"] if bad == "ingress" else []}
                ]
            }

        def describe_instance_credit_specifications(self, **kwargs):
            return {
                "InstanceCreditSpecifications": [
                    {"CpuCredits": "unlimited" if bad == "credits" else "standard"}
                ]
            }

        def describe_instance_attribute(self, **kwargs):
            return {"InstanceInitiatedShutdownBehavior": {"Value": "stop"}}

        def stop_instances(self, **kwargs):
            calls.append(kwargs)
            assert kwargs == {
                "InstanceIds": [ins.INSTANCE],
                "Hibernate": False,
                "Force": False,
                "SkipOsShutdown": False,
                "DryRun": True,
            }
            raise ClientError(
                {
                    "Error": {
                        "Code": "UnauthorizedOperation"
                        if bad == "stop_denied"
                        else "DryRunOperation"
                    }
                },
                "StopInstances",
            )

    monkeypatch.setattr(w.old, "instance", lambda _: node)
    monkeypatch.setattr(w.old, "document", lambda _: None)
    if bad:
        with pytest.raises(RecoveryBlocked):
            w.cloud_preflight(Ec2(), object(), "stopped")
    else:
        assert w.cloud_preflight(Ec2(), object(), "stopped") == node
        assert len(calls) == 1


@pytest.mark.parametrize("failure", [None, "controls", "inspect", "session"])
def test_relay_checks_boot_and_always_normally_stops_owned_window(
    tmp_path, monkeypatch, approval, failure
):
    relay = load("runtime_inspection_relay")
    w = relay.window
    launch = NOW - 10
    package = tmp_path / "package"
    package.mkdir()
    for action in ["upload", "controls", "inspect"]:
        (package / (action + "-command.template")).write_text(
            "__INSPECTION_APPROVAL_BASE64__" if action == "upload" else action + "\n"
        )
    marker = tmp_path / "marker"
    marker.write_text(str(tmp_path))
    monkeypatch.setattr(w, "MARKER", marker)
    monkeypatch.setattr(w, "verify", lambda _: (approval, package))
    monkeypatch.setattr(w, "local_dependencies", lambda: None)
    ec2 = object()
    monkeypatch.setattr(w.old, "clients", lambda _: (ec2, object()))
    monkeypatch.setattr(w, "admitted", lambda *_: launch)
    monkeypatch.setattr(w, "receipt", lambda *_: None)
    monkeypatch.setattr(relay.sys, "argv", ["relay"])
    monkeypatch.setattr(relay.time, "time", lambda: NOW)
    monkeypatch.setattr(relay.pty, "fork", lambda: (123, 456))
    sent, stopped, closed, writes = [], [], [], {}
    monkeypatch.setattr(relay, "close_owned_session", lambda *a: closed.append(a))
    monkeypatch.setattr(w.old, "normal_stop", lambda c: stopped.append(c) or "stopping")
    monkeypatch.setattr(w.old, "write", lambda p, value: writes.update({p.name: value}))

    def until(*args):
        if failure == "session":
            raise RuntimeError("synthetic session failure")

    monkeypatch.setattr(relay.terminal, "read_until", until)
    monkeypatch.setattr(relay.terminal, "send", lambda fd, data: sent.append(data))
    candidate = {
        "status": "RETAINED_METADATA_CANDIDATE_UNAPPROVED",
        "baseline_approved": False,
        "resume_authorized": False,
        "broker_watermarks_verified": False,
        "kafka_begin_offsets": None,
        "kafka_end_offsets": None,
    }
    replies = iter(
        [
            {
                "ok": True,
                "boot_id": approval["boot_id"],
                "boot_epoch": launch,
                "guest_epoch": NOW,
            },
            {"ok": False, "reason": "provider-text-must-be-suppressed"}
            if failure == "controls"
            else {
                "ok": True,
                "controls_verified": True,
                "guest_poweroff_epoch": launch + 720,
            },
            {"ok": False, "reason": "INSPECTION_METADATA_CHANGED"}
            if failure == "inspect"
            else {
                "ok": True,
                "inspection_completed": True,
                "candidate": candidate,
                "identity": {},
                "object_versions": {},
                "container_starts": 0,
                "secret_reads": 0,
                "baseline_approved": False,
            },
        ]
    )
    monkeypatch.setattr(relay.terminal, "read_json_reply", lambda *a: next(replies))
    if failure:
        with pytest.raises((RecoveryBlocked, RuntimeError)):
            relay.main()
    else:
        relay.main()
        assert (
            writes["guest-inspect-result.json"]["candidate"]["baseline_approved"]
            is False
        )
        assert "__INSPECTION_APPROVAL_BASE64__" not in "".join(sent)
    assert stopped == [ec2] and closed == [(123, 456)]
    assert writes["relay-normal-stop.json"]["normal_stop_state"] == "stopping"
    if failure in {"controls", "inspect"}:
        assert writes["guest-" + failure + "-refusal.json"]["reason"] == (
            "INSPECTION_GUEST_REFUSED"
            if failure == "controls"
            else "INSPECTION_METADATA_CHANGED"
        )


@pytest.mark.parametrize("changed", [None, "mount", "volume", "app", "container"])
def test_guest_host_refuses_wrong_storage_or_active_application(
    monkeypatch, approval, changed
):
    inspector = ins.Inspector.__new__(ins.Inspector)
    inspector.approval = approval
    monkeypatch.setattr(inspector, "fresh", lambda **kw: None)
    monkeypatch.setattr(ins.os, "geteuid", lambda: 0)
    original = ins.Path.read_text

    def read(path, *args, **kwargs):
        if str(path) == "/proc/swaps":
            return "Filename Type Size Used Priority\n"
        if str(path) == "/proc/stat":
            return "btime " + str(approval["launch_epoch"])
        return original(path, *args, **kwargs)

    monkeypatch.setattr(ins.Path, "read_text", read)
    monkeypatch.setattr(ins, "read_regular", lambda *a, **kw: b"reviewed-release")
    monkeypatch.setattr(
        ins, "RELEASE_SHA", hashlib.sha256(b"reviewed-release").hexdigest()
    )
    replies = iter(
        [io.BytesIO(b"private-imds-token"), io.BytesIO(ins.INSTANCE.encode())]
    )
    monkeypatch.setattr(
        ins.urllib.request,
        "build_opener",
        lambda *a: SimpleNamespace(open=lambda *a, **kw: next(replies)),
    )
    calls = []

    def call(args, **kwargs):
        calls.append(args)
        if args[0].endswith("findmnt"):
            if "FSTYPE" in args:
                return "tmpfs"
            return json.dumps(
                {
                    "filesystems": [
                        {
                            "target": str(ins.MOUNT),
                            "source": "/dev/nvme1n1",
                            "fstype": "xfs",
                            "uuid": "changed" if changed == "mount" else ins.FILESYSTEM,
                        }
                    ]
                }
            )
        if args[0].endswith("lsblk"):
            return "changed" if changed == "volume" else ins.VOLUME.replace("-", "")
        if args[0].endswith("systemctl"):
            return "active" if changed == "app" else "inactive"
        assert args == ["/usr/bin/docker", "ps", "--quiet"]
        return "running-container" if changed == "container" else ""

    monkeypatch.setattr(inspector, "call", call)
    if changed:
        with pytest.raises(RecoveryBlocked):
            inspector.host()
    else:
        inspector.host()
    assert all("start" not in args and "run" not in args for args in calls)


def test_guest_timer_deadline_cannot_be_extended(package, monkeypatch):
    _, _, stage, _, approval = package
    monkeypatch.setattr(ins.time, "time", lambda: NOW)
    inspector = ins.Inspector(stage)
    original = ins.Path.read_text
    monkeypatch.setattr(
        ins.Path,
        "read_text",
        lambda p, *a, **kw: (
            approval["boot_id"]
            if str(p) == "/proc/sys/kernel/random/boot_id"
            else original(p, *a, **kw)
        ),
    )
    calls = []

    def call(args, **kwargs):
        calls.append(args)
        if "--property=ActiveState" in args:
            return "active"
        if args[0].endswith("date"):
            return str(approval["guest_poweroff_epoch"] + 60)
        return "synthetic-timer-stamp"

    monkeypatch.setattr(inspector, "call", call)
    with pytest.raises(RecoveryBlocked, match="INSPECTION_TIMER_CHANGED"):
        inspector.fresh()
    assert not any("systemd-run" in args[0] for args in calls)
