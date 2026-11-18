"""Guest lifecycle contracts with fake commands and temporary directories only."""

import json
import hashlib
import io
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from runtime import pilot_operator as op
from runtime.contracts import PILOT_PREFIX, REGISTRY


@pytest.fixture
def operator(tmp_path, monkeypatch):
    release = {
        "instance_id": op.d.INSTANCE,
        "kafka_volume_id": "vol-" + "2" * 17,
        "kafka_filesystem_uuid": "12345678-1234-1234-1234-123456789abc",
        "cluster_id": "MkU3OEVBNTcwNTJENDM2Qk",
        "producer_image": REGISTRY + "/blockchain-runtime-producer@sha256:" + "a" * 64,
        "spark_image": REGISTRY + "/blockchain-runtime-spark@sha256:" + "b" * 64,
        "kafka_image": REGISTRY + "/blockchain-runtime-producer@sha256:" + "c" * 64,
        "secret_arn": "arn:aws:secretsmanager:us-west-2:209479274979:secret:pilot/test-ABC123",
        "pilot_prefix": PILOT_PREFIX,
        "max_runtime_seconds": 3600,
    }
    root = tmp_path / "operator"
    root.mkdir()
    (root / "release.json").write_text(json.dumps(release))
    run = tmp_path / "run"
    run.mkdir()
    monkeypatch.setattr(op, "RUN", run)
    monkeypatch.setattr(op.time, "time", lambda: 1795043100.0)
    instance = op.Operator(1795043000, root)
    instance.state.mkdir()
    rows = [
        {
            "role": role,
            "quarantined_digest_reference": release[role + "_image"],
            "config_digest": "sha256:" + str(index) * 64,
        }
        for index, role in enumerate(("producer", "spark", "kafka"))
    ]
    acceptance = {
        "accepted": True,
        "maximum_runs": 1,
        "streaming_runs_performed": 0,
        "expires_at_utc": "2030-01-01T00:00:00Z",
        "maximum_streaming_duration_seconds": 3600,
        "artifact_scope": rows,
    }
    (root / "security-acceptance.json").write_text(json.dumps(acceptance))
    instance.calls = []
    instance.guards = []
    monkeypatch.setattr(instance, "guard", lambda **kw: instance.guards.append(kw))
    monkeypatch.setattr(op.d, "verify_install", lambda: {})
    monkeypatch.setattr(op.d, "verify_unit", lambda: None)

    def call(args, **kwargs):
        instance.calls.append((args, kwargs))
        if args[:3] == ["aws", "s3api", "list-objects-v2"]:
            return '{"KeyCount":0}'
        if args[:3] == ["aws", "ecr", "get-login-password"]:
            return "dummy-password"
        if args[:2] == ["docker", "login"]:
            (run / "docker/config.json").write_text('{"auths":{"fixture":"dummy"}}')
        if args[:3] == ["docker", "image", "inspect"]:
            role = next(
                r for r in rows if r["quarantined_digest_reference"] == args[-1]
            )
            return json.dumps(
                {
                    "Architecture": "amd64",
                    "Id": role["config_digest"],
                    "RepoDigests": [args[-1]],
                }
            )
        return ""

    monkeypatch.setattr(instance, "call", call)
    monkeypatch.setattr(instance, "chain_gate", lambda: None)
    return instance


def test_prepare_has_no_application_start_and_removes_temporary_registry_auth(operator):
    result = operator.prepare()
    assert result["chain_id"] == 42161
    assert (operator.state / "prepared.json").exists()
    assert not (op.RUN / "docker/config.json").exists()
    assert not any(args[:2] == ["systemctl", "start"] for args, _ in operator.calls)
    assert not any("get-secret-value" in args for args, _ in operator.calls)


def test_failed_chain_never_creates_launch_receipt_and_cannot_retry(
    operator, monkeypatch
):
    def fail():
        raise op.ChainGateFailure(SimpleNamespace(stage="RPC_CONNECT", code="HTTP_401"))

    monkeypatch.setattr(operator, "chain_gate", fail)
    with pytest.raises(op.ChainGateFailure):
        operator.prepare()
    assert not (operator.state / "prepared.json").exists()
    assert not (op.RUN / "docker/config.json").exists()
    with pytest.raises(FileExistsError):
        operator.prepare()
    with pytest.raises(FileNotFoundError):
        operator.launch()
    assert not any(args[:2] == ["systemctl", "start"] for args, _ in operator.calls)


def test_nonempty_prefix_fails_before_registry_or_chain_calls(operator, monkeypatch):
    monkeypatch.setattr(
        operator,
        "call",
        lambda args, **kw: '{"KeyCount":1,"Contents":[{}]}' if args[0] == "aws" else "",
    )
    monkeypatch.setattr(
        operator,
        "chain_gate",
        lambda: pytest.fail("chain called after nonempty prefix"),
    )
    with pytest.raises(RuntimeError, match="PILOT_PREFIX_NOT_EMPTY"):
        operator.prepare()
    assert not (operator.state / "prepared.json").exists()


def test_image_mismatch_never_reaches_chain(operator, monkeypatch):
    original = operator.call

    def changed(args, **kw):
        if args[:3] == ["docker", "image", "inspect"]:
            return '{"Architecture":"arm64","Id":"wrong","RepoDigests":[]}'
        return original(args, **kw)

    monkeypatch.setattr(operator, "call", changed)
    monkeypatch.setattr(
        operator, "chain_gate", lambda: pytest.fail("bad image reached chain")
    )
    with pytest.raises(RuntimeError, match="IMAGE_CONFIG_CHANGED"):
        operator.prepare()
    assert not (op.RUN / "docker/config.json").exists()


def test_launch_requests_only_versioned_unit_once(operator):
    op.exclusive(
        operator.state / "prepared.json",
        {
            "private_chain_check_passed": True,
            "launch_epoch_seconds": operator.launch_epoch,
        },
    )
    result = operator.launch()
    assert result["controller_start_requested"] is True
    assert result["controller_start_confirmed"] is False
    with pytest.raises(FileExistsError):
        operator.launch()
    starts = [args for args, _ in operator.calls if args[:2] == ["systemctl", "start"]]
    assert starts == [["systemctl", "start", "--no-block", op.d.UNIT]]


def test_prepared_receipt_from_another_boot_cannot_start(operator):
    op.exclusive(
        operator.state / "prepared.json",
        {
            "private_chain_check_passed": True,
            "launch_epoch_seconds": operator.launch_epoch + 1,
        },
    )
    with pytest.raises(RuntimeError, match="CHAIN_GATE_REQUIRED"):
        operator.launch()
    assert not (operator.state / "controller.intent.json").exists()


def test_setup_timeout_blocks_before_subprocess(operator, monkeypatch):
    monkeypatch.setattr(op.time, "time", lambda: 1795044001)
    monkeypatch.setattr(
        op.subprocess, "run", lambda *a, **kw: pytest.fail("late command executed")
    )
    with pytest.raises(RuntimeError, match="SETUP_WINDOW_CLOSED"):
        op.Operator.call(operator, ["aws", "anything"], setup=True)


def test_chain_failure_returns_only_allowlisted_codes(operator, monkeypatch):
    scripts = operator.root / "scripts"
    scripts.mkdir()
    source = Path(__file__).resolve().parents[1] / "scripts/check_runtime_chain.py"
    (scripts / source.name).write_bytes(source.read_bytes())
    monkeypatch.setattr(
        op.subprocess,
        "run",
        lambda *a, **kw: SimpleNamespace(
            returncode=1,
            stdout="CHAIN_PREFLIGHT_FAILED stage=RPC_CONNECT code=HTTP_401\n",
            stderr="private dummy provider detail",
        ),
    )
    with pytest.raises(op.ChainGateFailure) as result:
        op.Operator.chain_gate(operator)
    assert result.value.stage == "RPC_CONNECT" and result.value.code == "HTTP_401"
    assert "private" not in str(result.value)


def test_stop_uses_shutdown_guard_without_launching_or_deleting(operator):
    op.exclusive(operator.state / "controller.intent.json", {"attempts": 1})
    operator.stop()
    assert operator.guards == [{"shutdown": True}]
    assert operator.calls == [(["systemctl", "stop", "--no-block", op.d.UNIT], {})]


def test_main_missing_execution_flag_never_contacts_host(monkeypatch):
    monkeypatch.setattr(
        "sys.argv", ["operator", "launch", "--launch-epoch", "1795043000"]
    )
    monkeypatch.setattr(
        op.d, "require_host", lambda: pytest.fail("host accessed without flag")
    )
    with pytest.raises(SystemExit) as error:
        op.main()
    assert error.value.code == 2


def test_application_stop_timer_is_fixed_not_extended_by_later_setup():
    launch = 1795043000
    first = op.application_stop_command(launch, launch + 10)
    assert first == op.application_stop_command(launch, launch + 899)
    assert first[-4:] == ["/usr/bin/systemctl", "stop", "--no-block", op.d.UNIT]
    with pytest.raises(RuntimeError, match="SETUP_WINDOW_CLOSED"):
        op.application_stop_command(launch, launch + 900)


def test_controls_install_both_absolute_timers_once(operator, monkeypatch):
    operator.state.rmdir()  # Empty temporary fixture, never a host path.
    scripts = operator.root / "scripts"
    scripts.mkdir()
    source = (
        Path(__file__).resolve().parents[1] / "scripts/runtime_demo_window_policy.py"
    )
    (scripts / source.name).write_bytes(source.read_bytes())
    read = Path.read_text

    def read_text(path, *args, **kwargs):
        if str(path) == "/proc/sys/kernel/random/boot_id":
            return "synthetic-boot"
        return read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    monkeypatch.setattr(op.host, "preflight", lambda release: None)
    original = operator.call

    def call(args, **kwargs):
        value = original(args, **kwargs)
        if args[:2] == ["systemctl", "show"]:
            return (
                "app"
                if args[2] == "blockchain-pilot-application-stop.timer"
                else "guest"
            )
        if args[0] == "/usr/bin/date":
            return str(operator.launch_epoch + (1200 if "--date=app" in args else 2100))
        return value

    monkeypatch.setattr(operator, "call", call)
    result = operator.controls()
    assert result["application_stop_epoch"] == operator.launch_epoch + 1200
    assert result["guest_shutdown_epoch"] == operator.launch_epoch + 2100
    with pytest.raises(FileExistsError):
        operator.controls()
    timer_commands = [
        args for args, _ in operator.calls if args[0] == "/usr/bin/systemd-run"
    ]
    assert len(timer_commands) == 2
    assert all("--timer-property=Persistent=false" in args for args in timer_commands)


def test_legacy_preservation_gate_compares_sources_and_units(
    operator, tmp_path, monkeypatch
):
    legacy, units = tmp_path / "legacy", tmp_path / "units"
    legacy.mkdir()
    units.mkdir()
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        for name in op.bundle.FILES:
            body = ("preserved fixture " + name).encode()
            member = tarfile.TarInfo(name)
            member.size = len(body)
            archive.addfile(member, io.BytesIO(body))
            path = legacy / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
    (operator.root / "legacy-host.tar").write_bytes(stream.getvalue())
    release = (operator.root / "release.json").read_bytes()
    (legacy / "release.json").write_bytes(release)
    (legacy / "check_runtime_chain.py").write_bytes(b"legacy helper")
    monkeypatch.setattr(op.d, "LEGACY", legacy)
    monkeypatch.setattr(op.d, "RELEASE_SHA", hashlib.sha256(release).hexdigest())
    monkeypatch.setattr(
        op, "LEGACY_CHAIN_SHA", hashlib.sha256(b"legacy helper").hexdigest()
    )
    monkeypatch.setattr(op, "LEGACY_UNITS", units)
    for name in (
        "blockchain-pilot.service",
        "blockchain-pilot-deadline.timer",
        "blockchain-pilot-poweroff.service",
    ):
        (units / name).write_bytes((legacy / "deploy/runtime" / name).read_bytes())
    before = {str(p): p.read_bytes() for p in legacy.rglob("*") if p.is_file()}
    operator.legacy()
    assert before == {str(p): p.read_bytes() for p in legacy.rglob("*") if p.is_file()}
    (legacy / "runtime/host.py").write_text("tampered")
    with pytest.raises(RuntimeError, match="LEGACY_SOURCE_CHANGED"):
        operator.legacy()
