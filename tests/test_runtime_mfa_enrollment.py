"""Synthetic-only enrollment checks; no account writes, QR seeds or real codes."""

import json
import stat
import subprocess
import warnings

import pytest

from scripts import enroll_runtime_mfa as mfa


class FakeIam:
    def __init__(self, folder):
        self.folder = folder
        self.calls = []
        self.assigned = set()
        self.existing = {"arn:aws:iam::209479274979:mfa/Authapp"}
        self.created = False
        self.fail_enable = False
        self.identity = {"Account": mfa.ACCOUNT, "Arn": mfa.USER_ARN}

    def __call__(self, service, operation, *args, payload=None):
        self.calls.append((service, operation, args, payload))
        if operation == "get-caller-identity":
            return self.identity
        if operation == "list-mfa-devices":
            return sorted(self.assigned)
        if operation == "list-virtual-mfa-devices":
            status = args[args.index("--assignment-status") + 1]
            devices = self.existing | ({mfa.DEVICE_ARN} if self.created else set())
            return sorted(devices - self.assigned if status == "Unassigned" else devices)
        if operation == "create-virtual-mfa-device":
            assert args[args.index("--virtual-mfa-device-name") + 1] == mfa.DEVICE
            assert args[args.index("--outfile") + 1] == str(self.folder / "bootstrap.png")
            (self.folder / "bootstrap.png").write_bytes(b"\x89PNG\r\n\x1a\nOFFLINE_TEST")
            self.created = True
            return mfa.DEVICE_ARN
        if operation == "enable-mfa-device":
            if self.fail_enable:
                raise mfa.EnrollmentError("Enable failed; no automatic cleanup.")
            assert payload["SerialNumber"] == mfa.DEVICE_ARN
            assert payload["UserName"] == mfa.USER
            self.assigned.add(mfa.DEVICE_ARN)
            return None
        raise AssertionError("Unexpected operation")


@pytest.fixture
def fake_enrollment(monkeypatch, tmp_path):
    folder = tmp_path / "private-enrollment"
    fake = FakeIam(folder)

    def make_directory(**kwargs):
        folder.mkdir()
        return str(folder)

    monkeypatch.setattr(mfa.tempfile, "mkdtemp", make_directory)
    monkeypatch.setattr(mfa, "aws_json", fake)
    monkeypatch.setattr(mfa, "require_history_disabled", lambda: None)
    for stream in (mfa.sys.stdin, mfa.sys.stdout, mfa.sys.stderr):
        monkeypatch.setattr(stream, "isatty", lambda: True)
    answers = iter(["ENROLL", "SCANNED"])
    monkeypatch.setattr("builtins.input", lambda prompt: next(answers))
    codes = iter(["123456", "234567"])
    monkeypatch.setattr(mfa.getpass, "getpass", lambda prompt: next(codes))
    return fake


def test_enrolls_exact_device_preserves_orphan_and_removes_only_new_local_qr(capsys, fake_enrollment, monkeypatch):
    # Pytest replaces capture streams between fixture setup and the test call.
    for stream in (mfa.sys.stdin, mfa.sys.stdout, mfa.sys.stderr):
        monkeypatch.setattr(stream, "isatty", lambda: True)
    fake = fake_enrollment
    receipt = mfa.enroll()
    assert stat.S_IMODE(receipt.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(receipt.stat().st_mode) == 0o600
    assert not (receipt.parent / "bootstrap.png").exists()
    data = json.loads(receipt.read_text())
    assert data["device_arn"] == mfa.DEVICE_ARN
    assert data["existing_unassigned_devices_preserved"] == sorted(fake.existing)
    mutations = [x[1] for x in fake.calls if x[1] in {"create-virtual-mfa-device", "enable-mfa-device"}]
    assert mutations == ["create-virtual-mfa-device", "enable-mfa-device"]
    output = capsys.readouterr()
    assert "123456" not in output.out + output.err + receipt.read_text()
    assert "234567" not in output.out + output.err + receipt.read_text()


@pytest.mark.parametrize("case", ["wrong-account", "root", "already-assigned", "name-collision"])
def test_bad_preflight_never_creates_or_enables(case, fake_enrollment):
    fake = fake_enrollment
    if case == "wrong-account":
        fake.identity["Account"] = "000000000000"
    elif case == "root":
        fake.identity["Arn"] = f"arn:aws:iam::{mfa.ACCOUNT}:root"
    elif case == "already-assigned":
        fake.assigned.add("existing-device")
    else:
        fake.existing.add(mfa.DEVICE_ARN)
    with pytest.raises(mfa.EnrollmentError):
        mfa.enroll()
    assert not any(x[1] in {"create-virtual-mfa-device", "enable-mfa-device"} for x in fake.calls)
    assert not fake.folder.exists()


def test_failure_preserves_pending_device_and_private_qr(fake_enrollment):
    fake_enrollment.fail_enable = True
    with pytest.raises(mfa.EnrollmentError):
        mfa.enroll()
    assert fake_enrollment.created
    assert (fake_enrollment.folder / "bootstrap.png").exists()
    assert not any("delete" in x[1] or "deactivate" in x[1] for x in fake_enrollment.calls)


def test_noninteractive_run_stops_before_aws(monkeypatch):
    monkeypatch.setattr(mfa.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(mfa, "aws_json", lambda *a, **k: pytest.fail("AWS must not be called"))
    with pytest.raises(mfa.EnrollmentError, match="interactive Terminal"):
        mfa.enroll()


def test_credential_and_code_values_never_go_to_arguments_or_errors(monkeypatch):
    seen = {}
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-environment-override")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/unapproved/config")

    def fail(command, **kwargs):
        seen.update(command=command, **kwargs)
        return subprocess.CompletedProcess(command, 255, "PRIVATE_RESPONSE", "An error occurred (AccessDenied): PRIVATE_SEED 123456")

    monkeypatch.setattr(mfa.subprocess, "run", fail)
    with pytest.raises(mfa.EnrollmentError) as exc:
        mfa.aws_json("iam", "list-mfa-devices", "--user-name", mfa.USER)
    assert "123456" not in str(exc.value) + " ".join(seen["command"])
    assert "PRIVATE" not in str(exc.value)
    assert "AWS_ACCESS_KEY_ID" not in seen["env"]
    assert "AWS_CONFIG_FILE" not in seen["env"]
    assert seen["env"]["AWS_MAX_ATTEMPTS"] == "1"
    assert "input" not in seen
    assert seen["capture_output"] is True


def test_delete_and_other_operations_are_unconditionally_disallowed():
    with pytest.raises(mfa.EnrollmentError, match="allowlist"):
        mfa.aws_json("iam", "delete-virtual-mfa-device")
    assert {x for x in mfa.OPERATIONS if x[1].startswith(("create", "enable"))} == {
        ("iam", "create-virtual-mfa-device"), ("iam", "enable-mfa-device"),
    }


def test_history_and_echo_fallback_fail_closed(monkeypatch):
    monkeypatch.setattr(mfa.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, "enabled\n", ""))
    with pytest.raises(mfa.EnrollmentError, match="history"):
        mfa.require_history_disabled()

    def echo_fallback(prompt):
        warnings.warn("No hidden input", mfa.getpass.GetPassWarning)

    monkeypatch.setattr(mfa.getpass, "getpass", echo_fallback)
    with pytest.raises(mfa.EnrollmentError, match="Hidden Terminal"):
        mfa.read_code("Code: ")


def test_resume_attaches_existing_device_without_creation_or_qr_access(fake_enrollment, monkeypatch):
    fake = fake_enrollment
    fake.created = True
    monkeypatch.setattr("builtins.input", lambda prompt: "RESUME")
    receipt = mfa.enroll(resume=True)
    assert not any(x[1] == "create-virtual-mfa-device" for x in fake.calls)
    assert not (receipt.parent / "bootstrap.png").exists()
    data = json.loads(receipt.read_text())
    assert data["resumed_existing_device"] is True
    assert data["existing_unassigned_devices_preserved"] == sorted(fake.existing)
    assert fake.assigned == {mfa.DEVICE_ARN}


def test_resume_missing_or_assigned_target_cannot_mutate(fake_enrollment):
    with pytest.raises(mfa.EnrollmentError, match="not unassigned"):
        mfa.enroll(resume=True)
    assert not any(x[1] in {"enable-mfa-device", "create-virtual-mfa-device"} for x in fake_enrollment.calls)


def test_secret_payload_uses_sdk_only_without_subprocess(monkeypatch):
    seen = []
    monkeypatch.setattr(mfa, "enable_with_sdk", lambda p: seen.append(p))
    monkeypatch.setattr(mfa.subprocess, "run", lambda *a, **k: pytest.fail("No CLI secret transport"))
    payload = {"UserName": mfa.USER, "SerialNumber": mfa.DEVICE_ARN, "AuthenticationCode1": "123456", "AuthenticationCode2": "234567"}
    assert mfa.aws_json("iam", "enable-mfa-device", payload=payload) is None
    assert seen == [payload]
    with pytest.raises(mfa.EnrollmentError, match="never CLI"):
        mfa.aws_json("iam", "enable-mfa-device", "--authentication-code1", "123456")


@pytest.mark.parametrize("fail", [False, True])
def test_actual_sdk_service_model_accepts_codes_in_memory_and_redacts_errors(monkeypatch, capsys, fail):
    from botocore.stub import Stubber

    client = mfa.boto3.client("iam", region_name="us-west-2", aws_access_key_id="offline-key", aws_secret_access_key="offline-secret")
    payload = {"UserName": mfa.USER, "SerialNumber": mfa.DEVICE_ARN, "AuthenticationCode1": "123456", "AuthenticationCode2": "234567"}
    seen = {}

    class Session:
        def client(self, service, **kwargs):
            assert service == "iam"
            assert kwargs["config"].retries["total_max_attempts"] == 1
            seen["environment"] = dict(mfa.os.environ)
            return client

    monkeypatch.setattr(mfa.boto3, "Session", lambda **kwargs: Session())
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "environment-override")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/unapproved/config")
    with Stubber(client) as stub:
        if fail:
            stub.add_client_error("enable_mfa_device", service_error_code="InvalidAuthenticationCode", service_message="PRIVATE 123456 234567", expected_params=payload)
            with pytest.raises(mfa.EnrollmentError) as exc:
                mfa.enable_with_sdk(payload)
            assert "PRIVATE" not in str(exc.value)
            assert "123456" not in str(exc.value)
        else:
            stub.add_response("enable_mfa_device", {}, expected_params=payload)
            assert mfa.enable_with_sdk(payload) is None
        stub.assert_no_pending_responses()
    assert "AWS_ACCESS_KEY_ID" not in seen["environment"]
    assert "AWS_CONFIG_FILE" not in seen["environment"]
    assert mfa.os.environ["AWS_ACCESS_KEY_ID"] == "environment-override"
    output = capsys.readouterr()
    assert "123456" not in output.out + output.err
    assert "234567" not in output.out + output.err


@pytest.mark.parametrize("phase", ["attachment", "confirmation", "receipt"])
def test_error_reporting_preserves_known_remote_outcome_and_never_prints_error_data(monkeypatch, capsys, phase):
    def interrupt(resume=False, progress=None):
        assert resume is True
        progress.phase = phase
        progress.attachment_started = True
        progress.attachment_returned = phase != "attachment"
        progress.verified = phase == "receipt"
        raise subprocess.TimeoutExpired(["aws"], 60, output="PRIVATE_SEED 123456")

    monkeypatch.setattr(mfa, "enroll", interrupt)
    assert mfa.main(["--resume"]) == 1
    output = capsys.readouterr().err
    assert "Do not repeat enrollment" in output
    assert f"Stage: {phase}" in output
    assert "TimeoutExpired" in output
    assert "PRIVATE_SEED" not in output
    assert "123456" not in output
    if phase == "attachment":
        assert "may already be enabled" in output
    elif phase == "confirmation":
        assert "request returned successfully" in output
    else:
        assert "MFA ENROLLMENT VERIFIED" in output
