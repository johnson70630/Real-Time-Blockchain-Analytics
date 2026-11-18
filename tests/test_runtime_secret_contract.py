import contextlib
import importlib.util
import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "secret_contract", ROOT / "scripts/check_runtime_secret_contract.py"
)
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
FAKE = "wss://arb-mainnet.g.alchemy.com/v2/fake"


def response(**changes):
    value = {
        "ARN": p.EXPECTED_SECRET,
        "Name": p.EXPECTED_NAME,
        "VersionId": p.EXPECTED_VERSION,
        "VersionStages": ["AWSCURRENT"],
        "SecretString": FAKE,
        "CreatedDate": "2026-11-17T03:02:41+00:00",
    }
    value.update(changes)
    return json.dumps(value)


def test_exact_version_and_format_pass_without_any_external_action():
    with patch.object(subprocess, "run", side_effect=AssertionError("NO_IO")):
        report = p.assess_response(response())
    assert report == {
        "status": "CONTRACT_PASS",
        "expected_version_id": p.EXPECTED_VERSION,
        "identity_codes": [],
        "format_status": "FORMAT_PASS",
        "format_codes": [],
        "response_identity_matches": True,
        "raw_format_status": "FORMAT_PASS",
        "raw_format_codes": [],
        "secret_representation": "BARE_URL",
    }
    assert FAKE not in json.dumps(report)


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"ARN": "private-placeholder"}, "SECRET_ARN_MISMATCH"),
        ({"Name": "private-placeholder"}, "SECRET_NAME_MISMATCH"),
        ({"VersionId": "private-placeholder"}, "VERSION_ID_MISMATCH"),
        ({"VersionStages": ["AWSPREVIOUS"]}, "CURRENT_STAGE_MISMATCH"),
        ({"VersionStages": "AWSCURRENT"}, "CURRENT_STAGE_MISMATCH"),
        ({"VersionStages": ["AWSCURRENT", {}]}, "CURRENT_STAGE_MISMATCH"),
        ({"SecretBinary": "private-placeholder"}, "SECRET_BINARY_PRESENT"),
        ({"SecretString": None}, "SECRET_STRING_NOT_STRING"),
        ({"SecretString": {}}, "SECRET_STRING_NOT_STRING"),
    ],
)
def test_identity_or_field_mismatch_fails_before_format_inspection(change, code):
    with patch.object(p, "dependencies", side_effect=AssertionError("NO_INSPECTION")):
        report = p.assess_response(response(**change))
    assert report["status"] == "CONTRACT_FAIL"
    assert report["format_status"] == "NOT_CHECKED"
    assert code in report["identity_codes"]
    assert "private-placeholder" not in json.dumps(report)


def test_binary_only_response_never_decodes_or_prints_binary():
    raw = json.loads(response(SecretBinary="private-placeholder"))
    del raw["SecretString"]
    report = p.assess_response(json.dumps(raw))
    assert set(report["identity_codes"]) == {
        "SECRET_BINARY_PRESENT",
        "SECRET_STRING_MISSING",
        "SECRET_STRING_NOT_STRING",
    }
    assert "private-placeholder" not in json.dumps(report)


@pytest.mark.parametrize(
    ("value", "code"),
    [
        ("", "SECRET_PAYLOAD_EMPTY_OR_TOO_LARGE"),
        (json.dumps({"wrong_field": FAKE}), "SECRET_JSON_SHAPE_UNSUPPORTED"),
        (FAKE + "\n", "INVALID_ENDPOINT"),
        (FAKE.replace("wss:", "https:"), "INVALID_ENDPOINT"),
        (FAKE.replace("arb-mainnet", "eth-mainnet"), "INVALID_ENDPOINT"),
        (FAKE + "?private-placeholder", "INVALID_ENDPOINT"),
        (FAKE + "/private-placeholder", "INVALID_ENDPOINT"),
        (FAKE.replace(".com/", ".com:80/"), "INVALID_ENDPOINT"),
        (FAKE.replace("://", "://["), "INVALID_ENDPOINT"),
    ],
)
def test_format_codes_are_specific_and_never_contain_value(value, code):
    report = p.assess_response(response(SecretString=value))
    assert report["response_identity_matches"] is True
    assert report["status"] == "CONTRACT_FAIL"
    assert code in report["format_codes"]
    chain, _ = p.dependencies()
    assert set(report["format_codes"]) <= chain.SECRET_CODES
    assert "private-placeholder" not in json.dumps(report)
    assert "alchemy.com" not in json.dumps(report)


@pytest.mark.parametrize(
    "stored", [json.dumps(FAKE), json.dumps({"ALCHEMY_WEBSOCKET_URL": FAKE})]
)
def test_supported_json_payload_passes_with_raw_format_observation_preserved(stored):
    report = p.assess_response(response(SecretString=stored))
    assert report["status"] == "CONTRACT_PASS"
    assert report["raw_format_status"] == "FORMAT_FAIL"
    assert "JSON_OR_QUOTES" in report["raw_format_codes"]
    assert report["format_status"] == "FORMAT_PASS"
    assert report["format_codes"] == []
    assert report["secret_representation"] in {"JSON_STRING", "JSON_OBJECT"}
    assert FAKE not in json.dumps(report)


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (None, "INVALID_RESPONSE_SIZE_OR_TYPE"),
        ("x" * 524289, "INVALID_RESPONSE_SIZE_OR_TYPE"),
        ("private-placeholder", "RESPONSE_NOT_JSON"),
        ("[]", "RESPONSE_NOT_OBJECT"),
        ("null", "RESPONSE_NOT_OBJECT"),
        ('"private-placeholder"', "RESPONSE_NOT_OBJECT"),
    ],
)
def test_bad_response_emits_only_constant_codes(payload, code):
    result = p.assess_response(payload)
    assert result["identity_codes"] == [code]
    assert "private-placeholder" not in json.dumps(result)


def test_unknown_response_fields_never_reach_output():
    report = p.assess_response(response(PrivateField={"key": "private-placeholder"}))
    assert report["status"] == "CONTRACT_PASS"
    assert "private-placeholder" not in json.dumps(report)


def test_pinned_request_uses_full_response_and_current_stage():
    args = p.cli_command("/usr/bin/aws")
    assert args[args.index("--version-id") + 1] == p.EXPECTED_VERSION
    assert args[args.index("--version-stage") + 1] == "AWSCURRENT"
    assert args[args.index("--secret-id") + 1] == p.EXPECTED_SECRET
    assert "--query" not in args
    assert FAKE not in args
    assert not any(x in args for x in ["--debug", "--profile", "--endpoint-url"])


def test_changed_dependency_blocks_inspection():
    with patch.object(p.Path, "read_bytes", return_value=b"changed"):
        with pytest.raises(p.DiagnosticFailure, match="DEPENDENCY_CHANGED"):
            p.assess_response(response())


@pytest.mark.parametrize("bad_path", ["/tmp/release.json", "/opt/other.json"])
def test_unreviewed_path_precedes_imds_or_cli(bad_path):
    chain = Mock()
    with patch.object(p.os, "geteuid", return_value=0):
        with pytest.raises(p.DiagnosticFailure, match="APPROVED_HOST_REQUIRED"):
            p.verify_host(bad_path, chain)
    chain.instance_id.assert_not_called()


def test_nonroot_host_is_rejected_before_release_read():
    with (
        patch.object(p.os, "geteuid", return_value=1000),
        patch.object(p.Path, "read_bytes", side_effect=AssertionError("NO_READ")),
    ):
        with pytest.raises(p.DiagnosticFailure, match="APPROVED_HOST_REQUIRED"):
            p.verify_host(p.RELEASE_PATH, Mock())


def test_wrong_instance_is_rejected_before_aws():
    chain, _ = p.dependencies()
    body = (
        ROOT / "docs/evidence/aws_runtime_release_candidate_2026-11-17.json"
    ).read_bytes()
    with (
        patch.object(p.os, "geteuid", return_value=0),
        patch.object(p.Path, "read_bytes", return_value=body),
        patch.object(
            p.Path, "read_text", return_value="Filename Type Size Used Priority\n"
        ),
        patch.object(chain, "instance_id", return_value="wrong-instance"),
    ):
        with pytest.raises(p.DiagnosticFailure, match="APPROVED_HOST_REQUIRED"):
            p.verify_host(p.RELEASE_PATH, chain)


def test_execute_makes_one_pinned_retrieval_with_no_inherited_credentials():
    version = SimpleNamespace(
        returncode=0, stdout="aws-cli/2.35.2 Python/3.14.5", stderr=""
    )
    answer = SimpleNamespace(returncode=0, stdout=response(), stderr="")
    with (
        patch.object(p, "verify_host"),
        patch.object(p.resource, "setrlimit"),
        patch.object(p.shutil, "which", return_value="/usr/bin/aws"),
        patch.object(p.subprocess, "run", side_effect=[version, answer]) as run,
        patch.dict(
            p.os.environ,
            {
                "AWS_SECRET_ACCESS_KEY": "private-placeholder",
                "AWS_PROFILE": "private-placeholder",
            },
        ),
    ):
        report = p.execute(p.RELEASE_PATH)
    assert report["status"] == "CONTRACT_PASS"
    assert report["aws_cli_version"] == "2.35.2"
    assert run.call_count == 2
    args, kwargs = run.call_args
    assert args[0] == p.cli_command("/usr/bin/aws")
    assert kwargs["env"]["AWS_MAX_ATTEMPTS"] == "1"
    assert kwargs["env"]["AWS_CONFIG_FILE"] == "/dev/null"
    assert "AWS_SECRET_ACCESS_KEY" not in kwargs["env"]
    assert "AWS_PROFILE" not in kwargs["env"]
    assert kwargs["capture_output"] is True and kwargs["timeout"] == 25


def test_failed_host_verification_prevents_every_cli_command():
    with (
        patch.object(
            p, "verify_host", side_effect=p.DiagnosticFailure("APPROVED_HOST_REQUIRED")
        ),
        patch.object(p.resource, "setrlimit"),
        patch.object(p.subprocess, "run") as run,
    ):
        with pytest.raises(p.DiagnosticFailure):
            p.execute(p.RELEASE_PATH)
    run.assert_not_called()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (p.DiagnosticFailure("DEPENDENCY_CHANGED"), "DEPENDENCY_CHANGED"),
        (RuntimeError("private-placeholder"), "HOST_ERROR"),
        (
            subprocess.TimeoutExpired(
                "private-placeholder", 25, output="private-placeholder"
            ),
            "TIMEOUT",
        ),
    ],
)
def test_main_hides_errors_and_partial_timeout_output(error, expected):
    output = io.StringIO()
    with (
        patch.object(
            p.sys, "argv", ["check", "--execute-on-approved-host", str(p.RELEASE_PATH)]
        ),
        patch.object(p, "execute", side_effect=error),
        contextlib.redirect_stdout(output),
    ):
        assert p.main() == 1
    assert json.loads(output.getvalue()) == {
        "status": "DIAGNOSTIC_FAILED",
        "code": expected,
    }


def test_provider_error_is_allowlisted_without_body_or_secret():
    version = SimpleNamespace(returncode=0, stdout="aws-cli/2.35.2", stderr="")
    failed = SimpleNamespace(
        returncode=254,
        stdout="private-placeholder",
        stderr="An error occurred (InvalidRequestException) private-placeholder",
    )
    with (
        patch.object(p, "verify_host"),
        patch.object(p.resource, "setrlimit"),
        patch.object(p.shutil, "which", return_value="/usr/bin/aws"),
        patch.object(p.subprocess, "run", side_effect=[version, failed]),
    ):
        report = p.execute(p.RELEASE_PATH)
    assert report == {
        "status": "DIAGNOSTIC_FAILED",
        "code": "AWS_CLI_FAILED",
        "aws_error_code": "InvalidRequestException",
    }
