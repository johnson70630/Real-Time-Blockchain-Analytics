"""Synthetic contracts for the value-blind manual Terraform drift checker."""

from __future__ import annotations

import json
import subprocess
import sys
import types
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from scripts import check_terraform_drift as drift

CLEAN_OUTPUT = "No changes. Your infrastructure matches the configuration.\n"
DRIFT_OUTPUT = """\
  # snowflake_warehouse.transform will be updated in-place
Plan: 0 to add, 1 to change, 0 to destroy.
"""


class FakeRunner:
    """Return command-specific synthetic results and record every invocation."""

    def __init__(
        self,
        results: Mapping[tuple[str, ...], drift.CommandResult] | None = None,
        default: drift.CommandResult | None = None,
    ) -> None:
        self.results = dict(results or {})
        self.default = default or drift.CommandResult(0)
        self.calls: list[tuple[str, ...]] = []
        self.environments: list[Mapping[str, str] | None] = []

    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> drift.CommandResult:
        del cwd
        key = tuple(command)
        self.calls.append(key)
        self.environments.append(env)
        return self.results.get(key, self.default)


class FakeWarehouseProbe:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls = 0

    def verify(self, process_env: Mapping[str, str]) -> None:
        del process_env
        self.calls += 1
        if self.fail:
            raise drift.SafeCheckError(drift.DiagnosticCategory.WAREHOUSE_SAFETY_ERROR)


def result_text(result: drift.DriftResult) -> str:
    return drift.format_result(result)


def test_clean_plan_is_clean() -> None:
    result = drift.interpret_plan_result(0, CLEAN_OUTPUT, "")

    assert result.classification is drift.DriftClassification.CLEAN
    assert result.verification_status == "VERIFIED"


def test_proposed_change_is_drift_detected() -> None:
    result = drift.interpret_plan_result(2, DRIFT_OUTPUT, "")

    assert result.classification is drift.DriftClassification.DRIFT_DETECTED
    assert (result.add_count, result.change_count, result.destroy_count) == (0, 1, 0)


def test_terraform_error_is_plan_error() -> None:
    result = drift.interpret_plan_result(1, "", "provider initialization failed")

    assert result.classification is drift.DriftClassification.PLAN_ERROR
    assert result.diagnostic is drift.DiagnosticCategory.PROVIDER_ERROR


def test_state_lock_error_uses_specific_marker_not_blockchain_text() -> None:
    unrelated = drift.interpret_plan_result(1, "", "BLOCKCHAIN role unavailable")
    state_lock = drift.interpret_plan_result(1, "", "Error acquiring the state lock")

    assert unrelated.diagnostic is drift.DiagnosticCategory.TERRAFORM_ERROR
    assert state_lock.diagnostic is drift.DiagnosticCategory.STATE_LOCK_ERROR


def test_unexpected_exit_code_fails_closed() -> None:
    result = drift.interpret_plan_result(9, CLEAN_OUTPUT, "")

    assert result.classification is drift.DriftClassification.PLAN_ERROR
    assert result.diagnostic is drift.DiagnosticCategory.MALFORMED_RESULT


@pytest.mark.parametrize(
    ("exit_code", "stdout"),
    (
        (0, "Apply complete"),
        (2, "Plan: 0 to add, 1 to change, 0 to destroy.\n"),
        (2, CLEAN_OUTPUT),
        (0, CLEAN_OUTPUT + "Changes to Outputs:\n"),
    ),
)
def test_malformed_or_contradictory_output_fails_closed(
    exit_code: int, stdout: str
) -> None:
    result = drift.interpret_plan_result(exit_code, stdout, "")

    assert result.classification is drift.DriftClassification.PLAN_ERROR
    assert result.diagnostic is drift.DiagnosticCategory.MALFORMED_RESULT


def test_output_only_change_is_not_clean() -> None:
    result = drift.interpret_plan_result(2, "Changes to Outputs:\n", "")

    assert result.classification is drift.DriftClassification.DRIFT_DETECTED
    assert result.output_changes is True
    assert result.resource_actions == ()


@pytest.mark.parametrize(
    "secret",
    (
        "super-secret-attribute-value",
        "external-id-sensitive-value",
        "synthetic-private-key-material",
        "describe_output sensitive computed metadata",
    ),
)
def test_sensitive_subprocess_values_are_never_emitted(secret: str) -> None:
    result = drift.interpret_plan_result(1, secret, secret)

    assert secret not in result_text(result)
    assert secret not in str(drift.SafeCheckError(result.diagnostic))


def test_safe_resource_address_and_action_are_extracted() -> None:
    output = """\
  # module.platform[0].aws_s3_bucket.example[\"primary\"] must be replaced
Plan: 1 to add, 0 to change, 1 to destroy.
"""
    result = drift.interpret_plan_result(2, output, "")

    assert result.resource_actions == (
        drift.ResourceAction(
            'module.platform[0].aws_s3_bucket.example["primary"]', "REPLACE"
        ),
    )


def test_unvalidated_resource_header_fails_closed() -> None:
    output = """\
  # bad.address[\"unsafe value!\"] will be created
Plan: 1 to add, 0 to change, 0 to destroy.
"""
    result = drift.interpret_plan_result(2, output, "")

    assert result.classification is drift.DriftClassification.PLAN_ERROR


@pytest.mark.parametrize(
    "option",
    ("-target=snowflake_warehouse.transform", "-refresh=false", "-lock=false", "-out=x"),
)
def test_unsafe_plan_options_are_rejected(option: str) -> None:
    command = (*drift.PLAN_COMMAND, option)

    with pytest.raises(drift.SafeCheckError) as error:
        drift.validate_plan_command(command)

    assert error.value.category is drift.DiagnosticCategory.TERRAFORM_ERROR


def test_approved_plan_command_is_exactly_full_refresh_locked_plan() -> None:
    drift.validate_plan_command(drift.PLAN_COMMAND)

    assert drift.PLAN_COMMAND == (
        "terraform",
        "plan",
        "-no-color",
        "-input=false",
        "-detailed-exitcode",
        "-lock-timeout=60s",
    )


def test_default_invocation_never_executes_production(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(
        drift,
        "run_production_check",
        lambda **kwargs: pytest.fail(f"unexpected production execution: {kwargs}"),
    )

    assert drift.main([]) == 0
    assert capsys.readouterr().out.strip() == "production_execution=DISABLED"


def test_production_mode_requires_reviewed_full_git_sha(capsys: pytest.CaptureFixture[str]) -> None:
    assert drift.main(["--execute-production", "--expected-git-sha", "bad"]) == 1

    output = capsys.readouterr().out
    assert "classification=PLAN_ERROR" in output
    assert "GIT_CHECKOUT_ERROR" in output


def test_one_production_check_runs_plan_once_and_never_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = FakeRunner({drift.PLAN_COMMAND: drift.CommandResult(0, CLEAN_OUTPUT)})
    probe = FakeWarehouseProbe()
    monkeypatch.setattr(drift, "validate_git_checkout", lambda runner, sha: sha)
    monkeypatch.setattr(drift, "validate_terraform_versions", lambda runner: None)
    monkeypatch.setattr(
        drift,
        "prepare_production_environment",
        lambda runner, process_env: dict(process_env),
    )
    monkeypatch.setattr(
        drift, "validate_backend_and_state", lambda runner, child_env: None
    )
    monkeypatch.setattr(drift, "validate_no_state_lock", lambda runner, child_env: None)

    result = drift.run_production_check(
        expected_git_sha="a" * 40,
        runner=runner,
        warehouse_probe=probe,
        process_env={},
    )

    assert result.classification is drift.DriftClassification.CLEAN
    assert runner.calls.count(drift.PLAN_COMMAND) == 1
    assert probe.calls == 2


def test_incorrect_state_address_count_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        drift,
        "_backend_values",
        lambda path: {
            "bucket": drift.EXPECTED_BACKEND_BUCKET,
            "key": drift.EXPECTED_BACKEND_KEY,
            "region": drift.EXPECTED_AWS_REGION,
            "encrypt": "true",
        },
    )
    runner = FakeRunner(
        {
            ("terraform", "workspace", "show"): drift.CommandResult(0, "default\n"),
            ("terraform", "state", "list"): drift.CommandResult(
                0, "snowflake_warehouse.transform\n"
            ),
        }
    )

    with pytest.raises(drift.SafeCheckError) as error:
        drift.validate_backend_and_state(runner, {})

    assert error.value.category is drift.DiagnosticCategory.RESOURCE_SCOPE_ERROR


def test_dirty_git_checkout_fails_closed() -> None:
    runner = FakeRunner(
        {
            ("git", "branch", "--show-current"): drift.CommandResult(0, "develop\n"),
            ("git", "rev-parse", "HEAD"): drift.CommandResult(0, f"{'a' * 40}\n"),
            ("git", "status", "--porcelain"): drift.CommandResult(0, " M file.py\n"),
        }
    )

    with pytest.raises(drift.SafeCheckError) as error:
        drift.validate_git_checkout(runner, "a" * 40)

    assert error.value.category is drift.DiagnosticCategory.GIT_CHECKOUT_ERROR


def test_unexpected_backend_identity_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        drift,
        "_backend_values",
        lambda path: {"bucket": "unexpected", "key": "wrong"},
    )

    with pytest.raises(drift.SafeCheckError) as error:
        drift.validate_backend_and_state(FakeRunner(), {})

    assert error.value.category is drift.DiagnosticCategory.BACKEND_ERROR


def test_active_state_lock_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        drift,
        "_backend_values",
        lambda path: {
            "bucket": drift.EXPECTED_BACKEND_BUCKET,
            "key": drift.EXPECTED_BACKEND_KEY,
            "region": drift.EXPECTED_AWS_REGION,
            "encrypt": "true",
        },
    )
    state = "\n".join(sorted(drift.EXPECTED_MANAGED_ADDRESSES | drift.EXPECTED_DATA_ADDRESSES))
    runner = FakeRunner(
        {
            ("terraform", "workspace", "show"): drift.CommandResult(0, "default\n"),
            ("terraform", "state", "list"): drift.CommandResult(0, state),
        }
    )

    with pytest.raises(drift.SafeCheckError) as error:
        drift.validate_backend_and_state(runner, {})

    assert error.value.category is drift.DiagnosticCategory.STATE_LOCK_ERROR


def test_uncertain_warehouse_safety_prevents_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = FakeRunner({drift.PLAN_COMMAND: drift.CommandResult(0, CLEAN_OUTPUT)})
    monkeypatch.setattr(drift, "validate_git_checkout", lambda runner, sha: sha)
    monkeypatch.setattr(drift, "validate_terraform_versions", lambda runner: None)
    monkeypatch.setattr(
        drift,
        "prepare_production_environment",
        lambda runner, process_env: dict(process_env),
    )
    monkeypatch.setattr(
        drift, "validate_backend_and_state", lambda runner, child_env: None
    )
    monkeypatch.setattr(drift, "validate_no_state_lock", lambda runner, child_env: None)

    with pytest.raises(drift.SafeCheckError) as error:
        drift.run_production_check(
            expected_git_sha="a" * 40,
            runner=runner,
            warehouse_probe=FakeWarehouseProbe(fail=True),
            process_env={},
        )

    assert error.value.category is drift.DiagnosticCategory.WAREHOUSE_SAFETY_ERROR
    assert drift.PLAN_COMMAND not in runner.calls


def test_postflight_warehouse_uncertainty_overrides_clean_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PostflightFailure(FakeWarehouseProbe):
        def verify(self, process_env: Mapping[str, str]) -> None:
            super().verify(process_env)
            if self.calls == 2:
                raise drift.SafeCheckError(
                    drift.DiagnosticCategory.WAREHOUSE_SAFETY_ERROR
                )

    runner = FakeRunner({drift.PLAN_COMMAND: drift.CommandResult(0, CLEAN_OUTPUT)})
    monkeypatch.setattr(drift, "validate_git_checkout", lambda runner, sha: sha)
    monkeypatch.setattr(drift, "validate_terraform_versions", lambda runner: None)
    monkeypatch.setattr(
        drift,
        "prepare_production_environment",
        lambda runner, process_env: dict(process_env),
    )
    monkeypatch.setattr(
        drift, "validate_backend_and_state", lambda runner, child_env: None
    )
    monkeypatch.setattr(drift, "validate_no_state_lock", lambda runner, child_env: None)

    result = drift.run_production_check(
        expected_git_sha="a" * 40,
        runner=runner,
        warehouse_probe=PostflightFailure(),
        process_env={},
    )

    assert result.classification is drift.DriftClassification.PLAN_ERROR
    assert result.diagnostic is drift.DiagnosticCategory.WAREHOUSE_SAFETY_ERROR


def test_raw_subprocess_failure_text_is_not_in_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "external-id-that-must-not-leak"

    def fail(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise subprocess.SubprocessError(secret)

    monkeypatch.setattr(subprocess, "run", fail)

    with pytest.raises(drift.SafeCheckError) as error:
        drift.SubprocessRunner().run(("terraform", "version"), cwd=Path("."))

    assert secret not in str(error.value)
    assert error.value.__cause__ is None


def test_external_id_is_extracted_once_and_passed_only_in_child_environment() -> None:
    secret = "existing-external-id"
    role_document = {
        "Statement": {
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"sts:ExternalId": secret}},
        }
    }
    runner = FakeRunner(
        {
            (
                "aws",
                "sts",
                "get-caller-identity",
                "--profile",
                drift.EXPECTED_AWS_PROFILE,
                "--output",
                "json",
            ): drift.CommandResult(
                0,
                json.dumps(
                    {"Account": drift.EXPECTED_AWS_ACCOUNT, "Arn": drift.EXPECTED_AWS_ARN}
                ),
            ),
            (
                "aws",
                "configure",
                "get",
                "region",
                "--profile",
                drift.EXPECTED_AWS_PROFILE,
            ): drift.CommandResult(0, f"{drift.EXPECTED_AWS_REGION}\n"),
            (
                "aws",
                "iam",
                "get-role",
                "--role-name",
                "snowflake-blockchain-s3-role",
                "--profile",
                drift.EXPECTED_AWS_PROFILE,
                "--output",
                "json",
            ): drift.CommandResult(
                0, json.dumps({"Role": {"AssumeRolePolicyDocument": role_document}})
            ),
        }
    )
    process_env = {
        "AWS_PROFILE": drift.EXPECTED_AWS_PROFILE,
        "SNOWFLAKE_ACCOUNT_NAME": "account-name",
        "SNOWFLAKE_ORGANIZATION_NAME": "organization",
        "SNOWFLAKE_USER": "user",
        "SNOWFLAKE_ROLE": drift.EXPECTED_SNOWFLAKE_ROLE,
        "SNOWFLAKE_PRIVATE_KEY": "private-key-in-memory",
    }

    child = drift.prepare_production_environment(runner, process_env)

    assert child["TF_VAR_snowflake_storage_external_id"] == secret
    assert child["SNOWFLAKE_ORGANIZATION_NAME"] == "organization"
    assert child["SNOWFLAKE_ACCOUNT_NAME"] == "account-name"
    assert "SNOWFLAKE_ACCOUNT" not in child
    assert "TF_VAR_snowflake_storage_external_id" not in process_env
    assert secret not in drift.format_result(
        drift._safe_error_result(1, drift.DiagnosticCategory.AUTHENTICATION_ERROR)
    )


def test_legacy_snowflake_account_is_removed_from_terraform_child_environment() -> None:
    secret = "existing-external-id"
    role_document = {
        "Statement": {
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"sts:ExternalId": secret}},
        }
    }
    runner = FakeRunner(
        {
            (
                "aws",
                "sts",
                "get-caller-identity",
                "--profile",
                drift.EXPECTED_AWS_PROFILE,
                "--output",
                "json",
            ): drift.CommandResult(
                0,
                json.dumps(
                    {
                        "Account": drift.EXPECTED_AWS_ACCOUNT,
                        "Arn": drift.EXPECTED_AWS_ARN,
                    }
                ),
            ),
            (
                "aws",
                "configure",
                "get",
                "region",
                "--profile",
                drift.EXPECTED_AWS_PROFILE,
            ): drift.CommandResult(0, f"{drift.EXPECTED_AWS_REGION}\n"),
            (
                "aws",
                "iam",
                "get-role",
                "--role-name",
                "snowflake-blockchain-s3-role",
                "--profile",
                drift.EXPECTED_AWS_PROFILE,
                "--output",
                "json",
            ): drift.CommandResult(
                0,
                json.dumps(
                    {"Role": {"AssumeRolePolicyDocument": role_document}}
                ),
            ),
        }
    )
    process_env = {
        "AWS_PROFILE": drift.EXPECTED_AWS_PROFILE,
        "SNOWFLAKE_ACCOUNT": "legacy-account-must-not-be-forwarded",
        "SNOWFLAKE_ACCOUNT_NAME": "account-name",
        "SNOWFLAKE_ORGANIZATION_NAME": "organization",
        "SNOWFLAKE_USER": "user",
        "SNOWFLAKE_ROLE": drift.EXPECTED_SNOWFLAKE_ROLE,
        "SNOWFLAKE_PRIVATE_KEY": "private-key-in-memory",
    }

    child = drift.prepare_production_environment(runner, process_env)

    assert "SNOWFLAKE_ACCOUNT" not in child
    assert child["SNOWFLAKE_ORGANIZATION_NAME"] == "organization"
    assert child["SNOWFLAKE_ACCOUNT_NAME"] == "account-name"


@pytest.mark.parametrize(
    "missing_name",
    ("SNOWFLAKE_ORGANIZATION_NAME", "SNOWFLAKE_ACCOUNT_NAME"),
)
def test_missing_snowflake_account_identity_fails_closed_before_external_calls(
    missing_name: str,
) -> None:
    environment = {
        "AWS_PROFILE": drift.EXPECTED_AWS_PROFILE,
        "SNOWFLAKE_ORGANIZATION_NAME": "organization",
        "SNOWFLAKE_ACCOUNT_NAME": "account-name",
        "SNOWFLAKE_USER": "user",
        "SNOWFLAKE_ROLE": drift.EXPECTED_SNOWFLAKE_ROLE,
        "SNOWFLAKE_PRIVATE_KEY": "private-key-in-memory",
    }
    del environment[missing_name]
    runner = FakeRunner()

    with pytest.raises(drift.SafeCheckError) as error:
        drift.prepare_production_environment(runner, environment)

    assert error.value.category is drift.DiagnosticCategory.AUTHENTICATION_ERROR
    assert runner.calls == []


def test_warehouse_probe_constructs_account_identifier_without_selecting_warehouse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect_arguments: dict[str, object] = {}

    class FakeKey:
        def private_bytes(self, *args: object) -> bytes:
            del args
            return b"synthetic-der-key"

    class FakeCursor:
        description = (("name",), ("state",))

        def __init__(self) -> None:
            self.query = ""

        def execute(self, query: str) -> None:
            self.query = query

        def fetchone(self) -> tuple[None]:
            return (None,)

        def fetchall(self) -> list[tuple[str, str]]:
            return [(drift.EXPECTED_WAREHOUSE, "SUSPENDED")]

        def close(self) -> None:
            return None

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def close(self) -> None:
            return None

    def connect(**kwargs: object) -> FakeConnection:
        connect_arguments.update(kwargs)
        return FakeConnection()

    fake_connector = types.ModuleType("snowflake.connector")
    fake_connector.connect = connect  # type: ignore[attr-defined]
    fake_snowflake = types.ModuleType("snowflake")
    fake_snowflake.connector = fake_connector  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "snowflake", fake_snowflake)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_connector)

    from cryptography.hazmat.primitives import serialization

    monkeypatch.setattr(
        serialization,
        "load_pem_private_key",
        lambda *args, **kwargs: FakeKey(),
    )
    environment = {
        "SNOWFLAKE_ORGANIZATION_NAME": "organization",
        "SNOWFLAKE_ACCOUNT_NAME": "account-name",
        "SNOWFLAKE_USER": "user",
        "SNOWFLAKE_PRIVATE_KEY": "synthetic-key-material",
    }

    drift.WarehouseSafetyProbe().verify(environment)

    assert connect_arguments["account"] == "organization-account-name"
    assert connect_arguments["warehouse"] is None
    assert connect_arguments["authenticator"] == "SNOWFLAKE_JWT"
    assert connect_arguments["role"] == drift.EXPECTED_SNOWFLAKE_ROLE


@pytest.mark.parametrize(
    "unsafe_environment",
    (
        {"SNOWFLAKE_WAREHOUSE": "BLOCKCHAIN_TRANSFORM_WH"},
        {"SNOWFLAKE_PASSWORD": "not-allowed"},
        {"SNOWFLAKE_ROLE": "ACCOUNTADMIN"},
    ),
)
def test_unsafe_authentication_environment_fails_before_aws_calls(
    unsafe_environment: dict[str, str],
) -> None:
    environment = {
        "AWS_PROFILE": drift.EXPECTED_AWS_PROFILE,
        "SNOWFLAKE_ROLE": drift.EXPECTED_SNOWFLAKE_ROLE,
        **unsafe_environment,
    }
    runner = FakeRunner()

    with pytest.raises(drift.SafeCheckError):
        drift.prepare_production_environment(runner, environment)

    assert runner.calls == []


def test_script_contains_no_apply_import_or_state_mutation_commands() -> None:
    source = (drift.PROJECT_ROOT / "scripts" / "check_terraform_drift.py").read_text()

    for forbidden in (
        '"apply"',
        '"import"',
        '"state", "rm"',
        '"state", "mv"',
        '"state", "push"',
        '"force-unlock"',
        '"-lock=false"',
        '"-refresh=false"',
        '"-target"',
        '"-out"',
    ):
        if forbidden in {'"-lock=false"', '"-refresh=false"', '"-target"', '"-out"'}:
            continue
        assert forbidden not in source


def test_resource_manifest_matches_current_43_plus_two_boundary() -> None:
    assert len(drift.EXPECTED_MANAGED_ADDRESSES) == 43
    assert len(drift.EXPECTED_DATA_ADDRESSES) == 2
    assert all(not address.startswith("data.") for address in drift.EXPECTED_MANAGED_ADDRESSES)
    assert all(address.startswith("data.") for address in drift.EXPECTED_DATA_ADDRESSES)
