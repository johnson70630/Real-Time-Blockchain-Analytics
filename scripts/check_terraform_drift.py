"""Value-blind, explicitly authorized production Terraform drift checking."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ROOT = PROJECT_ROOT / "infra" / "terraform" / "environments" / "prod"

EXPECTED_BRANCH = "develop"
EXPECTED_TERRAFORM_VERSION = "1.17.0"
EXPECTED_AWS_PROVIDER_VERSION = "6.69.1"
EXPECTED_SNOWFLAKE_PROVIDER_VERSION = "2.22.0"
EXPECTED_AWS_PROFILE = "blockchain-de"
EXPECTED_AWS_ACCOUNT = "209479274979"
EXPECTED_AWS_ARN = "arn:aws:iam::209479274979:user/blockchain-de-local"
EXPECTED_AWS_REGION = "us-west-2"
EXPECTED_BACKEND_BUCKET = (
    "blockchain-analytics-terraform-state-209479274979-usw2"
)
EXPECTED_BACKEND_KEY = "production/platform/terraform.tfstate"
EXPECTED_WORKSPACE = "default"
EXPECTED_WAREHOUSE = "BLOCKCHAIN_TRANSFORM_WH"
EXPECTED_SNOWFLAKE_ROLE = "BLOCKCHAIN_ADMIN_ROLE"
EXPECTED_MANAGED_RESOURCE_COUNT = 43
EXPECTED_DATA_SOURCE_COUNT = 2

PLAN_COMMAND = (
    "terraform",
    "plan",
    "-no-color",
    "-input=false",
    "-detailed-exitcode",
    "-lock-timeout=60s",
)

EXPECTED_MANAGED_ADDRESSES = frozenset(
    {
        "aws_iam_policy.snowflake_s3_read",
        "aws_iam_role.snowflake_s3",
        "aws_iam_role_policy_attachment.snowflake_s3_read",
        "aws_s3_bucket.production_data",
        "aws_s3_bucket_ownership_controls.production_data",
        "aws_s3_bucket_public_access_block.production_data",
        "aws_s3_bucket_server_side_encryption_configuration.production_data",
        "aws_s3_bucket_versioning.production_data",
        "snowflake_account_role.admin",
        "snowflake_account_role.analyst",
        "snowflake_account_role.dbt",
        "snowflake_account_role.de",
        "snowflake_account_role.pipeline",
        "snowflake_database.blockchain",
        "snowflake_file_format_parquet.raw",
        "snowflake_grant_account_role.admin_to_sysadmin",
        "snowflake_grant_account_role.analyst_to_admin",
        "snowflake_grant_account_role.dbt_to_admin",
        "snowflake_grant_account_role.de_to_admin",
        "snowflake_grant_account_role.pipeline_to_admin",
        "snowflake_grant_privileges_to_account_role.control_schema_usage_pipeline",
        "snowflake_grant_privileges_to_account_role.core_schema_dbt_privileges",
        "snowflake_grant_privileges_to_account_role.database_usage_analyst",
        "snowflake_grant_privileges_to_account_role.database_usage_dbt",
        "snowflake_grant_privileges_to_account_role.database_usage_de",
        "snowflake_grant_privileges_to_account_role.database_usage_pipeline",
        "snowflake_grant_privileges_to_account_role.marts_schema_dbt_privileges",
        "snowflake_grant_privileges_to_account_role.raw_schema_usage_dbt",
        "snowflake_grant_privileges_to_account_role.raw_schema_usage_de",
        "snowflake_grant_privileges_to_account_role.raw_schema_usage_pipeline",
        "snowflake_grant_privileges_to_account_role.staging_schema_dbt_privileges",
        "snowflake_grant_privileges_to_account_role.warehouse_usage_analyst",
        "snowflake_grant_privileges_to_account_role.warehouse_usage_dbt",
        "snowflake_grant_privileges_to_account_role.warehouse_usage_de",
        "snowflake_grant_privileges_to_account_role.warehouse_usage_pipeline",
        "snowflake_schema.control",
        "snowflake_schema.core",
        "snowflake_schema.marts",
        "snowflake_schema.raw",
        "snowflake_schema.staging",
        "snowflake_stage_external_s3.silver",
        "snowflake_storage_integration_aws.blockchain_s3",
        "snowflake_warehouse.transform",
    }
)
EXPECTED_DATA_ADDRESSES = frozenset(
    {
        "data.aws_iam_policy_document.snowflake_s3_assume_role",
        "data.aws_iam_policy_document.snowflake_s3_read",
    }
)

_SUMMARY = re.compile(
    r"(?m)^Plan: (?P<add>\d+) to add, (?P<change>\d+) to change, "
    r"(?P<destroy>\d+) to destroy\.$"
)
_NO_CHANGES = re.compile(
    r"(?m)^No changes\. (?:Your infrastructure matches the configuration|"
    r"Infrastructure is up-to-date)\.$"
)
_OUTPUT_CHANGES = re.compile(r"(?m)^Changes to Outputs:$")
_ADDRESS = (
    r"(?:module\.[A-Za-z0-9_-]+(?:\[(?:\d+|\"[A-Za-z0-9_-]+\")\])?\.)*"
    r"(?:data\.)?[A-Za-z0-9_]+\.[A-Za-z0-9_-]+"
    r"(?:\[(?:\d+|\"[A-Za-z0-9_-]+\")\])?"
)
_ACTION_HEADER = re.compile(
    rf"(?m)^\s*#\s+(?P<address>{_ADDRESS})\s+"
    r"(?P<description>will be created|will be updated in-place|"
    r"will be destroyed|must be replaced|will be replaced)$"
)
_ACTION_NAMES = {
    "will be created": "CREATE",
    "will be updated in-place": "UPDATE",
    "will be destroyed": "DESTROY",
    "must be replaced": "REPLACE",
    "will be replaced": "REPLACE",
}


class DriftClassification(StrEnum):
    """Stable operational outcomes for one Terraform plan."""

    CLEAN = "CLEAN"
    DRIFT_DETECTED = "DRIFT_DETECTED"
    PLAN_ERROR = "PLAN_ERROR"


class DiagnosticCategory(StrEnum):
    """Value-free error categories safe to report."""

    NONE = "NONE"
    AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
    BACKEND_ERROR = "BACKEND_ERROR"
    GIT_CHECKOUT_ERROR = "GIT_CHECKOUT_ERROR"
    MALFORMED_RESULT = "MALFORMED_RESULT"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    RESOURCE_SCOPE_ERROR = "RESOURCE_SCOPE_ERROR"
    STATE_LOCK_ERROR = "STATE_LOCK_ERROR"
    TERRAFORM_ERROR = "TERRAFORM_ERROR"
    VERSION_ERROR = "VERSION_ERROR"
    WAREHOUSE_SAFETY_ERROR = "WAREHOUSE_SAFETY_ERROR"


class SafeCheckError(RuntimeError):
    """Fail closed without retaining unsafe subprocess output."""

    def __init__(self, category: DiagnosticCategory) -> None:
        self.category = category
        super().__init__(category.value)


@dataclass(frozen=True)
class CommandResult:
    """Privately captured subprocess result."""

    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandRunner(Protocol):
    """Injectable subprocess boundary used by synthetic tests."""

    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> CommandResult: ...


class SubprocessRunner:
    """Capture command output privately and never raise it to callers."""

    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> CommandResult:
        try:
            completed = subprocess.run(
                tuple(command),
                cwd=cwd,
                env=dict(env) if env is not None else None,
                capture_output=True,
                text=True,
                check=False,
                timeout=900,
            )
        except (OSError, subprocess.SubprocessError):
            raise SafeCheckError(DiagnosticCategory.TERRAFORM_ERROR) from None
        return CommandResult(completed.returncode, completed.stdout, completed.stderr)


@dataclass(frozen=True)
class ResourceAction:
    """Value-free Terraform action attached to a validated address."""

    address: str
    action: str


@dataclass(frozen=True)
class DriftResult:
    """Sanitized interpretation of one Terraform plan."""

    classification: DriftClassification
    terraform_exit_code: int
    add_count: int = 0
    change_count: int = 0
    destroy_count: int = 0
    resource_actions: tuple[ResourceAction, ...] = ()
    output_changes: bool = False
    diagnostic: DiagnosticCategory = DiagnosticCategory.NONE
    git_sha: str | None = None
    verification_status: str = "UNVERIFIED"


def _safe_error_result(
    exit_code: int,
    category: DiagnosticCategory,
) -> DriftResult:
    return DriftResult(
        classification=DriftClassification.PLAN_ERROR,
        terraform_exit_code=exit_code,
        diagnostic=category,
    )


def _diagnostic_category(stderr: str) -> DiagnosticCategory:
    """Classify known errors internally without returning their text."""
    folded = stderr.casefold()
    if "error acquiring the state lock" in folded:
        return DiagnosticCategory.STATE_LOCK_ERROR
    if any(marker in folded for marker in ("jwt", "authentication", "credential")):
        return DiagnosticCategory.AUTHENTICATION_ERROR
    if any(marker in folded for marker in ("failed to load backend", "backend initialization")):
        return DiagnosticCategory.BACKEND_ERROR
    if "provider" in folded:
        return DiagnosticCategory.PROVIDER_ERROR
    return DiagnosticCategory.TERRAFORM_ERROR


def _resource_actions(stdout: str) -> tuple[ResourceAction, ...] | None:
    """Extract only syntactically validated addresses and action names."""
    actions = tuple(
        ResourceAction(
            address=match.group("address"),
            action=_ACTION_NAMES[match.group("description")],
        )
        for match in _ACTION_HEADER.finditer(stdout)
    )
    raw_headers = len(re.findall(r"(?m)^\s*#\s+\S+\s+(?:will|must)\b", stdout))
    if raw_headers != len(actions):
        return None
    return actions


def interpret_plan_result(exit_code: int, stdout: str, stderr: str) -> DriftResult:
    """Interpret Terraform output without copying any raw value into the result."""
    if exit_code not in {0, 1, 2}:
        return _safe_error_result(exit_code, DiagnosticCategory.MALFORMED_RESULT)
    if exit_code == 1:
        return _safe_error_result(exit_code, _diagnostic_category(stderr))

    actions = _resource_actions(stdout)
    if actions is None:
        return _safe_error_result(exit_code, DiagnosticCategory.MALFORMED_RESULT)
    summary_matches = tuple(_SUMMARY.finditer(stdout))
    if len(summary_matches) > 1:
        return _safe_error_result(exit_code, DiagnosticCategory.MALFORMED_RESULT)
    summary = summary_matches[0] if summary_matches else None
    output_changes = bool(_OUTPUT_CHANGES.search(stdout))
    no_changes = bool(_NO_CHANGES.search(stdout))

    counts = (0, 0, 0)
    if summary is not None:
        counts = tuple(
            int(summary.group(name)) for name in ("add", "change", "destroy")
        )
    add_count, change_count, destroy_count = counts
    resource_change_count = add_count + change_count + destroy_count

    if exit_code == 0:
        if (
            not no_changes
            or summary is not None
            or actions
            or output_changes
        ):
            return _safe_error_result(exit_code, DiagnosticCategory.MALFORMED_RESULT)
        return DriftResult(
            classification=DriftClassification.CLEAN,
            terraform_exit_code=0,
            verification_status="VERIFIED",
        )

    if no_changes or (summary is None and not output_changes):
        return _safe_error_result(exit_code, DiagnosticCategory.MALFORMED_RESULT)
    if resource_change_count and not actions:
        return _safe_error_result(exit_code, DiagnosticCategory.MALFORMED_RESULT)
    if not resource_change_count and not output_changes:
        return _safe_error_result(exit_code, DiagnosticCategory.MALFORMED_RESULT)
    return DriftResult(
        classification=DriftClassification.DRIFT_DETECTED,
        terraform_exit_code=2,
        add_count=add_count,
        change_count=change_count,
        destroy_count=destroy_count,
        resource_actions=actions,
        output_changes=output_changes,
        verification_status="VERIFIED",
    )


def validate_plan_command(command: Sequence[str]) -> None:
    """Require the single approved full-plan command exactly."""
    if tuple(command) != PLAN_COMMAND:
        raise SafeCheckError(DiagnosticCategory.TERRAFORM_ERROR)
    forbidden = {"-target", "-refresh=false", "-lock=false", "-out"}
    if any(
        item == option or item.startswith(f"{option}=")
        for item in command
        for option in forbidden
    ):
        raise SafeCheckError(DiagnosticCategory.TERRAFORM_ERROR)


def validate_git_checkout(runner: CommandRunner, expected_sha: str) -> str:
    """Require the reviewed develop commit and a clean checkout."""
    checks = {
        ("git", "branch", "--show-current"): EXPECTED_BRANCH,
        ("git", "rev-parse", "HEAD"): expected_sha,
        ("git", "status", "--porcelain"): "",
    }
    for command, expected in checks.items():
        result = runner.run(command, cwd=PROJECT_ROOT)
        if result.returncode != 0 or result.stdout.strip() != expected:
            raise SafeCheckError(DiagnosticCategory.GIT_CHECKOUT_ERROR)
    return expected_sha


def validate_terraform_versions(runner: CommandRunner) -> None:
    """Verify Terraform and locked provider versions without exposing output."""
    version = runner.run(("terraform", "version", "-json"), cwd=PRODUCTION_ROOT)
    try:
        terraform_version = json.loads(version.stdout)["terraform_version"]
    except (json.JSONDecodeError, KeyError, TypeError):
        raise SafeCheckError(DiagnosticCategory.VERSION_ERROR) from None
    if version.returncode != 0 or terraform_version != EXPECTED_TERRAFORM_VERSION:
        raise SafeCheckError(DiagnosticCategory.VERSION_ERROR)

    lockfile = (PRODUCTION_ROOT / ".terraform.lock.hcl").read_text()
    expected = {
        "registry.terraform.io/hashicorp/aws": EXPECTED_AWS_PROVIDER_VERSION,
        "registry.terraform.io/snowflakedb/snowflake": (
            EXPECTED_SNOWFLAKE_PROVIDER_VERSION
        ),
    }
    for source, provider_version in expected.items():
        pattern = (
            rf'provider\s+"{re.escape(source)}"\s*\{{.*?'
            rf'version\s*=\s*"{re.escape(provider_version)}"'
        )
        if not re.search(pattern, lockfile, re.DOTALL):
            raise SafeCheckError(DiagnosticCategory.VERSION_ERROR)


def _backend_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for key, value in re.findall(r'(?m)^\s*(bucket|key|region|encrypt)\s*=\s*(.+?)\s*$', path.read_text()):
        values[key] = value.strip().strip('"')
    return values


def validate_backend_and_state(
    runner: CommandRunner,
    child_env: Mapping[str, str],
) -> None:
    """Verify backend identity, workspace, and exact state address boundary."""
    values = _backend_values(PRODUCTION_ROOT / "backend.hcl")
    if values != {
        "bucket": EXPECTED_BACKEND_BUCKET,
        "key": EXPECTED_BACKEND_KEY,
        "region": EXPECTED_AWS_REGION,
        "encrypt": "true",
    }:
        raise SafeCheckError(DiagnosticCategory.BACKEND_ERROR)
    if "use_lockfile = true" not in (PRODUCTION_ROOT / "backend.tf").read_text():
        raise SafeCheckError(DiagnosticCategory.BACKEND_ERROR)

    workspace = runner.run(
        ("terraform", "workspace", "show"), cwd=PRODUCTION_ROOT, env=child_env
    )
    if workspace.returncode != 0 or workspace.stdout.strip() != EXPECTED_WORKSPACE:
        raise SafeCheckError(DiagnosticCategory.BACKEND_ERROR)
    state = runner.run(
        ("terraform", "state", "list"), cwd=PRODUCTION_ROOT, env=child_env
    )
    if state.returncode != 0:
        raise SafeCheckError(DiagnosticCategory.BACKEND_ERROR)
    addresses = frozenset(line.strip() for line in state.stdout.splitlines() if line.strip())
    if addresses != EXPECTED_MANAGED_ADDRESSES | EXPECTED_DATA_ADDRESSES:
        raise SafeCheckError(DiagnosticCategory.RESOURCE_SCOPE_ERROR)

    validate_no_state_lock(runner, child_env)


def validate_no_state_lock(
    runner: CommandRunner,
    child_env: Mapping[str, str],
) -> None:
    """Require the native S3 lock object to be absent."""

    lock = runner.run(
        (
            "aws",
            "s3api",
            "head-object",
            "--bucket",
            EXPECTED_BACKEND_BUCKET,
            "--key",
            f"{EXPECTED_BACKEND_KEY}.tflock",
            "--profile",
            EXPECTED_AWS_PROFILE,
        ),
        cwd=PROJECT_ROOT,
        env=child_env,
    )
    if lock.returncode == 0:
        raise SafeCheckError(DiagnosticCategory.STATE_LOCK_ERROR)
    lock_error = lock.stderr.casefold()
    if not any(marker in lock_error for marker in ("404", "not found", "nosuchkey")):
        raise SafeCheckError(DiagnosticCategory.BACKEND_ERROR)


def _external_ids_from_role(document: object) -> set[str]:
    if isinstance(document, str):
        document = json.loads(urllib.parse.unquote(document))
    if not isinstance(document, dict):
        return set()
    statements = document.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    values: set[str] = set()
    for statement in statements:
        if not isinstance(statement, dict):
            continue
        actions = statement.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        if "sts:AssumeRole" not in actions:
            continue
        condition = statement.get("Condition", {})
        for operator in ("StringEquals", "StringLike"):
            fields = condition.get(operator, {}) if isinstance(condition, dict) else {}
            external = fields.get("sts:ExternalId") if isinstance(fields, dict) else None
            if isinstance(external, str) and external:
                values.add(external)
            elif isinstance(external, list):
                values.update(item for item in external if isinstance(item, str) and item)
    return values


def prepare_production_environment(
    runner: CommandRunner,
    process_env: Mapping[str, str],
) -> dict[str, str]:
    """Validate identities and inject the existing external ID in memory only."""
    if process_env.get("AWS_PROFILE") != EXPECTED_AWS_PROFILE:
        raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR)
    if process_env.get("SNOWFLAKE_ROLE") != EXPECTED_SNOWFLAKE_ROLE:
        raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR)
    if process_env.get("SNOWFLAKE_WAREHOUSE"):
        raise SafeCheckError(DiagnosticCategory.WAREHOUSE_SAFETY_ERROR)
    if process_env.get("SNOWFLAKE_PASSWORD"):
        raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR)
    if process_env.get("SNOWFLAKE_AUTHENTICATOR", "SNOWFLAKE_JWT") != "SNOWFLAKE_JWT":
        raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR)
    for required in (
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PRIVATE_KEY",
        "SNOWFLAKE_ORGANIZATION_NAME",
        "SNOWFLAKE_ACCOUNT_NAME",
    ):
        if not process_env.get(required):
            raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR)

    identity = runner.run(
        (
            "aws",
            "sts",
            "get-caller-identity",
            "--profile",
            EXPECTED_AWS_PROFILE,
            "--output",
            "json",
        ),
        cwd=PROJECT_ROOT,
        env=process_env,
    )
    try:
        identity_values = json.loads(identity.stdout)
        account = str(identity_values["Account"])
        arn = str(identity_values["Arn"])
    except (json.JSONDecodeError, KeyError, TypeError):
        raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR) from None
    if (
        identity.returncode != 0
        or account != EXPECTED_AWS_ACCOUNT
        or arn != EXPECTED_AWS_ARN
    ):
        raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR)

    region = runner.run(
        (
            "aws",
            "configure",
            "get",
            "region",
            "--profile",
            EXPECTED_AWS_PROFILE,
        ),
        cwd=PROJECT_ROOT,
        env=process_env,
    )
    if region.returncode != 0 or region.stdout.strip() != EXPECTED_AWS_REGION:
        raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR)

    role = runner.run(
        (
            "aws",
            "iam",
            "get-role",
            "--role-name",
            "snowflake-blockchain-s3-role",
            "--profile",
            EXPECTED_AWS_PROFILE,
            "--output",
            "json",
        ),
        cwd=PROJECT_ROOT,
        env=process_env,
    )
    try:
        document = json.loads(role.stdout)["Role"]["AssumeRolePolicyDocument"]
        external_ids = _external_ids_from_role(document)
    except (json.JSONDecodeError, KeyError, TypeError):
        raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR) from None
    if role.returncode != 0 or len(external_ids) != 1:
        raise SafeCheckError(DiagnosticCategory.AUTHENTICATION_ERROR)

    child_env = dict(process_env)
    child_env["TF_VAR_snowflake_storage_external_id"] = next(iter(external_ids))
    child_env["TF_IN_AUTOMATION"] = "true"
    child_env["SNOWFLAKE_AUTHENTICATOR"] = "SNOWFLAKE_JWT"
    return child_env


class WarehouseSafetyProbe:
    """Read-only Snowflake verification with no selected warehouse."""

    def verify(self, process_env: Mapping[str, str]) -> None:
        try:
            from cryptography.hazmat.primitives import serialization
            import snowflake.connector

            password = process_env.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
            key = serialization.load_pem_private_key(
                process_env["SNOWFLAKE_PRIVATE_KEY"].encode(),
                password=password.encode() if password else None,
            )
            private_key = key.private_bytes(
                serialization.Encoding.DER,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            connection = snowflake.connector.connect(
                account=process_env["SNOWFLAKE_ACCOUNT"],
                user=process_env["SNOWFLAKE_USER"],
                role=EXPECTED_SNOWFLAKE_ROLE,
                warehouse=None,
                authenticator="SNOWFLAKE_JWT",
                private_key=private_key,
            )
            try:
                cursor = connection.cursor()
                try:
                    cursor.execute("SELECT CURRENT_WAREHOUSE()")
                    current = cursor.fetchone()
                    cursor.execute(f"SHOW WAREHOUSES LIKE '{EXPECTED_WAREHOUSE}'")
                    columns = [item[0].lower() for item in cursor.description]
                    rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
                finally:
                    cursor.close()
            finally:
                connection.close()
        except Exception:
            raise SafeCheckError(DiagnosticCategory.WAREHOUSE_SAFETY_ERROR) from None
        if current != (None,) or len(rows) != 1 or rows[0].get("state") != "SUSPENDED":
            raise SafeCheckError(DiagnosticCategory.WAREHOUSE_SAFETY_ERROR)


def run_production_check(
    *,
    expected_git_sha: str,
    runner: CommandRunner,
    warehouse_probe: WarehouseSafetyProbe,
    process_env: Mapping[str, str],
) -> DriftResult:
    """Run one future-authorized production plan with pre/post safety checks."""
    git_sha = validate_git_checkout(runner, expected_git_sha)
    validate_terraform_versions(runner)
    child_env = prepare_production_environment(runner, process_env)
    validate_backend_and_state(runner, child_env)
    warehouse_probe.verify(process_env)
    validate_plan_command(PLAN_COMMAND)

    plan_error: SafeCheckError | None = None
    plan: CommandResult | None = None
    try:
        plan = runner.run(PLAN_COMMAND, cwd=PRODUCTION_ROOT, env=child_env)
    except SafeCheckError as error:
        plan_error = error
    postflight_error: DiagnosticCategory | None = None
    try:
        validate_no_state_lock(runner, child_env)
        warehouse_probe.verify(process_env)
    except SafeCheckError as error:
        postflight_error = error.category
    if postflight_error is not None:
        return DriftResult(
            classification=DriftClassification.PLAN_ERROR,
            terraform_exit_code=plan.returncode if plan is not None else -1,
            diagnostic=postflight_error,
            git_sha=git_sha,
        )
    if plan_error is not None:
        raise plan_error
    if plan is None:
        raise SafeCheckError(DiagnosticCategory.TERRAFORM_ERROR)
    result = interpret_plan_result(plan.returncode, plan.stdout, plan.stderr)
    return DriftResult(
        **{**result.__dict__, "git_sha": git_sha},
    )


def format_result(result: DriftResult) -> str:
    """Render only allowlisted fields from the sanitized result."""
    lines = [
        f"classification={result.classification.value}",
        f"terraform_exit_code={result.terraform_exit_code}",
        f"add={result.add_count}",
        f"change={result.change_count}",
        f"destroy={result.destroy_count}",
        f"output_changes={str(result.output_changes).lower()}",
        f"diagnostic={result.diagnostic.value}",
        f"verification_status={result.verification_status}",
    ]
    if result.git_sha is not None:
        lines.append(f"git_sha={result.git_sha}")
    lines.extend(
        f"resource={action.address} action={action.action}"
        for action in result.resource_actions
    )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Default safely; require explicit future authorization to contact production."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--execute-production",
        action="store_true",
        help="run one authenticated production plan after all safety checks",
    )
    parser.add_argument(
        "--expected-git-sha",
        help="exact reviewed Git SHA required for production execution",
    )
    args = parser.parse_args(argv)
    if not args.execute_production:
        print("production_execution=DISABLED")
        return 0
    if not args.expected_git_sha or not re.fullmatch(r"[0-9a-f]{40}", args.expected_git_sha):
        print(format_result(_safe_error_result(-1, DiagnosticCategory.GIT_CHECKOUT_ERROR)))
        return 1

    try:
        result = run_production_check(
            expected_git_sha=args.expected_git_sha,
            runner=SubprocessRunner(),
            warehouse_probe=WarehouseSafetyProbe(),
            process_env=os.environ,
        )
    except SafeCheckError as error:
        result = _safe_error_result(-1, error.category)
    print(format_result(result))
    if result.classification is DriftClassification.CLEAN:
        return 0
    if result.classification is DriftClassification.DRIFT_DETECTED:
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
