"""Contracts for the non-applying Terraform foundation."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TERRAFORM_ROOT = PROJECT_ROOT / "infra" / "terraform"
BOOTSTRAP_ROOT = TERRAFORM_ROOT / "bootstrap" / "state"
PROD_ROOT = TERRAFORM_ROOT / "environments" / "prod"


def _terraform_text(root: Path) -> str:
    return "\n".join(path.read_text() for path in sorted(root.glob("*.tf")))


def _is_ignored(path: Path) -> bool:
    result = subprocess.run(
        ("git", "check-ignore", "--quiet", str(path.relative_to(PROJECT_ROOT))),
        cwd=PROJECT_ROOT,
        check=False,
    )
    return result.returncode == 0


def test_expected_terraform_roots_and_versions_exist() -> None:
    assert BOOTSTRAP_ROOT.is_dir()
    assert PROD_ROOT.is_dir()

    for root in (BOOTSTRAP_ROOT, PROD_ROOT):
        versions = (root / "versions.tf").read_text()
        assert re.search(
            r'required_version\s*=\s*">= 1\.10\.0, < 2\.0\.0"',
            versions,
        )


def test_official_provider_sources_are_bounded() -> None:
    bootstrap = (BOOTSTRAP_ROOT / "versions.tf").read_text()
    production = (PROD_ROOT / "versions.tf").read_text()

    assert re.search(r'source\s*=\s*"hashicorp/aws"', bootstrap)
    assert re.search(r'version\s*=\s*"~> 6\.69\.0"', bootstrap)
    assert re.search(r'source\s*=\s*"hashicorp/aws"', production)
    assert re.search(r'version\s*=\s*"~> 6\.69\.0"', production)
    assert re.search(r'source\s*=\s*"snowflakedb/snowflake"', production)
    assert re.search(r'version\s*=\s*"~> 2\.22\.0"', production)


def test_state_artifacts_are_ignored_but_provider_locks_are_not() -> None:
    assert _is_ignored(PROD_ROOT / ".terraform" / "providers")
    assert _is_ignored(PROD_ROOT / "terraform.tfstate")
    assert _is_ignored(PROD_ROOT / "terraform.tfstate.backup")
    assert _is_ignored(PROD_ROOT / "backend.hcl")

    assert not _is_ignored(BOOTSTRAP_ROOT / ".terraform.lock.hcl")
    assert not _is_ignored(PROD_ROOT / ".terraform.lock.hcl")
    assert not _is_ignored(PROD_ROOT / "backend.hcl.example")


def test_production_backend_uses_native_s3_locking() -> None:
    backend = (PROD_ROOT / "backend.tf").read_text()
    example = (PROD_ROOT / "backend.hcl.example").read_text()

    assert re.search(r'backend\s+"s3"', backend)
    assert re.search(r"use_lockfile\s*=\s*true", backend)
    assert "dynamodb" not in backend.lower()
    assert "production/platform/terraform.tfstate" in example
    assert "us-west-2" in example
    assert "terraform-state" in example
    assert "data-lake" not in example


def test_bootstrap_models_required_state_bucket_controls() -> None:
    bootstrap = _terraform_text(BOOTSTRAP_ROOT)

    for resource_type in (
        "aws_s3_bucket",
        "aws_s3_bucket_versioning",
        "aws_s3_bucket_public_access_block",
        "aws_s3_bucket_server_side_encryption_configuration",
        "aws_s3_bucket_ownership_controls",
        "aws_s3_bucket_lifecycle_configuration",
    ):
        assert re.search(rf'resource\s+"{resource_type}"', bootstrap)
    assert re.search(r'sse_algorithm\s*=\s*"AES256"', bootstrap)
    assert re.search(r'status\s*=\s*"Enabled"', bootstrap)
    assert re.search(r"prevent_destroy\s*=\s*true", bootstrap)


def test_production_root_models_only_approved_aws_adoption_scope() -> None:
    production = _terraform_text(PROD_ROOT)

    expected_resources = {
        "aws_s3_bucket",
        "aws_s3_bucket_versioning",
        "aws_s3_bucket_server_side_encryption_configuration",
        "aws_s3_bucket_public_access_block",
        "aws_s3_bucket_ownership_controls",
        "aws_iam_role",
        "aws_iam_policy",
        "aws_iam_role_policy_attachment",
    }
    actual_resources = set(
        re.findall(r'(?m)^resource\s+"([^"]+)"', production)
    )

    assert actual_resources == expected_resources
    assert not re.search(r"(?m)^\s*import\s*\{", production)


def test_adopted_bucket_matches_discovered_safety_controls() -> None:
    production = _terraform_text(PROD_ROOT)

    assert 'production_data_bucket_name = "johnson-blockchain-analytics-prod"' in production
    assert re.search(r"prevent_destroy\s*=\s*true", production)
    assert re.search(r"force_destroy\s*=\s*false", production)
    assert re.search(r'status\s*=\s*"Enabled"', production)
    assert re.search(r'sse_algorithm\s*=\s*"AES256"', production)
    assert re.search(r'blocked_encryption_types\s*=\s*\["SSE-C"\]', production)
    assert re.search(r'bucket_key_enabled\s*=\s*true', production)
    assert re.search(r'object_ownership\s*=\s*"BucketOwnerEnforced"', production)

    for setting in (
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ):
        assert re.search(rf"{setting}\s*=\s*true", production)

    for unmanaged_type in (
        "aws_s3_bucket_policy",
        "aws_s3_bucket_lifecycle_configuration",
        "aws_s3_bucket_cors_configuration",
        "aws_s3_bucket_object_lock_configuration",
    ):
        assert not re.search(rf'resource\s+"{unmanaged_type}"', production)


def test_adopted_iam_scope_matches_discovery() -> None:
    production = _terraform_text(PROD_ROOT)

    assert re.search(
        r'snowflake_s3_role_name\s*=\s*"snowflake-blockchain-s3-role"',
        production,
    )
    assert re.search(
        r'snowflake_s3_policy_name\s*=\s*"snowflake-blockchain-s3-read"',
        production,
    )

    for expected in (
        'actions = ["sts:AssumeRole"]',
        'variable = "sts:ExternalId"',
        'values   = [var.snowflake_storage_external_id]',
        'sid    = "ReadBlockchainProductionObjects"',
        '"s3:GetObject"',
        '"s3:GetObjectVersion"',
        'sid    = "ListBlockchainProductionPrefix"',
        '"s3:GetBucketLocation"',
        '"s3:ListBucket"',
        'variable = "s3:prefix"',
        'values   = ["blockchain-analytics/production/*"]',
        "max_session_duration = 3600",
    ):
        assert expected in production

    assert 'resources = ["*"]' not in production
    assert 'actions   = ["*"]' not in production
    assert not re.search(r'(?i)"(?:s3|iam):\*"', production)


def test_external_id_is_required_sensitive_input() -> None:
    variables = (PROD_ROOT / "variables.tf").read_text()

    match = re.search(
        r'variable\s+"snowflake_storage_external_id"\s*\{(?P<body>.*?)\n\}',
        variables,
        re.DOTALL,
    )
    assert match is not None
    body = match.group("body")
    assert re.search(r"sensitive\s*=\s*true", body)
    assert "default" not in body


def test_adopted_untagged_resources_do_not_receive_default_tags() -> None:
    providers = (PROD_ROOT / "providers.tf").read_text()
    adoption = (PROD_ROOT / "aws_adoption.tf").read_text()

    assert re.search(r'provider\s+"aws"\s*\{\s*alias\s*=\s*"adopted"', providers)
    assert adoption.count("provider = aws.adopted") == 8
    assert "ignore_changes" not in adoption


def test_terraform_source_contains_no_credential_assignments() -> None:
    terraform = _terraform_text(BOOTSTRAP_ROOT) + _terraform_text(PROD_ROOT)
    forbidden_assignments = (
        "access_key",
        "secret_key",
        "session_token",
        "password",
        "private_key",
        "token",
    )

    for setting in forbidden_assignments:
        assert not re.search(rf"(?m)^\s*{setting}\s*=", terraform)


def test_ci_is_static_and_contains_no_plan_import_or_apply() -> None:
    workflow = (PROJECT_ROOT / ".github" / "workflows" / "ci.yaml").read_text()
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    automation = f"{workflow}\n{makefile}".lower()

    assert "make ci-terraform" in workflow
    assert "terraform fmt -check" in makefile
    assert "init -backend=false -input=false" in makefile
    assert "terraform -chdir=$$root validate" in makefile
    assert not re.search(r"terraform(?:\s+-chdir=\S+)?\s+apply\b", automation)
    assert not re.search(r"terraform(?:\s+-chdir=\S+)?\s+import\b", automation)
    assert not re.search(r"terraform(?:\s+-chdir=\S+)?\s+plan\b", automation)
