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


def test_production_root_models_only_approved_adoption_scope() -> None:
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
        "snowflake_account_role",
        "snowflake_database",
        "snowflake_grant_account_role",
        "snowflake_schema",
        "snowflake_warehouse",
    }
    actual_resources = set(
        re.findall(r'(?m)^resource\s+"([^"]+)"', production)
    )

    assert actual_resources == expected_resources
    assert not re.search(r"(?m)^\s*import\s*\{", production)


def test_snowflake_warehouse_matches_discovered_configuration() -> None:
    warehouse = (PROD_ROOT / "snowflake_warehouse.tf").read_text()

    for expected in (
        'resource "snowflake_warehouse" "transform"',
        'name = "BLOCKCHAIN_TRANSFORM_WH"',
        'warehouse_type = "STANDARD"',
        'warehouse_size = "XSMALL"',
        'generation     = "2"',
        "auto_suspend   = 60",
        'auto_resume    = "true"',
    ):
        assert expected in warehouse

    for excluded in (
        "initially_suspended",
        "resource_monitor",
        "snowflake_grant_ownership",
        "snowflake_grant_privileges_to_account_role",
        "ignore_changes",
    ):
        assert excluded not in warehouse


def test_snowflake_database_and_schemas_match_discovery() -> None:
    database_schemas = (
        PROD_ROOT / "snowflake_database_schemas.tf"
    ).read_text()

    assert 'resource "snowflake_database" "blockchain"' in database_schemas
    assert 'name         = "BLOCKCHAIN_ANALYTICS"' in database_schemas
    assert "is_transient = false" in database_schemas

    for resource_name, schema_name in (
        ("raw", "RAW"),
        ("staging", "STAGING"),
        ("core", "CORE"),
        ("marts", "MARTS"),
        ("control", "CONTROL"),
    ):
        assert (
            f'resource "snowflake_schema" "{resource_name}"'
            in database_schemas
        )
        assert f'name     = "{schema_name}"' in database_schemas

    assert database_schemas.count(
        "database = snowflake_database.blockchain.name"
    ) == 5
    assert database_schemas.count('is_transient        = "false"') == 5
    assert database_schemas.count(
        'with_managed_access = "false"'
    ) == 5
    assert database_schemas.count("prevent_destroy = true") == 6
    assert (
        'comment             = "Operational control state separate from '
        'analytical models"'
        in database_schemas
    )

    for excluded in (
        "data_retention_time_in_days",
        "snowflake_grant_ownership",
        "snowflake_grant_privileges_to_account_role",
        "snowflake_table",
        "snowflake_view",
        "ignore_changes",
    ):
        assert excluded not in database_schemas


def test_snowflake_account_roles_match_discovery() -> None:
    account_roles = (PROD_ROOT / "snowflake_account_roles.tf").read_text()

    expected_roles = (
        (
            "admin",
            "BLOCKCHAIN_ADMIN_ROLE",
            "Owns blockchain analytics platform infrastructure",
        ),
        (
            "de",
            "BLOCKCHAIN_DE_ROLE",
            "Human development role for blockchain data engineering",
        ),
        (
            "pipeline",
            "BLOCKCHAIN_PIPELINE_ROLE",
            "Service role for automated blockchain data pipelines",
        ),
        (
            "dbt",
            "BLOCKCHAIN_DBT_ROLE",
            "Transformation role for dbt models in the blockchain analytics "
            "platform",
        ),
        (
            "analyst",
            "BLOCKCHAIN_ANALYST_ROLE",
            "Read-only analytics role for curated blockchain datasets",
        ),
    )

    assert account_roles.count('resource "snowflake_account_role"') == 5
    assert account_roles.count("prevent_destroy = true") == 5
    for resource_name, role_name, comment in expected_roles:
        assert (
            f'resource "snowflake_account_role" "{resource_name}"'
            in account_roles
        )
        assert f'name    = "{role_name}"' in account_roles
        assert f'comment = "{comment}"' in account_roles

    for excluded in (
        "snowflake_grant_account_role",
        "snowflake_grant_privileges_to_account_role",
        "snowflake_grant_ownership",
        "snowflake_user",
        "ignore_changes",
    ):
        assert excluded not in account_roles


def test_snowflake_role_hierarchy_matches_discovery() -> None:
    hierarchy = (PROD_ROOT / "snowflake_role_hierarchy.tf").read_text()

    expected_resources = (
        "admin_to_sysadmin",
        "de_to_admin",
        "pipeline_to_admin",
        "dbt_to_admin",
        "analyst_to_admin",
    )
    expected_child_references = (
        "admin",
        "de",
        "pipeline",
        "dbt",
        "analyst",
    )

    assert hierarchy.count(
        'resource "snowflake_grant_account_role"'
    ) == 5
    for resource_name in expected_resources:
        assert (
            f'resource "snowflake_grant_account_role" "{resource_name}"'
            in hierarchy
        )
    for child_reference in expected_child_references:
        assert (
            "role_name        = snowflake_account_role."
            f"{child_reference}.fully_qualified_name"
            in hierarchy
        )

    assert 'parent_role_name = "\\\"SYSADMIN\\\""' in hierarchy
    assert hierarchy.count(
        "parent_role_name = "
        "snowflake_account_role.admin.fully_qualified_name"
    ) == 4

    for excluded in (
        "user_name",
        "snowflake_user",
        "snowflake_grant_privileges_to_account_role",
        "snowflake_grant_ownership",
    ):
        assert excluded not in hierarchy


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
