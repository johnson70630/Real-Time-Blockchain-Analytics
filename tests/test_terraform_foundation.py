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


def test_production_root_defines_no_resources_or_imports() -> None:
    production = _terraform_text(PROD_ROOT)

    assert not re.search(r"(?m)^\s*resource\s+\"", production)
    assert not re.search(r"(?m)^\s*import\s*\{", production)


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
