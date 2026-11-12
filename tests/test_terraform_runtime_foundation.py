"""Offline contracts for the independently stateful AWS runtime root."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TERRAFORM_ROOT = PROJECT_ROOT / "infra" / "terraform"
RUNTIME_ROOT = TERRAFORM_ROOT / "environments" / "runtime"
PROD_ROOT = TERRAFORM_ROOT / "environments" / "prod"


def _runtime_text() -> str:
    return "\n".join(
        path.read_text() for path in sorted(RUNTIME_ROOT.glob("*.tf"))
    )


def _resource_types(terraform: str) -> list[str]:
    return re.findall(r'(?m)^resource\s+"([^"]+)"', terraform)


def _iam_statement(iam: str, sid: str) -> str:
    sid_match = re.search(rf'sid\s*=\s*"{re.escape(sid)}"', iam)
    assert sid_match is not None
    sid_position = sid_match.start()
    statement_start = iam.rfind("  statement {", 0, sid_position)
    statement_end = iam.find("\n  statement {", sid_position)
    if statement_end == -1:
        statement_end = len(iam)
    return iam[statement_start:statement_end]


def _is_ignored(path: Path) -> bool:
    result = subprocess.run(
        ("git", "check-ignore", "--quiet", str(path.relative_to(PROJECT_ROOT))),
        cwd=PROJECT_ROOT,
        check=False,
    )
    return result.returncode == 0


def test_runtime_root_is_separate_and_uses_only_aws() -> None:
    assert RUNTIME_ROOT.is_dir()

    versions = (RUNTIME_ROOT / "versions.tf").read_text()
    assert 'required_version = ">= 1.10.0, < 2.0.0"' in versions
    assert 'source  = "hashicorp/aws"' in versions
    assert 'version = "~> 6.69.0"' in versions
    assert "snowflake" not in versions.lower()

    backend = (RUNTIME_ROOT / "backend.tf").read_text()
    example = (RUNTIME_ROOT / "backend.hcl.example").read_text()
    assert 'backend "s3"' in backend
    assert "use_lockfile = true" in backend
    assert "production/runtime/terraform.tfstate" in example
    assert "production/platform/terraform.tfstate" not in example
    assert "johnson-blockchain-analytics-prod" not in example
    assert not _is_ignored(RUNTIME_ROOT / "backend.hcl.example")
    assert _is_ignored(RUNTIME_ROOT / "backend.hcl")


def test_runtime_does_not_claim_existing_platform_resources() -> None:
    runtime = _runtime_text()
    production = "\n".join(
        path.read_text() for path in sorted(PROD_ROOT.glob("*.tf"))
    )

    assert len(_resource_types(production)) == 43
    for resource_type in (
        "aws_s3_bucket",
        "aws_s3_bucket_versioning",
        "aws_s3_bucket_server_side_encryption_configuration",
        "aws_s3_bucket_public_access_block",
        "aws_s3_bucket_ownership_controls",
        "snowflake_warehouse",
        "snowflake_database",
        "snowflake_schema",
    ):
        assert not re.search(rf'resource\s+"{resource_type}"', runtime)

    assert not re.search(r"(?m)^\s*import\s*\{", runtime)


def test_network_has_no_ingress_or_unnecessary_paid_services() -> None:
    networking = (RUNTIME_ROOT / "networking.tf").read_text()
    runtime = _runtime_text().lower()

    assert 'resource "aws_vpc" "runtime"' in networking
    assert 'resource "aws_subnet" "public"' in networking
    assert "map_public_ip_on_launch = true" in networking
    assert 'resource "aws_internet_gateway" "runtime"' in networking
    assert 'cidr_block = "0.0.0.0/0"' in networking

    assert "aws_vpc_security_group_ingress_rule" not in runtime
    assert not re.search(r"(?m)^\s*ingress\s*\{", runtime)
    for forbidden in (
        "aws_nat_gateway",
        "aws_eip",
        "aws_lb",
        "aws_msk",
        "aws_mwaa",
        "aws_db_instance",
        "aws_emr",
    ):
        assert forbidden not in runtime


def test_instance_requires_imdsv2_has_no_ssh_key_and_controls_cost() -> None:
    compute = (RUNTIME_ROOT / "compute.tf").read_text()

    for expected in (
        'instance_type               = var.instance_type',
        'http_tokens                 = "required"',
        'instance_initiated_shutdown_behavior = "stop"',
        "disable_api_termination              = true",
        'cpu_credits = "standard"',
        "encrypted             = true",
        "delete_on_termination = true",
        "prevent_destroy = true",
        "user_data_replace_on_change = false",
    ):
        assert expected in compute

    for excluded in (
        "key_name",
        "user_data_base64",
        "password_data",
        "source_dest_check = false",
    ):
        assert excluded not in compute


def test_kafka_volume_is_encrypted_persistent_gp3() -> None:
    storage = (RUNTIME_ROOT / "storage.tf").read_text()
    compute = (RUNTIME_ROOT / "compute.tf").read_text()

    for expected in (
        'resource "aws_ebs_volume" "kafka"',
        'type              = "gp3"',
        "encrypted         = true",
        "size              = var.kafka_volume_size_gib",
        "prevent_destroy = true",
        'State     = "persistent"',
        'Retention = "72-hours"',
    ):
        assert expected in storage

    assert 'resource "aws_volume_attachment" "kafka"' in compute
    assert 'device_name                    = "/dev/sdf"' in compute
    assert "stop_instance_before_detaching = true" in compute


def test_bootstrap_never_formats_a_volume_and_starts_no_workload() -> None:
    user_data = (RUNTIME_ROOT / "templates" / "user_data.sh.tftpl").read_text()

    assert 'filesystem_probe="$(blkid -p -o export' in user_data
    assert 'if [[ "$probe_key" == "TYPE" ]]' in user_data
    assert 'if [[ -z "$filesystem_type" ]]' in user_data
    assert 'elif [[ "$filesystem_type" != "xfs" ]]' in user_data
    assert "sha256sum --check --status" in user_data
    assert "mkfs" not in user_data
    assert "wipefs" not in user_data

    for safe_status in (
        "EBS_VOLUME_MISSING_OR_DETACHED",
        "EBS_VOLUME_IDENTITY_MISMATCH",
        "EBS_INITIALIZATION_REQUIRED",
        "EBS_FILESYSTEM_UNRECOGNIZED_OR_DAMAGED",
        "EBS_FILESYSTEM_UNEXPECTED",
        "EBS_XFS_UUID_MISSING",
        "EBS_MOUNT_IDENTITY_MISMATCH",
        "EBS_MOUNT_FAILED",
        "EBS_MOUNT_VERIFIED",
    ):
        assert safe_status in user_data

    assert 'device_serial="$(lsblk -ndo SERIAL' in user_data
    assert 'mountpoint --quiet "${kafka_mount_point}"' in user_data
    assert 'findmnt --noheadings --output UUID' in user_data
    assert 'fstab_entry="UUID=$volume_uuid' in user_data

    for forbidden in (
        "docker compose up",
        "docker run",
        "secretsmanager",
        "SNOWFLAKE_PRIVATE_KEY",
        "ALCHEMY_",
        "aws_access_key_id",
        "aws_secret_access_key",
    ):
        assert forbidden.lower() not in user_data.lower()


def test_runtime_iam_is_prefix_scoped_and_denies_state_access() -> None:
    iam = (RUNTIME_ROOT / "iam.tf").read_text()

    for expected in (
        '"GetPilotBucketLocation"',
        '"ListOnlyPilotData"',
        '"ReadWriteOnlyPilotData"',
        '"${var.pilot_s3_prefix}/*"',
        '"DenyTerraformStateBucketMetadata"',
        '"DenyTerraformStateObjects"',
        'effect = "Deny"',
        'actions   = ["secretsmanager:GetSecretValue"]',
        "aws_ecr_repository.producer.arn",
        "aws_ecr_repository.spark.arn",
        'policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"',
    ):
        assert expected in iam

    for forbidden in (
        '"s3:*"',
        '"iam:*"',
        '"secretsmanager:*"',
        '"logs:*"',
        'actions   = ["*"]',
        "snowflake-blockchain-s3-role",
    ):
        assert forbidden not in iam

    location_statement = _iam_statement(iam, "GetPilotBucketLocation")
    assert 'actions   = ["s3:GetBucketLocation"]' in location_statement
    assert 'resources = ["arn:aws:s3:::${var.data_bucket_name}"]' in location_statement
    assert "s3:prefix" not in location_statement
    assert "s3:ListBucket" not in location_statement

    list_statement = _iam_statement(iam, "ListOnlyPilotData")
    assert 'actions   = ["s3:ListBucket"]' in list_statement
    assert 'variable = "s3:prefix"' in list_statement
    assert '"${var.pilot_s3_prefix}/*"' in list_statement
    assert "s3:GetBucketLocation" not in list_statement

    variables = (RUNTIME_ROOT / "variables.tf").read_text()
    secret_variable = re.search(
        r'variable "approved_secret_arns"\s*\{(?P<body>.*?)\n\}',
        variables,
        re.DOTALL,
    )
    assert secret_variable is not None
    assert "sensitive   = true" in secret_variable.group("body")
    assert "default" not in secret_variable.group("body")


def test_pilot_paths_cannot_target_production_bronze() -> None:
    variables = (RUNTIME_ROOT / "variables.tf").read_text()
    outputs = (RUNTIME_ROOT / "outputs.tf").read_text()

    assert 'default     = "blockchain-analytics/pilot/runtime"' in variables
    assert 'startswith(var.pilot_s3_prefix, "blockchain-analytics/pilot/")' in variables
    assert '${var.pilot_s3_prefix}/bronze/' in outputs
    assert '${var.pilot_s3_prefix}/checkpoints/' in outputs
    assert "blockchain-analytics/production/bronze" not in _runtime_text().lower()


def test_observability_retention_and_stop_only_schedule_are_bounded() -> None:
    observability = (RUNTIME_ROOT / "observability.tf").read_text()
    variables = (RUNTIME_ROOT / "variables.tf").read_text()

    assert 'name              = "/blockchain/runtime/containers"' in observability
    assert "retention_in_days = var.log_retention_days" in observability
    assert 'actions   = ["ec2:StopInstances"]' in observability
    assert "aws_scheduler_schedule" in observability
    assert "var.enable_scheduled_shutdown ? 1 : 0" in observability
    assert 'default     = false' in variables
    assert "ec2:StartInstances" not in observability


def test_images_are_immutable_scanned_and_lifecycle_managed() -> None:
    ecr = (RUNTIME_ROOT / "ecr.tf").read_text()

    assert ecr.count('image_tag_mutability = "IMMUTABLE"') == 2
    assert ecr.count("scan_on_push = true") == 2
    assert ecr.count('encryption_type = "AES256"') == 2
    assert ecr.count('resource "aws_ecr_lifecycle_policy"') == 2
    assert 'tagStatus   = "untagged"' in ecr
    assert "countNumber = 14" in ecr


def test_operations_contract_covers_recovery_and_cost_boundaries() -> None:
    operations = (RUNTIME_ROOT / "OPERATIONS.md").read_text()

    for expected in (
        "Exactly one Spark writer",
        "startingOffsets=latest",
        "production/runtime/terraform.tfstate",
        "production/platform/terraform.tfstate",
        "Stop producer ingress",
        "Stop Kafka last",
        "72 hours",
        "delayed alerts rather than hard spending caps",
        "starts no",
        "application containers",
        "production Bronze paths",
        "forbidden during the pilot",
    ):
        assert expected in operations

    assert "AWS Budgets" in operations
    assert "Cost Anomaly Detection" in operations
    assert "StartInstances" in operations
    assert "disabled by default" in operations


def test_one_time_initialization_is_separate_and_fail_closed() -> None:
    operations = (RUNTIME_ROOT / "OPERATIONS.md").read_text()
    user_data = (RUNTIME_ROOT / "templates" / "user_data.sh.tftpl").read_text()

    for expected in (
        "One-time Kafka volume initialization",
        "Normal user data never formats a block device",
        "EBS_INITIALIZATION_REQUIRED",
        "explicit operator approval",
        "not the root device",
        "not mounted anywhere",
        "wipefs --no-act",
        "recognized filesystem alone is not proof",
        "must never be invoked automatically",
    ):
        assert expected in operations

    assert "mkfs.xfs" in operations
    assert "mkfs" not in user_data


def test_shared_container_iam_boundary_is_explicit() -> None:
    operations = (RUNTIME_ROOT / "OPERATIONS.md").read_text()
    compute = (RUNTIME_ROOT / "compute.tf").read_text()

    for expected in (
        "IMDSv2 is required",
        "hop limit is two",
        "shared trust boundary",
        "do not have independent IAM roles",
        "Do not expose the Docker socket",
        "explicit denial for the Terraform state",
        "ECS task roles",
    ):
        assert expected in operations

    assert 'http_tokens                 = "required"' in compute
    assert "http_put_response_hop_limit = 2" in compute


def test_make_ci_terraform_validates_runtime_without_backend() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()

    assert "infra/terraform/environments/runtime" in makefile
    assert "init -backend=false -input=false" in makefile
    assert "terraform -chdir=$$root validate" in makefile


def test_runtime_source_has_no_plaintext_secret_assignments() -> None:
    runtime = _runtime_text()
    user_data = (RUNTIME_ROOT / "templates" / "user_data.sh.tftpl").read_text()
    source = f"{runtime}\n{user_data}"

    for setting in (
        "access_key",
        "secret_key",
        "session_token",
        "password",
        "private_key",
    ):
        assert not re.search(rf"(?m)^\s*{setting}\s*=", source)
