"""Offline security cases for the proposed demo role, not an AWS IAM simulator.

The small evaluator covers only the explicit operators used in these reviewed
documents. AWS policy validation, account policies, dependent-action checks and
the refreshed Terraform plan remain required before deployment.
"""

from __future__ import annotations

import fnmatch
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "docs/policies/runtime-deployer"
ACCOUNT = "209479274979"
EC2 = f"arn:aws:ec2:us-west-2:{ACCOUNT}"
IAM = f"arn:aws:iam::{ACCOUNT}"
BOUNDARY = f"{IAM}:policy/blockchain-runtime-ec2-boundary"
RUNTIME_ROLE = f"{IAM}:role/blockchain-runtime-ec2"
STATE_BUCKET = "arn:aws:s3:::blockchain-analytics-terraform-state-209479274979-usw2"
STATE = f"{STATE_BUCKET}/production/runtime/terraform.tfstate"
PILOT = "blockchain-analytics/pilot/runtime/11-6c-v1"
DATA_BUCKET = "arn:aws:s3:::johnson-blockchain-analytics-prod"
REGION = {"aws:RequestedRegion": "us-west-2"}
TAGS = {
    "Project": "real-time-blockchain-analytics",
    "Environment": "production-runtime",
    "Workload": "blockchain-runtime",
}


def _list(value):
    return value if isinstance(value, list) else [value]


def _conditions(conditions, context):
    for operator, pairs in conditions.items():
        for key, expected in pairs.items():
            actual = context.get(key)
            values = _list(expected)
            if operator == "Null":
                result = (actual is None) == (expected == "true")
            elif actual is None:
                result = False
            elif operator in {"StringEquals", "ArnEquals", "Bool"}:
                result = str(actual) in values
            elif operator == "StringLike":
                result = any(fnmatch.fnmatchcase(str(actual), v) for v in values)
            elif operator == "NumericEquals":
                result = any(float(actual) == float(v) for v in values)
            elif operator == "ForAllValues:ArnEquals":
                result = all(v in values for v in _list(actual))
            else:
                raise AssertionError(f"Unsupported test operator: {operator}")
            if not result:
                return False
    return True


def _allows(files, action, resource, context=None):
    allowed = False
    for filename in files:
        policy = json.loads((ROOT / filename).read_text())
        for statement in policy["Statement"]:
            if not any(fnmatch.fnmatchcase(action, a) for a in _list(statement["Action"])):
                continue
            if not any(
                fnmatch.fnmatchcase(resource, r)
                for r in _list(statement.get("Resource", "*"))
            ):
                continue
            if not _conditions(statement.get("Condition", {}), context or {}):
                continue
            if statement["Effect"] == "Deny":
                return False
            allowed = True
    return allowed


DEPLOY = ["deploy-control.json", "deploy-discovery-tags.json", "deploy-ec2.json"]


def test_policy_size_and_explicit_action_limits():
    for path in ROOT.glob("*.json"):
        policy = json.loads(path.read_text())
        limit = 2048 if path.name in {"trust.json", "assume-role.json"} else 6144
        assert len(json.dumps(policy, separators=(",", ":"))) <= limit
        assert policy["Version"] == "2012-10-17"
        for statement in policy["Statement"]:
            if statement["Effect"] == "Allow":
                assert not any("*" in a for a in _list(statement["Action"]))
                assert "NotAction" not in statement
                assert "NotResource" not in statement


@pytest.mark.parametrize("mfa", [None, "false", "true"])
def test_trust_requires_exact_user_and_mfa(mfa):
    context = {"aws:PrincipalArn": f"{IAM}:user/blockchain-de-local"}
    if mfa is not None:
        context["aws:MultiFactorAuthPresent"] = mfa
    trust = json.loads((ROOT / "trust.json").read_text())["Statement"][0]
    assert trust["Principal"] == {"AWS": f"{IAM}:root"}
    assert _allows(["trust.json"], "sts:AssumeRole", "*", context) == (mfa == "true")
    context["aws:PrincipalArn"] = f"{IAM}:user/defi-pipeline-user"
    assert not _allows(["trust.json"], "sts:AssumeRole", "*", context)


@pytest.mark.parametrize("boundary", [None, f"{IAM}:policy/unreviewed", BOUNDARY])
def test_runtime_role_cannot_be_created_or_expanded_without_fixed_boundary(boundary):
    context = {} if boundary is None else {"iam:PermissionsBoundary": boundary}
    for action in ("iam:CreateRole", "iam:PutRolePolicy"):
        assert _allows(DEPLOY, action, RUNTIME_ROLE, context) == (boundary == BOUNDARY)
        assert not _allows(DEPLOY, action, f"{IAM}:role/platform-role", context)


def test_cannot_change_boundary_deployer_or_users():
    for action, resource in [
        ("iam:DeleteRolePermissionsBoundary", RUNTIME_ROLE),
        ("iam:PutRolePermissionsBoundary", RUNTIME_ROLE),
        ("iam:CreatePolicyVersion", BOUNDARY),
        ("iam:SetDefaultPolicyVersion", BOUNDARY),
        ("iam:UpdateAssumeRolePolicy", RUNTIME_ROLE),
        ("iam:PutRolePolicy", f"{IAM}:role/blockchain-runtime-deployer"),
        ("iam:CreateAccessKey", f"{IAM}:user/blockchain-de-local"),
        ("iam:PutUserPolicy", f"{IAM}:user/blockchain-de-local"),
    ]:
        assert not _allows(DEPLOY, action, resource, {"iam:PermissionsBoundary": BOUNDARY})


def test_passrole_and_attachment_cannot_delegate_administrator_access():
    context = {"iam:PassedToService": "ec2.amazonaws.com"}
    assert _allows(DEPLOY, "iam:PassRole", RUNTIME_ROLE, context)
    assert not _allows(DEPLOY, "iam:PassRole", f"{IAM}:role/platform-role", context)
    assert not _allows(DEPLOY, "iam:PassRole", RUNTIME_ROLE, {"iam:PassedToService": "lambda.amazonaws.com"})
    context = {"iam:PermissionsBoundary": BOUNDARY, "iam:PolicyARN": "arn:aws:iam::aws:policy/AdministratorAccess"}
    assert not _allows(DEPLOY, "iam:AttachRolePolicy", RUNTIME_ROLE, context)


def test_runtime_state_and_lock_are_separate_from_platform_and_data():
    assert _allows(DEPLOY, "s3:PutObject", STATE)
    assert not _allows(DEPLOY, "s3:DeleteObject", STATE)
    assert _allows(DEPLOY, "s3:DeleteObject", STATE + ".tflock")
    for key in ("production/platform/terraform.tfstate", "production/other/terraform.tfstate"):
        for action in ("s3:GetObject", "s3:PutObject", "s3:DeleteObject"):
            assert not _allows(DEPLOY, action, f"{STATE_BUCKET}/{key}")
    assert not _allows(DEPLOY, "s3:GetObject", f"{DATA_BUCKET}/{PILOT}/bronze/example.parquet")
    assert not _allows(DEPLOY, "s3:ListBucket", STATE_BUCKET, {"s3:prefix": "production/platform/"})


def test_assumer_gets_only_named_role_and_nonmutating_mfa_inventory():
    files = ["assume-role.json"]
    role = f"{IAM}:role/blockchain-runtime-deployer"
    assert _allows(files, "sts:AssumeRole", role, {"aws:MultiFactorAuthPresent": "true"})
    assert not _allows(files, "sts:AssumeRole", role)
    assert not _allows(files, "sts:AssumeRole", RUNTIME_ROLE, {"aws:MultiFactorAuthPresent": "true"})
    assert _allows(files, "iam:ListMFADevices", f"{IAM}:user/blockchain-de-local")
    assert not _allows(files, "iam:ListMFADevices", f"{IAM}:user/defi-pipeline-user")
    assert _allows(files, "iam:ListVirtualMFADevices", "*")
    for action in ("iam:CreateVirtualMFADevice", "iam:DeleteVirtualMFADevice", "iam:EnableMFADevice", "iam:DeactivateMFADevice"):
        assert not _allows(files, action, f"{IAM}:user/blockchain-de-local")


def test_cannot_relabel_existing_resources_to_obtain_tag_scoped_permissions():
    resource = f"{EC2}:vpc/vpc-existing"
    assert not _allows(DEPLOY, "ec2:CreateTags", resource, REGION)
    assert not _allows(DEPLOY, "ec2:ModifyVpcAttribute", resource, REGION)
    assert not _allows(DEPLOY, "ec2:DescribeInstanceAttribute", f"{EC2}:instance/i-existing", REGION)
    context = REGION | {f"aws:ResourceTag/{k}": v for k, v in TAGS.items()}
    assert _allows(DEPLOY, "ec2:ModifyVpcAttribute", resource, context)
    context["aws:RequestedRegion"] = "us-east-1"
    assert not _allows(DEPLOY, "ec2:ModifyVpcAttribute", resource, context)


def test_launch_requires_reviewed_shape_metadata_profile_and_ami():
    context = REGION | {f"aws:RequestTag/{k}": v for k, v in TAGS.items()} | {
        "ec2:InstanceType": "t3.xlarge",
        "ec2:AvailabilityZone": "us-west-2a",
        "ec2:Tenancy": "default",
        "ec2:MetadataHttpTokens": "required",
        "ec2:MetadataHttpEndpoint": "enabled",
        "ec2:InstanceProfile": f"{IAM}:instance-profile/blockchain-runtime-ec2",
        "ec2:MetadataHttpPutResponseHopLimit": "2",
    }
    resource = f"{EC2}:instance/i-offline"
    assert _allows(DEPLOY, "ec2:RunInstances", resource, context)
    for key, value in [("ec2:InstanceType", "t3.2xlarge"), ("ec2:MetadataHttpTokens", "optional"), ("ec2:InstanceProfile", f"{IAM}:instance-profile/platform")]:
        assert not _allows(DEPLOY, "ec2:RunInstances", resource, context | {key: value})
    assert _allows(DEPLOY, "ec2:RunInstances", "arn:aws:ec2:us-west-2::image/ami-0dac6cd09676c9fb1", REGION)
    assert not _allows(DEPLOY, "ec2:RunInstances", "arn:aws:ec2:us-west-2::image/ami-unreviewed", REGION)


def test_ebs_requires_encryption_and_fixed_sizes():
    context = REGION | {"ec2:VolumeType": "gp3", "ec2:AvailabilityZone": "us-west-2a", "ec2:Encrypted": "true", "ec2:VolumeSize": "30"}
    resource = f"{EC2}:volume/vol-offline"
    assert _allows(DEPLOY, "ec2:RunInstances", resource, context)
    assert not _allows(DEPLOY, "ec2:RunInstances", resource, context | {"ec2:Encrypted": "false"})
    assert not _allows(DEPLOY, "ec2:RunInstances", resource, context | {"ec2:VolumeSize": "1000"})


def test_stop_allowed_only_for_tagged_runtime_and_no_start_destroy_or_ingress():
    context = REGION | {f"aws:ResourceTag/{k}": v for k, v in TAGS.items()}
    assert _allows(DEPLOY, "ec2:StopInstances", f"{EC2}:instance/i-offline", context)
    assert not _allows(DEPLOY, "ec2:StopInstances", f"{EC2}:instance/i-offline", REGION)
    for action in ("ec2:StartInstances", "ec2:TerminateInstances", "ec2:DeleteVolume", "ec2:AuthorizeSecurityGroupIngress"):
        assert not _allows(DEPLOY, action, f"{EC2}:instance/i-offline", context)


def test_no_image_push_secrets_or_ssm_execution_in_initial_provisioner():
    for action, resource in [
        ("ecr:PutImage", f"arn:aws:ecr:us-west-2:{ACCOUNT}:repository/blockchain-runtime-producer"),
        ("secretsmanager:CreateSecret", "*"),
        ("secretsmanager:GetSecretValue", "*"),
        ("ssm:SendCommand", "*"),
        ("ssm:StartSession", "*"),
    ]:
        assert not _allows(DEPLOY, action, resource, REGION)


def test_runtime_boundary_confines_data_and_explicitly_denies_secrets_and_state():
    files = ["runtime-boundary.json"]
    assert _allows(files, "s3:PutObject", f"{DATA_BUCKET}/{PILOT}/bronze/swaps/example.parquet")
    assert not _allows(files, "s3:PutObject", f"{DATA_BUCKET}/blockchain-analytics/bronze/example.parquet")
    assert _allows(files, "s3:ListBucket", DATA_BUCKET, {"s3:prefix": PILOT + "/checkpoints/swaps_bronze"})
    assert not _allows(files, "s3:ListBucket", DATA_BUCKET, {"s3:prefix": "blockchain-analytics/bronze"})
    assert not _allows(files, "s3:GetObject", STATE)
    assert not _allows(files, "secretsmanager:GetSecretValue", "*", REGION)
    assert not _allows(files, "ssm:GetParameter", f"arn:aws:ssm:us-west-2:{ACCOUNT}:parameter/private-secret", REGION)


def test_event_target_cannot_be_redirected():
    rule = f"arn:aws:events:us-west-2:{ACCOUNT}:rule/blockchain-runtime-state-change"
    topic = f"arn:aws:sns:us-west-2:{ACCOUNT}:blockchain-runtime-alerts"
    assert _allows(DEPLOY, "events:PutTargets", rule, REGION | {"events:TargetArn": [topic]})
    assert not _allows(DEPLOY, "events:PutTargets", rule, REGION)
    assert not _allows(DEPLOY, "events:PutTargets", rule, REGION | {"events:TargetArn": [topic, topic + "-other"]})
