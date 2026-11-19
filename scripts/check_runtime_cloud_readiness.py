"""Read-only current-image/IAM checks for the one approved demo; no secret reads."""

import argparse
import datetime
import hashlib
import importlib.util
import json
import logging
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from runtime.pilot_security import require_unused_acceptance  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "window", str(Path(__file__).with_name("runtime_pilot_window.py"))
)
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def policy_normalize(value, key=""):
    if key in {"Action", "Resource", "NotAction", "NotResource"}:
        return sorted(value if isinstance(value, list) else [value])
    if isinstance(value, dict):
        return {k: policy_normalize(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return sorted([policy_normalize(x, key) for x in value], key=canonical)
    return value


def finding_key(value):
    attrs = {x["key"]: x["value"] for x in value.get("attributes", [])}
    return (
        value.get("name"),
        value.get("severity"),
        attrs.get("package_name"),
        attrs.get("package_version"),
    )


def check_findings(live, baseline):
    expected = sorted(
        (x["cve"], x["severity"], x.get("source_package"), x.get("package_version"))
        for x in baseline
    )
    actual = sorted(finding_key(x) for x in live)
    w.require(actual == expected, "SCAN_FINDING_INVENTORY_CHANGED")


def validate_local():
    require_unused_acceptance(ROOT)
    root = Path(w.MARKER.read_text().strip())
    receipt = json.loads((root / "owner-approval.json").read_text())
    w.require(
        subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
        == receipt["source_commit"],
        "GIT_CHECKPOINT_CHANGED",
    )
    w.require(
        subprocess.check_output(
            ["git", "branch", "--show-current"], cwd=ROOT, text=True
        ).strip()
        == "develop",
        "DEVELOP_REQUIRED",
    )
    w.require(
        not subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=ROOT,
            text=True,
        ).strip(),
        "TRACKED_WORKTREE_CHANGED",
    )
    for name, expected in receipt["approved_artifact_sha256"].items():
        w.require(
            hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected,
            "APPROVED_ARTIFACT_CHANGED",
        )
    acceptance = json.loads(
        (
            ROOT / "docs/evidence/aws_runtime_security_acceptance_2026-11-17.json"
        ).read_text()
    )
    expires = datetime.datetime.fromisoformat(
        acceptance["expires_at_utc"].replace("Z", "+00:00")
    )
    w.require(
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=45)
        < expires,
        "SECURITY_EXCEPTION_TOO_CLOSE_TO_EXPIRY",
    )
    w.require(
        acceptance["accepted"] is True and acceptance["maximum_runs"] == 1,
        "SECURITY_ACCEPTANCE_REQUIRED",
    )
    return root, acceptance


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expected-state", choices=["stopped", "running"], default="stopped"
    )
    args = parser.parse_args()
    root, acceptance = validate_local()
    ec2, ssm = w.clients(5400 if args.expected_state == "stopped" else 3000)
    w.require(
        w.instance(ec2)["State"]["Name"] == args.expected_state,
        "INSTANCE_STATE_MISMATCH",
    )
    if args.expected_state == "running":
        w.require_admitted_watch(root, ec2)
    import boto3
    from botocore.config import Config

    logging.disable(logging.CRITICAL)
    session = boto3.Session(profile_name=w.PROFILE, region_name=w.REGION)
    config = Config(
        connect_timeout=8, read_timeout=20, retries={"total_max_attempts": 1}
    )
    iam = session.client("iam", config=config)
    ecr = session.client("ecr", config=config)
    role = iam.get_role(RoleName="blockchain-runtime-ec2")["Role"]
    boundary = "arn:aws:iam::209479274979:policy/blockchain-runtime-ec2-boundary"
    w.require(
        role.get("PermissionsBoundary", {}).get("PermissionsBoundaryArn") == boundary,
        "BOUNDARY_ATTACHMENT_CHANGED",
    )
    trust = role["AssumeRolePolicyDocument"]
    rows = trust.get("Statement", [])
    w.require(trust.get("Version") == "2012-10-17" and len(rows) == 1, "TRUST_CHANGED")
    row = rows[0]
    w.require(
        row.get("Effect") == "Allow"
        and row.get("Action") in ["sts:AssumeRole", ["sts:AssumeRole"]]
        and row.get("Principal") == {"Service": "ec2.amazonaws.com"}
        and set(row) <= {"Sid", "Effect", "Action", "Principal"},
        "TRUST_CHANGED",
    )
    inline = iam.list_role_policies(RoleName=role["RoleName"])["PolicyNames"]
    attached = iam.list_attached_role_policies(RoleName=role["RoleName"])[
        "AttachedPolicies"
    ]
    w.require(
        inline == ["blockchain-runtime-least-privilege"], "INLINE_INVENTORY_CHANGED"
    )
    w.require(
        [x["PolicyArn"] for x in attached]
        == ["arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"],
        "ROLE_ATTACHMENTS_CHANGED",
    )
    current = iam.get_role_policy(RoleName=role["RoleName"], PolicyName=inline[0])[
        "PolicyDocument"
    ]
    expected = json.loads(
        (
            ROOT
            / "docs/policies/runtime-deployer/runtime-role-secret-read.proposed.json"
        ).read_text()
    )
    w.require(
        policy_normalize(current) == policy_normalize(expected),
        "WORKLOAD_POLICY_CHANGED",
    )
    meta = iam.get_policy(PolicyArn=boundary)["Policy"]
    w.require(meta["DefaultVersionId"] == "v2", "BOUNDARY_VERSION_CHANGED")
    doc = iam.get_policy_version(PolicyArn=boundary, VersionId="v2")["PolicyVersion"][
        "Document"
    ]
    proposed = json.loads(
        (
            ROOT
            / "infra/terraform/environments/runtime/policies/runtime-secret-boundary.proposed.json"
        ).read_text()
    )
    w.require(canonical(doc) == canonical(proposed), "BOUNDARY_CONTENT_CHANGED")
    profile = iam.get_instance_profile(InstanceProfileName="blockchain-runtime-ec2")[
        "InstanceProfile"
    ]
    w.require(
        len(profile["Roles"]) == 1 and profile["Roles"][0]["RoleId"] == role["RoleId"],
        "PROFILE_ROLE_CHANGED",
    )
    scanning = ecr.get_registry_scanning_configuration()["scanningConfiguration"]
    w.require(scanning.get("scanType") == "BASIC", "REGISTRY_SCANNING_CHANGED")
    publication = json.loads(
        (
            ROOT / "docs/evidence/aws_runtime_quarantined_publication_2026-11-17.json"
        ).read_text()
    )
    repos = ecr.describe_repositories(
        registryId=w.ACCOUNT,
        repositoryNames=["blockchain-runtime-producer", "blockchain-runtime-spark"],
    )["repositories"]
    w.require(len(repos) == 2, "REPOSITORY_COUNT_CHANGED")
    for repo in repos:
        w.require(
            repo.get("imageTagMutability") == "IMMUTABLE"
            and repo.get("imageScanningConfiguration", {}).get("scanOnPush") is True
            and repo.get("encryptionConfiguration") == {"encryptionType": "AES256"}
            and not repo.get("imageTagMutabilityExclusionFilters"),
            "REPOSITORY_SAFEGUARDS_CHANGED",
        )
        rules = json.loads(
            ecr.get_lifecycle_policy(
                registryId=w.ACCOUNT, repositoryName=repo["repositoryName"]
            )["lifecyclePolicyText"]
        ).get("rules", [])
        w.require(
            bool(rules)
            and all(
                x.get("selection", {}).get("tagStatus") == "untagged"
                and x.get("action", {}).get("type") == "expire"
                for x in rules
            ),
            "TAGGED_RETENTION_CHANGED",
        )
    baseline = {x["role"]: x for x in publication["images"]}
    results = []
    for item in acceptance["artifact_scope"]:
        image = baseline[item["role"]]
        repo = image["repository"]
        digest = image["manifest_digest"]
        w.require(
            item["quarantined_digest_reference"]
            == image["quarantined_digest_reference"],
            "BASELINE_IMAGE_BINDING_MISMATCH",
        )
        details = ecr.describe_images(
            registryId=w.ACCOUNT,
            repositoryName=repo,
            imageIds=[{"imageDigest": digest}],
        )["imageDetails"]
        w.require(
            len(details) == 1 and details[0]["imageDigest"] == digest,
            "IMAGE_IDENTITY_CHANGED",
        )
        manifest = ecr.batch_get_image(
            registryId=w.ACCOUNT,
            repositoryName=repo,
            imageIds=[{"imageDigest": digest}],
            acceptedMediaTypes=[
                "application/vnd.oci.image.manifest.v1+json",
                "application/vnd.docker.distribution.manifest.v2+json",
            ],
        )
        w.require(
            not manifest.get("failures") and len(manifest.get("images", [])) == 1,
            "MANIFEST_READ_FAILED",
        )
        body = manifest["images"][0]["imageManifest"]
        w.require(
            "sha256:" + hashlib.sha256(body.encode()).hexdigest() == digest,
            "MANIFEST_HASH_CHANGED",
        )
        w.require(
            json.loads(body)["config"]["digest"] == item["config_digest"],
            "IMAGE_CONFIG_CHANGED",
        )
        rows = []
        token = None
        pages = 0
        counts = None
        while True:
            kwargs = {
                "registryId": w.ACCOUNT,
                "repositoryName": repo,
                "imageId": {"imageDigest": digest},
                "maxResults": 1000,
            }
            if token:
                kwargs["nextToken"] = token
            page = ecr.describe_image_scan_findings(**kwargs)
            pages += 1
            w.require(
                page["imageScanStatus"]["status"] == "COMPLETE", "SCAN_NOT_COMPLETE"
            )
            scan = page.get("imageScanFindings", {})
            w.require(not scan.get("enhancedFindings"), "SCAN_TYPE_CHANGED")
            if counts is None:
                counts = scan.get("findingSeverityCounts", {})
            rows.extend(scan.get("findings", []))
            token = page.get("nextToken")
            if not token:
                break
            w.require(pages < 20, "SCAN_PAGINATION_LIMIT")
        check_findings(rows, image["findings"])
        w.require(
            {k: v for k, v in counts.items() if v}
            == {k: v for k, v in image["scan_severity_counts"].items() if v},
            "SCAN_COUNTS_CHANGED",
        )
        results.append(
            {
                "role": item["role"],
                "manifest_and_config_match": True,
                "finding_inventory_unchanged": True,
                "findings": len(rows),
                "counts": counts,
            }
        )
    w.require(
        w.instance(ec2)["State"]["Name"] == args.expected_state,
        "INSTANCE_STATE_CHANGED",
    )
    if args.expected_state == "running":
        admitted = w.require_admitted_watch(root, ec2)
    result = {
        "checked_at_utc": w.now(),
        "all_cloud_readiness_checks_passed": True,
        "instance_state": args.expected_state,
        "iam_boundary_trust_profile_and_secret_policy_match": True,
        "images": results,
        "secret_values_read": False,
        "aws_writes": False,
    }
    if args.expected_state == "running":
        result["launch_epoch_seconds"] = admitted["launch_epoch_seconds"]
    w.write(
        root
        / (
            "cloud-readiness.json"
            if args.expected_state == "stopped"
            else "running-cloud-readiness.json"
        ),
        result,
    )
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        print(
            json.dumps(
                {
                    "STOP": "CLOUD_READINESS_FAILED",
                    "category": code or type(error).__name__,
                    "guard": str(error) if type(error) is RuntimeError else None,
                    "details_suppressed": True,
                }
            ),
            flush=True,
        )
        raise SystemExit(1)
