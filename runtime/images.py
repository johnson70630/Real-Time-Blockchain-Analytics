"""Prepare committed build context and a non-publishing ECR review plan."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

from runtime.bundle import _git
from runtime.contracts import REGISTRY

KAFKA_SOURCE = "apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed"
BUILD_PATHS = (
    ".dockerignore",
    "config",
    "producer",
    "spark",
    "runtime",
    "deploy/runtime",
)
LOCAL_IMAGES = {
    "producer": "blockchain-pilot-producer:offline",
    "spark": "blockchain-pilot-spark:offline",
    "kafka": KAFKA_SOURCE,
}
ROOT = Path(__file__).resolve().parents[1]


def _write_new(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)


def build_context(repo: Path, output: Path) -> dict:
    commit = _git(repo, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
    if not re.fullmatch("[0-9a-f]{40}", commit):
        raise ValueError("SOURCE_COMMIT_INVALID")
    data = _git(repo, "archive", "--format=tar", commit, "--", *BUILD_PATHS)
    _write_new(output, data)
    return {"source_commit": commit, "context_sha256": hashlib.sha256(data).hexdigest()}


def publication_plan(inspections: dict, commit: str) -> dict:
    """Validate local evidence; never equate a local image ID with an ECR digest."""
    if not re.fullmatch("[0-9a-f]{40}", commit) or set(inspections) != set(
        LOCAL_IMAGES
    ):
        raise ValueError("IMAGE_REVIEW_SCOPE_INVALID")
    images = []
    ids = set()
    for role in LOCAL_IMAGES:
        image = inspections[role]
        image_id = image.get("id")
        if (
            not isinstance(image_id, str)
            or not re.fullmatch("sha256:[0-9a-f]{64}", image_id)
            or image_id in ids
            or image.get("architecture") != "amd64"
            or image.get("os") != "linux"
        ):
            raise ValueError("LINUX_AMD64_DISTINCT_IMAGES_REQUIRED")
        ids.add(image_id)
        if role == "kafka":
            digests = image.get("repo_digests")
            if not isinstance(digests, list) or not (
                {KAFKA_SOURCE, "docker.io/" + KAFKA_SOURCE} & set(digests)
            ):
                raise ValueError("PINNED_KAFKA_SOURCE_REQUIRED")
        elif (
            image.get("revision") != commit
            or image.get("title") != "blockchain-runtime-" + role
            or image.get("user") != "10001:10001"
        ):
            raise ValueError("REVIEWED_SOURCE_LABELS_AND_USER_REQUIRED")
        repository = "spark" if role == "spark" else "producer"
        tag = ("kafka-3.9.1" if role == "kafka" else role) + "-" + commit
        images.append(
            {
                "role": role,
                "local_image_id": image_id,
                "platform": "linux/amd64",
                "source_commit": commit if role != "kafka" else None,
                "upstream_reference": KAFKA_SOURCE if role == "kafka" else None,
                "target": f"{REGISTRY}/blockchain-runtime-{repository}:{tag}",
                "ecr_deployment_digest": None,
                "scan_review": "pending",
            }
        )
    return {
        "source_commit": commit,
        "images": images,
        "publication_authorized": False,
        "aws_calls_made": False,
        "push_performed": False,
    }


def inspect_local_images() -> dict:
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("AWS_", "DOCKER_", "BUILDX_", "BUILDKIT_"))
    }

    def docker(*args):
        result = subprocess.run(
            ["docker", *args], env=env, text=True, capture_output=True, check=False
        )
        if result.returncode:
            raise RuntimeError("LOCAL_DOCKER_INSPECTION_FAILED")
        return result.stdout.strip()

    endpoint = json.loads(
        docker("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}")
    )
    if not isinstance(endpoint, str) or not endpoint.startswith("unix:///"):
        raise ValueError("LOCAL_UNIX_DOCKER_REQUIRED")
    fields = ("id", "os", "architecture", "user", "repo_digests", "revision", "title")
    expressions = (
        ".Id",
        ".Os",
        ".Architecture",
        ".Config.User",
        ".RepoDigests",
        '(index .Config.Labels "org.opencontainers.image.revision")',
        '(index .Config.Labels "org.opencontainers.image.title")',
    )
    template = "\n".join("{{json " + expression + "}}" for expression in expressions)
    result = {}
    for role, reference in LOCAL_IMAGES.items():
        raw = docker(
            "--host",
            endpoint,
            "image",
            "inspect",
            "--platform",
            "linux/amd64",
            "--format",
            template,
            reference,
        )
        values = [json.loads(line) for line in raw.splitlines()]
        if len(values) != len(fields):
            raise ValueError("IMAGE_INSPECTION_FIELDS_INVALID")
        result[role] = dict(zip(fields, values))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["context", "plan"])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.action == "context":
            result = build_context(ROOT, args.output)
        else:
            commit = (
                _git(ROOT, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
            )
            result = publication_plan(inspect_local_images(), commit)
            _write_new(args.output, (json.dumps(result, indent=2) + "\n").encode())
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        print(f"IMAGE_REVIEW_BLOCKED type={type(error).__name__}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
