"""Prepare committed build context and a non-publishing ECR review plan."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import io
import json
import os
import re
import subprocess
import tarfile
import tempfile
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
JAR_CONTEXT_PREFIX = "deploy/runtime/verified-jars"
MAX_JAR_CACHE_BYTES = 1024**3


def _write_new(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)


def build_context(repo: Path, output: Path, jar_cache: Path | None = None) -> dict:
    commit = _git(repo, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
    if not re.fullmatch("[0-9a-f]{40}", commit):
        raise ValueError("SOURCE_COMMIT_INVALID")
    data = _git(repo, "archive", "--format=tar", commit, "--", *BUILD_PATHS)
    if jar_cache is None:
        _write_new(output, data)
        return {"source_commit": commit, "context_sha256": hashlib.sha256(data).hexdigest()}
    if jar_cache.is_symlink() or not jar_cache.is_dir():
        raise ValueError("VERIFIED_JAR_CACHE_REQUIRED")
    # Trust the committed lock, not a dirty worktree or a restored cache key.
    lock = json.loads(_git(repo, "show", f"{commit}:deploy/runtime/jars.lock.json"))
    with ExitStack() as stack:
        dependencies = []
        names = set()
        total = 0
        for artifact in lock:
            path = artifact["path"]
            name = path.rsplit("/", 1)[-1]
            if (
                ".." in path or path.startswith("/") or not name.endswith(".jar")
                or name in names or not re.fullmatch("[0-9a-f]{128}", artifact["sha512"])
            ):
                raise ValueError("JAR_LOCK_INVALID")
            names.add(name)
            cached = jar_cache / name
            if cached.is_symlink() or not cached.is_file():
                raise ValueError("JAR_CACHE_FILE_INVALID")
            if total + cached.stat().st_size > MAX_JAR_CACHE_BYTES:
                raise ValueError("JAR_CACHE_TOO_LARGE")
            # Snapshot while hashing so a cache-file swap cannot alter the tar.
            staged = stack.enter_context(tempfile.TemporaryFile())
            digest = hashlib.sha512()
            size = 0
            with cached.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    size += len(chunk)
                    if total + size > MAX_JAR_CACHE_BYTES:
                        raise ValueError("JAR_CACHE_TOO_LARGE")
                    digest.update(chunk)
                    staged.write(chunk)
            if digest.hexdigest() != artifact["sha512"]:
                raise ValueError("JAR_CACHE_CHECKSUM_MISMATCH")
            staged.seek(0)
            total += size
            member = tarfile.TarInfo(f"{JAR_CONTEXT_PREFIX}/{name}")
            member.size, member.mode, member.mtime = size, 0o644, 0
            dependencies.append((member, staged))
        descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as destination, tarfile.open(
            fileobj=io.BytesIO(data)
        ) as original, tarfile.open(
            fileobj=destination, mode="w", format=tarfile.PAX_FORMAT,
            # Like git archive, begin with a global provenance header. Docker
            # sniffs only 1024 bytes: a per-file PAX header needs the following
            # file header too, which would lie beyond that lookahead window.
            pax_headers={"comment": commit},
        ) as archive:
            for member in original:
                if member.name.rstrip("/") == JAR_CONTEXT_PREFIX or member.name.startswith(JAR_CONTEXT_PREFIX + "/"):
                    raise ValueError("JAR_CONTEXT_TRACKED_PATH_CONFLICT")
                archive.addfile(member, original.extractfile(member) if member.isfile() else None)
            directory = tarfile.TarInfo(JAR_CONTEXT_PREFIX)
            directory.type, directory.mode, directory.mtime = tarfile.DIRTYPE, 0o755, 0
            archive.addfile(directory)
            for member, staged in dependencies:
                archive.addfile(member, staged)
    digest = hashlib.sha256()
    with output.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return {
        "source_commit": commit, "context_sha256": digest.hexdigest(),
        "verified_maven_artifacts": len(dependencies), "verified_maven_bytes": total,
    }


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
                "local_reference": LOCAL_IMAGES[role],
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
    parser.add_argument("--jar-cache", type=Path)
    args = parser.parse_args()
    try:
        if args.action == "context":
            result = build_context(ROOT, args.output, args.jar_cache)
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
