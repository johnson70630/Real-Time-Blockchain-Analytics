"""Build/verify an offline host bundle; never install, extract or deploy it."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import tarfile
from pathlib import Path

FILES = (
    "runtime/__init__.py",
    "runtime/bundle.py",
    "runtime/contracts.py",
    "runtime/host.py",
    "deploy/runtime/compose.yaml",
    "deploy/runtime/blockchain-pilot.service",
    "deploy/runtime/blockchain-pilot-deadline.timer",
    "deploy/runtime/blockchain-pilot-poweroff.service",
    "deploy/runtime/release.example.json",
)
MANIFEST = "bundle.manifest.json"
MAX_FILE_BYTES = 256 * 1024
MAX_BUNDLE_BYTES = 2 * 1024 * 1024


def _git(repo: Path, *args: str) -> bytes:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("GIT_", "AWS_"))
    }
    env.update(GIT_ALLOW_PROTOCOL="", GIT_NO_LAZY_FETCH="1", GIT_TERMINAL_PROMPT="0")
    result = subprocess.run(
        ["git", "--no-replace-objects", "--no-pager", "-C", str(repo), *args],
        capture_output=True,
        env=env,
        check=False,
    )
    if result.returncode:
        raise ValueError("LOCAL_COMMITTED_SOURCE_REQUIRED")
    return result.stdout


def _json(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _archive(payload: dict[str, bytes], manifest: bytes) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name, content in [*sorted(payload.items()), (MANIFEST, manifest)]:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            member.mode = 0o644
            member.mtime = member.uid = member.gid = 0
            tar.addfile(member, io.BytesIO(content))
    return stream.getvalue()


def build_bundle(repo: Path, output: Path) -> dict:
    """Read an allowlist from immutable HEAD blobs, excluding the work tree."""
    commit = _git(repo, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
    if not re.fullmatch("[0-9a-f]{40}", commit):
        raise ValueError("SOURCE_COMMIT_INVALID")
    payload = {}
    for name in FILES:
        entry = _git(repo, "ls-tree", "-z", commit, "--", name)
        if not entry:
            raise ValueError("BUNDLE_SOURCE_MISSING")
        metadata, path = entry.rstrip(b"\0").split(b"\t")
        mode, kind, _object = metadata.split()
        if (
            path.decode() != name
            or mode not in (b"100644", b"100755")
            or kind != b"blob"
        ):
            raise ValueError("BUNDLE_SOURCE_NOT_REGULAR")
        size = int(_git(repo, "cat-file", "-s", f"{commit}:{name}"))
        if size > MAX_FILE_BYTES:
            raise ValueError("BUNDLE_SOURCE_TOO_LARGE")
        content = _git(repo, "cat-file", "blob", f"{commit}:{name}")
        if len(content) != size:
            raise ValueError("BUNDLE_SOURCE_SIZE_MISMATCH")
        payload[name] = content
    manifest = {
        "schema_version": 1,
        "git_commit": commit,
        "files": {
            name: {"sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}
            for name, data in sorted(payload.items())
        },
    }
    data = _archive(payload, _json(manifest))
    if len(data) > MAX_BUNDLE_BYTES:
        raise ValueError("BUNDLE_TOO_LARGE")
    # Exclusive creation avoids clobbering an artifact or following its symlink.
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
    return {
        "git_commit": commit,
        "sha256": hashlib.sha256(data).hexdigest(),
        "files": len(payload),
    }


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("BUNDLE_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def verify_bundle(archive: Path, expected_sha256: str) -> dict:
    """Verify a trusted external digest and bounded contents without extraction."""
    if not re.fullmatch("[0-9a-f]{64}", expected_sha256):
        raise ValueError("REVIEWED_BUNDLE_DIGEST_REQUIRED")
    with archive.open("rb") as handle:
        data = handle.read(MAX_BUNDLE_BYTES + 1)
    if len(data) > MAX_BUNDLE_BYTES:
        raise ValueError("BUNDLE_TOO_LARGE")
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise ValueError("BUNDLE_DIGEST_MISMATCH")
    payload = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as tar:
        for member in tar:
            if (
                member.name not in (*FILES, MANIFEST)
                or member.name in payload
                or not member.isfile()
                or not 0 <= member.size <= MAX_FILE_BYTES
            ):
                raise ValueError("BUNDLE_MEMBER_INVALID")
            handle = tar.extractfile(member)
            if handle is None:
                raise ValueError("BUNDLE_MEMBER_MISSING")
            content = handle.read(MAX_FILE_BYTES + 1)
            if len(content) != member.size:
                raise ValueError("BUNDLE_MEMBER_SIZE_MISMATCH")
            payload[member.name] = content
    if set(payload) != set(FILES) | {MANIFEST}:
        raise ValueError("BUNDLE_FILE_SET_INVALID")
    manifest_bytes = payload.pop(MANIFEST)
    manifest = json.loads(manifest_bytes, object_pairs_hook=_unique_object)
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {"schema_version", "git_commit", "files"}
        or type(manifest["schema_version"]) is not int
        or manifest["schema_version"] != 1
        or not isinstance(manifest["git_commit"], str)
        or not re.fullmatch("[0-9a-f]{40}", manifest["git_commit"])
        or not isinstance(manifest["files"], dict)
        or set(manifest["files"]) != set(FILES)
    ):
        raise ValueError("BUNDLE_MANIFEST_INVALID")
    for name, content in payload.items():
        record = manifest["files"][name]
        if (
            not isinstance(record, dict)
            or set(record) != {"sha256", "size"}
            or type(record["size"]) is not int
            or record["size"] != len(content)
            or record["sha256"] != hashlib.sha256(content).hexdigest()
        ):
            raise ValueError("BUNDLE_CONTENT_MISMATCH")
    # Enforce canonical metadata/order/padding too, rejecting hidden extra bytes.
    if manifest_bytes != _json(manifest) or data != _archive(payload, manifest_bytes):
        raise ValueError("BUNDLE_NOT_CANONICAL")
    return {
        "git_commit": manifest["git_commit"],
        "sha256": expected_sha256,
        "files": len(payload),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    build = commands.add_parser("build")
    build.add_argument("--output", type=Path, required=True)
    verify = commands.add_parser("verify")
    verify.add_argument("archive", type=Path)
    verify.add_argument("--sha256", required=True)
    args = parser.parse_args()
    try:
        if args.action == "build":
            result = build_bundle(Path(__file__).resolve().parents[1], args.output)
        else:
            result = verify_bundle(args.archive, args.sha256)
        print(json.dumps({**result, "deployment_performed": False}, sort_keys=True))
        return 0
    except Exception as error:
        print(f"BUNDLE_BLOCKED type={type(error).__name__}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
