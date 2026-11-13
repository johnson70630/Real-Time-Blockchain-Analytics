"""Offline deployment-artifact integrity tests; no Docker, AWS or extraction."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile

import pytest

from runtime import bundle


@pytest.fixture
def packaged(tmp_path):
    repo = tmp_path / "source"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    for name in bundle.FILES:
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("reviewed host source\n")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=Offline Test",
            "-c",
            "user.email=offline@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    archive = tmp_path / "host.tar"
    result = bundle.build_bundle(repo, archive)
    return repo, archive, result


def _rewrite(archive, change):
    members = []
    with tarfile.open(archive, "r:") as tar:
        for member in tar:
            members.append((member, tar.extractfile(member).read()))
    members = change(members)
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for member, content in members:
            member.size = len(content)
            tar.addfile(member, io.BytesIO(content) if member.isfile() else None)
    archive.write_bytes(stream.getvalue())
    return hashlib.sha256(stream.getvalue()).hexdigest()


def test_bundle_is_reproducible_and_ignores_worktree_secrets(packaged, tmp_path, monkeypatch):
    repo, archive, result = packaged
    (repo / ".env").write_text("untracked-private-value")
    (repo / "runtime/host.py").write_text("uncommitted-private-value")
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "unreviewed-git-directory"))
    again = tmp_path / "again.tar"
    assert bundle.build_bundle(repo, again) == result
    assert archive.read_bytes() == again.read_bytes()
    assert b"private-value" not in archive.read_bytes()
    assert bundle.verify_bundle(archive, result["sha256"]) == result
    assert archive.stat().st_mode & 0o777 == 0o600


def test_existing_output_is_never_overwritten(packaged):
    repo, archive, _ = packaged
    original = archive.read_bytes()
    with pytest.raises(FileExistsError):
        bundle.build_bundle(repo, archive)
    assert archive.read_bytes() == original


def test_symlink_source_is_rejected(packaged, tmp_path):
    repo, _, _ = packaged
    path = repo / "runtime/host.py"
    path.unlink()
    path.symlink_to("/private-value")
    subprocess.run(["git", "-C", str(repo), "add", "runtime/host.py"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=Offline Test",
            "-c",
            "user.email=offline@example.invalid",
            "commit",
            "-qm",
            "symlink",
        ],
        check=True,
    )
    with pytest.raises(ValueError, match="NOT_REGULAR"):
        bundle.build_bundle(repo, tmp_path / "rejected.tar")


def test_verification_never_calls_external_commands(packaged, monkeypatch):
    _, archive, result = packaged

    def forbidden(*_args, **_kwargs):
        pytest.fail("Verification must not invoke Git, AWS, Docker or extraction")

    monkeypatch.setattr(subprocess, "run", forbidden)
    assert bundle.verify_bundle(archive, result["sha256"]) == result


@pytest.mark.parametrize("digest", ["", "a" * 63, "A" * 64, "a" * 64])
def test_reviewed_external_digest_is_required(packaged, digest):
    _, archive, _ = packaged
    with pytest.raises(ValueError):
        bundle.verify_bundle(archive, digest)


@pytest.mark.parametrize(
    "kind", ["extra", "missing", "duplicate", "symlink", "traversal"]
)
def test_archive_member_attacks_are_rejected_even_with_matching_outer_digest(
    packaged, kind
):
    _, archive, _ = packaged

    def change(members):
        if kind == "missing":
            return members[1:]
        if kind == "duplicate":
            return [*members, members[0]]
        if kind == "symlink":
            member, data = members[0]
            member.type = tarfile.SYMTYPE
            member.linkname = "/private-value"
            return [(member, b""), *members[1:]]
        member = tarfile.TarInfo("../outside" if kind == "traversal" else ".env")
        return [*members, (member, b"not-a-release-file")]

    digest = _rewrite(archive, change)
    with pytest.raises(ValueError, match="BUNDLE_(MEMBER|FILE_SET)_INVALID"):
        bundle.verify_bundle(archive, digest)


def test_payload_tampering_is_detected(packaged):
    _, archive, _ = packaged

    def change(members):
        member, _ = members[0]
        return [(member, b"modified host code"), *members[1:]]

    with pytest.raises(ValueError, match="CONTENT_MISMATCH"):
        bundle.verify_bundle(archive, _rewrite(archive, change))


def test_duplicate_manifest_keys_are_rejected(packaged):
    _, archive, _ = packaged

    def change(members):
        result = []
        for member, data in members:
            if member.name == bundle.MANIFEST:
                manifest = json.loads(data)
                data = ('{"schema_version":1,' + json.dumps(manifest)[1:]).encode()
            result.append((member, data))
        return result

    with pytest.raises(ValueError, match="DUPLICATE_JSON_KEY"):
        bundle.verify_bundle(archive, _rewrite(archive, change))


def test_hidden_trailing_bytes_are_rejected(packaged):
    _, archive, _ = packaged
    data = archive.read_bytes() + b"hidden payload"
    archive.write_bytes(data)
    with pytest.raises(ValueError, match="NOT_CANONICAL"):
        bundle.verify_bundle(archive, hashlib.sha256(data).hexdigest())


def test_oversized_input_is_rejected(packaged, monkeypatch):
    _, archive, result = packaged
    monkeypatch.setattr(bundle, "MAX_BUNDLE_BYTES", 16)
    with pytest.raises(ValueError, match="TOO_LARGE"):
        bundle.verify_bundle(archive, result["sha256"])
