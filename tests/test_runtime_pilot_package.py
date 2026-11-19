"""Offline package integrity and command generation from immutable inputs."""

import ast
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import subprocess
import tarfile

import pytest

from runtime import bundle, deployment as d, pilot_operator as op

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "pilot_packager", ROOT / "scripts/prepare_runtime_pilot_package.py"
)
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


def make_archive(contents, commit):
    manifest = {
        "schema_version": 1,
        "git_commit": commit,
        "files": {
            n: {"sha256": hashlib.sha256(b).hexdigest(), "size": len(b)}
            for n, b in contents.items()
        },
    }
    return bundle._archive(contents, bundle._json(manifest))


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    contents = {name: (ROOT / name).read_bytes() for name in bundle.FILES}
    candidate = tmp_path / "candidate.tar"
    candidate.write_bytes(make_archive(contents, d.SOURCE_COMMIT))
    assert hashlib.sha256(candidate.read_bytes()).hexdigest() == d.BUNDLE_SHA
    legacy = tmp_path / "legacy.tar"
    legacy.write_bytes(make_archive(contents, "1" * 40))
    monkeypatch.setattr(
        op, "LEGACY_BUNDLE_SHA", hashlib.sha256(legacy.read_bytes()).hexdigest()
    )
    release = tmp_path / "release.json"
    release.write_text(json.dumps({"instance_id": d.INSTANCE}))
    monkeypatch.setattr(
        d, "RELEASE_SHA", hashlib.sha256(release.read_bytes()).hexdigest()
    )

    def git(repo, *args):
        if args == ("branch", "--show-current"):
            return b"develop\n"
        if args == ("status", "--porcelain", "--untracked-files=no"):
            return b""
        if args == ("rev-parse", "HEAD"):
            return b"a" * 40 + b"\n"
        assert args[:2] == ("cat-file", "blob")
        return (ROOT / args[2].split(":", 1)[1]).read_bytes()

    monkeypatch.setattr(bundle, "_git", git)
    # Exercise packaging for a hypothetical unused successor decision. The real
    # repository decision is consumed and is tested independently below.
    monkeypatch.setattr(p, "require_unused_acceptance", lambda repo: None)
    return candidate, legacy, release


def test_reproducible_package_and_safe_shell_upload(inputs, tmp_path):
    result = p.build(ROOT, *inputs, tmp_path / "one")
    assert p.build(ROOT, *inputs, tmp_path / "two") == result
    assert result["owner_approved"] is False and result["cloud_actions"] is False
    archive = tmp_path / "one/operator.tar"
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == result["archive_sha256"]
    with tarfile.open(archive) as handle:
        assert set(handle.getnames()) == set(op.FILES) | {op.MANIFEST}
        root = tmp_path / "verified"
        root.mkdir(mode=0o700)
        for member in handle:
            target = root / member.name
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            target.write_bytes(handle.extractfile(member).read())
            target.chmod(0o600)
    assert op.package(root)["source_commit"] == "a" * 40
    script = (tmp_path / "one/upload-command.sh").read_text()
    subprocess.run(["/bin/sh", "-n"], input=script, text=True, check=True)
    ast.parse("\n".join(script.splitlines()[1:-1]), feature_version=(3, 9))
    assert max(map(len, script.splitlines())) < 2048
    for action in (
        "controls",
        "install",
        "prepare",
        "launch",
        "status",
        "sample",
        "stop",
        "journal",
    ):
        command = (
            (tmp_path / ("one/" + action + "-command.template"))
            .read_text()
            .replace("__APPROVED_LAUNCH_EPOCH__", "1795043000.0")
        )
        tokens = shlex.split(command)
        assert tokens[:4] == ["sudo", "-n", "/usr/bin/env", "-i"]
        assert "cd " + result["guest_directory"] in tokens[-1]
        assert "runtime.pilot_operator " + action in tokens[-1]
        subprocess.run(["/bin/sh", "-n"], input=command, text=True, check=True)
    (root / "runtime/pilot_operator.py").write_text("changed")
    with pytest.raises(RuntimeError, match="PACKAGE_FILE_CHANGED"):
        op.package(root)


def test_replaced_archive_is_blocked_before_output(inputs, tmp_path):
    inputs[0].write_bytes(b"unreviewed")
    with pytest.raises(ValueError, match="BUNDLE_DIGEST_MISMATCH"):
        p.build(ROOT, *inputs, tmp_path / "blocked")
    assert not (tmp_path / "blocked").exists()


def test_existing_output_is_not_overwritten(inputs, tmp_path):
    p.build(ROOT, *inputs, tmp_path / "one")
    before = (tmp_path / "one/proposal.json").read_bytes()
    with pytest.raises(FileExistsError):
        p.build(ROOT, *inputs, tmp_path / "one")
    assert (tmp_path / "one/proposal.json").read_bytes() == before


def test_consumed_real_decision_blocks_before_output(inputs, tmp_path, monkeypatch):
    from runtime.pilot_security import require_unused_acceptance

    monkeypatch.setattr(p, "require_unused_acceptance", require_unused_acceptance)
    with pytest.raises(RuntimeError, match="SECURITY_EXCEPTION_ALREADY_CONSUMED"):
        p.build(ROOT, *inputs, tmp_path / "blocked")
    assert not (tmp_path / "blocked").exists()
