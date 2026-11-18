"""Generate proposed commands in temporary directories; never execute them."""

import ast
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tarfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "probe_package", ROOT / "scripts/prepare_runtime_reader_probe.py"
)
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


@pytest.fixture
def committed(monkeypatch):
    def git(repo, *args):
        if args == ("branch", "--show-current"):
            return b"develop\n"
        if args == ("status", "--porcelain", "--untracked-files=no"):
            return b""
        if args == ("rev-parse", "--verify", "HEAD^{commit}"):
            return b"a" * 40 + b"\n"
        assert args[:2] == ("cat-file", "blob")
        return (ROOT / args[2].split(":", 1)[1]).read_bytes()

    monkeypatch.setattr(p, "_git", git)
    return git


def test_package_is_reproducible_private_and_contains_only_pinned_sources(
    committed, tmp_path
):
    first = p.build(ROOT, tmp_path / "first")
    second = p.build(ROOT, tmp_path / "second")
    assert first == second
    assert first["owner_approved"] is False
    assert first["cloud_actions"] is False
    archive = tmp_path / "first/probe.tar"
    assert (
        hashlib.sha256(archive.read_bytes()).hexdigest()
        == first["probe_archive_sha256"]
    )
    assert (tmp_path / "first").stat().st_mode & 0o777 == 0o700
    assert all(
        f.stat().st_mode & 0o777 == 0o600 for f in (tmp_path / "first").iterdir()
    )
    with tarfile.open(archive) as bundle:
        assert set(bundle.getnames()) == set(p.FILES) | {"probe.manifest.json"}
        manifest = json.loads(bundle.extractfile("probe.manifest.json").read())
        assert manifest["maximum_secret_reads"] == 1
        assert (
            manifest["secret_writes"]
            == manifest["container_starts"]
            == manifest["provider_rpc_calls"]
            == 0
        )
        assert manifest["secret_version_id"] == p.VERSION
        for name, digest in manifest["file_sha256"].items():
            assert hashlib.sha256(bundle.extractfile(name).read()).hexdigest() == digest
    upload = (tmp_path / "first/upload-command.sh").read_text()
    lines = upload.splitlines()
    assert lines[0] == "sudo /usr/bin/python3 - <<'BLOCKCHAIN_READER_PROBE_UPLOAD'"
    assert lines[-1] == "BLOCKCHAIN_READER_PROBE_UPLOAD"
    assert max(map(len, lines)) < 2048  # Avoid SSM pseudo-terminal line truncation.
    script = "\n".join(lines[1:-1])
    tree = ast.parse(script, feature_version=(3, 9))
    assert not any(isinstance(node, ast.Assert) for node in ast.walk(tree))
    assert first["probe_archive_sha256"] in script
    subprocess.run(
        ["/bin/sh", "-n"], input=upload, text=True, check=True
    )  # Syntax only.
    probe = (tmp_path / "first/probe-command.sh").read_text()
    assert probe.endswith(
        "--execute-on-approved-host /opt/blockchain-runtime/release.json\n"
    )


def test_existing_output_is_preserved(committed, tmp_path):
    destination = tmp_path / "output"
    p.build(ROOT, destination)
    before = (destination / "proposal.json").read_bytes()
    with pytest.raises(FileExistsError):
        p.build(ROOT, destination)
    assert (destination / "proposal.json").read_bytes() == before


def test_changed_shared_contract_is_blocked_before_any_output(
    committed, tmp_path, monkeypatch
):
    def changed(repo, *args):
        value = committed(repo, *args)
        if args[:2] == ("cat-file", "blob") and args[2].endswith(
            ":runtime/contracts.py"
        ):
            value += b"\n# changed fixture\n"
        return value

    monkeypatch.setattr(p, "_git", changed)
    with pytest.raises(ValueError, match="SHARED_CONTRACT_HASH_MISMATCH"):
        p.build(ROOT, tmp_path / "blocked")
    assert not (tmp_path / "blocked").exists()


def test_dirty_source_blocks_before_reading_payload(committed, tmp_path, monkeypatch):
    def dirty(repo, *args):
        if args[0] == "status":
            return b" M runtime/contracts.py"
        return committed(repo, *args)

    monkeypatch.setattr(p, "_git", dirty)
    with pytest.raises(ValueError, match="TRACKED_WORKTREE_CHANGED"):
        p.build(ROOT, tmp_path / "blocked")
    assert not (tmp_path / "blocked").exists()
