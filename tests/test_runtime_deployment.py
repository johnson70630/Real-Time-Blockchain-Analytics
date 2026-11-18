"""Temporary fixtures only: exclusive installation and immutable candidate checks."""

import hashlib
import json
import os

import pytest

from runtime import bundle, deployment as d


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    payload = {name: b"offline fixture\n" for name in bundle.FILES}
    manifest = {
        "schema_version": 1,
        "git_commit": d.SOURCE_COMMIT,
        "files": {
            name: {"sha256": hashlib.sha256(body).hexdigest(), "size": len(body)}
            for name, body in payload.items()
        },
    }
    archive = tmp_path / "candidate.tar"
    archive.write_bytes(bundle._archive(payload, bundle._json(manifest)))
    release = tmp_path / "release.json"
    release.write_text(json.dumps({"instance_id": d.INSTANCE}))
    monkeypatch.setattr(
        d, "BUNDLE_SHA", hashlib.sha256(archive.read_bytes()).hexdigest()
    )
    monkeypatch.setattr(
        d, "RELEASE_SHA", hashlib.sha256(release.read_bytes()).hexdigest()
    )
    base = tmp_path / "candidates"
    return archive, release, base


def test_install_is_separate_and_repeat_verifies_without_overwriting(
    candidate, tmp_path
):
    archive, release, base = candidate
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    original = legacy / "host.py"
    original.write_bytes(b"original installation")
    result = d.install_candidate(archive, release, base)
    assert result["existing_candidate_reused"] is False
    root = base / d.SOURCE_COMMIT
    before = {
        str(p): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file()
    }
    assert (
        d.install_candidate(archive, release, base)["existing_candidate_reused"] is True
    )
    assert before == {
        str(p): (p.read_bytes(), p.stat().st_mtime_ns)
        for p in root.rglob("*")
        if p.is_file()
    }
    assert original.read_bytes() == b"original installation"
    assert (root / "release.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "mutation",
    [
        "contents",
        "receipt",
        "extra",
        "extra_directory",
        "missing",
        "symlink",
        "writable",
    ],
)
def test_changed_candidate_fails_closed_without_repair(candidate, mutation):
    archive, release, base = candidate
    d.install_candidate(archive, release, base)
    root = base / d.SOURCE_COMMIT
    target = root / "runtime/host.py"
    if mutation == "contents":
        target.write_bytes(b"changed")
    elif mutation == "receipt":
        (root / d.RECEIPT).write_text("{}")
    elif mutation == "extra":
        (root / "extra").write_text("unexpected")
    elif mutation == "extra_directory":
        (root / "extra").mkdir()
    elif mutation == "missing":
        target.unlink()
    elif mutation == "symlink":
        target.unlink()
        target.symlink_to(archive)
    else:
        target.chmod(0o666)
    with pytest.raises(ValueError):
        d.install_candidate(archive, release, base)
    assert (root / d.RECEIPT).exists()


def test_incomplete_install_is_preserved(candidate, monkeypatch):
    archive, release, base = candidate
    original = d._write

    def fail_on_receipt(path, data, mode):
        if path.name == d.RECEIPT:
            raise OSError("simulated interruption")
        original(path, data, mode)

    monkeypatch.setattr(d, "_write", fail_on_receipt)
    with pytest.raises(OSError):
        d.install_candidate(archive, release, base)
    root = base / d.SOURCE_COMMIT
    assert not (root / d.RECEIPT).exists()
    assert (root / "runtime/host.py").is_file()
    with pytest.raises(ValueError, match="CANDIDATE_INCOMPLETE"):
        d.install_candidate(archive, release, base)


def test_changed_inputs_rejected_before_destination_creation(candidate):
    archive, release, base = candidate
    archive.write_bytes(b"unreviewed")
    with pytest.raises(ValueError, match="BUNDLE_DIGEST_MISMATCH"):
        d.install_candidate(archive, release, base)
    assert not base.exists()


def test_verified_snapshot_survives_source_path_replacement(candidate, monkeypatch):
    archive, release, base = candidate
    original, expected_release = d._inputs, release.read_bytes()

    def snapshot_then_replace(*args):
        result = original(*args)
        archive.write_bytes(b"replacement")
        release.write_bytes(b"replacement")
        return result

    monkeypatch.setattr(d, "_inputs", snapshot_then_replace)
    d.install_candidate(archive, release, base)
    assert (base / d.SOURCE_COMMIT / "release.json").read_bytes() == expected_release
    assert d.verify_install(base)["installed_files_verified"] == 9


def test_symlink_destination_is_rejected(candidate, tmp_path):
    archive, release, base = candidate
    redirected = tmp_path / "redirected"
    redirected.mkdir()
    base.symlink_to(redirected)
    with pytest.raises(ValueError, match="UNSAFE_CANDIDATE_DIRECTORY"):
        d.install_candidate(archive, release, base)
    assert list(redirected.iterdir()) == []


def test_unit_is_transient_manual_and_uses_candidate_with_graceful_stop(tmp_path):
    body = d.unit_text()
    assert "[Install]" not in body
    assert f"WorkingDirectory={d.BASE / d.SOURCE_COMMIT}" in body
    assert "KillSignal=SIGTERM\nKillMode=process\nTimeoutStopSec=420" in body
    assert "Restart=no" in body
    assert "PYTHONDONTWRITEBYTECODE=1" in body
    assert "ALCHEMY" not in body
    assert "ExecStopPost=/usr/bin/systemctl --no-block poweroff" in body
    d.stage_unit(tmp_path)
    original = (tmp_path / d.UNIT).stat().st_mtime_ns
    d.stage_unit(tmp_path)
    assert (tmp_path / d.UNIT).stat().st_mtime_ns == original
    (tmp_path / d.UNIT).chmod(0o666)
    with pytest.raises(ValueError, match="EXISTING_UNIT_CHANGED"):
        d.stage_unit(tmp_path)


def test_install_cli_requires_explicit_host_execution(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["deployment", "install"])
    monkeypatch.setattr(
        d, "require_host", lambda: pytest.fail("host touched before approval flag")
    )
    assert d.main() == 1
    assert "CANDIDATE_BLOCKED" in capsys.readouterr().out


def test_wrong_user_cannot_execute_on_host(monkeypatch):
    monkeypatch.setattr(os, "geteuid", lambda: 10001)
    monkeypatch.setattr(
        d, "metadata_instance", lambda: pytest.fail("metadata queried for non-root")
    )
    with pytest.raises(ValueError, match="APPROVED_ROOT_HOST_REQUIRED"):
        d.require_host()


def test_loaded_unit_rejects_inherited_dropins(monkeypatch):
    from types import SimpleNamespace

    def show(args, **kwargs):
        assert args[:3] == ["systemctl", "show", d.UNIT]
        value = (
            str(d.UNIT_DIRECTORY / d.UNIT)
            if "--property=FragmentPath" in args
            else "/etc/systemd/system/service.d/override.conf"
        )
        return SimpleNamespace(returncode=0, stdout=value)

    monkeypatch.setattr(d.subprocess, "run", show)
    with pytest.raises(ValueError, match="LOADED_UNIT_MISMATCH"):
        d.verify_unit()
