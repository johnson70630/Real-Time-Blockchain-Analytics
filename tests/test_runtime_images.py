"""Offline source provenance and publication contract tests; no registry access."""

from __future__ import annotations

import copy
import io
import json
import subprocess
import tarfile

import pytest

from runtime import images

COMMIT = "a" * 40


@pytest.fixture
def inspected():
    result = {}
    for index, role in enumerate(images.LOCAL_IMAGES, start=1):
        result[role] = {
            "id": "sha256:" + str(index) * 64,
            "os": "linux",
            "architecture": "amd64",
            "user": "10001:10001",
            "revision": COMMIT,
            "title": "blockchain-runtime-" + role,
            "repo_digests": [images.KAFKA_SOURCE] if role == "kafka" else [],
        }
    return result


def test_exact_targets_require_no_aws_calls_and_no_fabricated_ecr_digests(inspected):
    result = images.publication_plan(inspected, COMMIT)
    assert result["publication_authorized"] is False
    assert result["aws_calls_made"] is False
    assert result["push_performed"] is False
    expected = [
        f"{images.REGISTRY}/blockchain-runtime-producer:producer-{COMMIT}",
        f"{images.REGISTRY}/blockchain-runtime-spark:spark-{COMMIT}",
        f"{images.REGISTRY}/blockchain-runtime-producer:kafka-3.9.1-{COMMIT}",
    ]
    assert [item["target"] for item in result["images"]] == expected
    assert all(item["ecr_deployment_digest"] is None for item in result["images"])
    assert all(item["scan_review"] == "pending" for item in result["images"])
    assert result["images"][2]["source_commit"] is None
    assert result["images"][2]["upstream_reference"] == images.KAFKA_SOURCE


@pytest.mark.parametrize(
    "role,field,value",
    [
        ("producer", "architecture", "arm64"),
        ("spark", "os", "windows"),
        ("producer", "revision", "b" * 40),
        ("spark", "title", "unreviewed"),
        ("producer", "user", "root"),
        ("spark", "id", "sha256:invalid"),
        ("spark", "id", "sha256:" + "1" * 64),
        ("kafka", "repo_digests", ["apache/kafka:latest"]),
        ("kafka", "repo_digests", None),
    ],
)
def test_unreviewed_images_are_rejected(inspected, role, field, value):
    inspected[role][field] = value
    with pytest.raises(ValueError):
        images.publication_plan(inspected, COMMIT)


@pytest.mark.parametrize("commit", ["", "a" * 7, "A" * 40])
def test_full_source_commit_required(inspected, commit):
    with pytest.raises(ValueError, match="SCOPE_INVALID"):
        images.publication_plan(inspected, commit)


def test_missing_role_rejected(inspected):
    del inspected["kafka"]
    with pytest.raises(ValueError, match="SCOPE_INVALID"):
        images.publication_plan(inspected, COMMIT)


def test_context_uses_only_committed_build_inputs(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    for item in images.BUILD_PATHS:
        destination = repo / (item if item.startswith(".") else item + "/input.py")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("committed input\n")
    (repo / "outside.txt").write_text("out-of-scope tracked file")
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
    (repo / "runtime/input.py").write_text("unreviewed worktree")
    (repo / ".env").write_text("private untracked value")
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "untrusted-git"))
    first, second = tmp_path / "first.tar", tmp_path / "second.tar"
    receipt = images.build_context(repo, first)
    assert images.build_context(repo, second) == receipt
    assert first.read_bytes() == second.read_bytes()
    assert first.stat().st_mode & 0o777 == 0o600
    with tarfile.open(fileobj=io.BytesIO(first.read_bytes())) as archive:
        assert archive.extractfile("runtime/input.py").read() == b"committed input\n"
        assert ".env" not in archive.getnames()
        assert "outside.txt" not in archive.getnames()
    with pytest.raises(FileExistsError):
        images.build_context(repo, first)


def test_inspection_reads_only_allowlisted_fields_from_local_engine(
    inspected, monkeypatch
):
    calls = []
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-reach-docker")
    monkeypatch.setenv("DOCKER_HOST", "tcp://untrusted.invalid")

    def run(command, **kwargs):
        calls.append(command)
        assert not any(key.startswith(("AWS_", "DOCKER_")) for key in kwargs["env"])
        if command[1:3] == ["context", "inspect"]:
            stdout = json.dumps("unix:///local/docker.sock")
        else:
            assert command[1:5] == [
                "--host",
                "unix:///local/docker.sock",
                "image",
                "inspect",
            ]
            assert command[5:7] == ["--platform", "linux/amd64"]
            template = command[8]
            assert ".Config.Env" not in template
            assert "{{json .Config}}" not in template
            role = next(
                role for role, ref in images.LOCAL_IMAGES.items() if ref == command[-1]
            )
            row = inspected[role]
            stdout = "\n".join(
                json.dumps(row[key])
                for key in (
                    "id",
                    "os",
                    "architecture",
                    "user",
                    "repo_digests",
                    "revision",
                    "title",
                )
            )
        return subprocess.CompletedProcess(command, 0, stdout, "")

    monkeypatch.setattr(subprocess, "run", run)
    assert images.inspect_local_images() == inspected
    assert len(calls) == 4


@pytest.mark.parametrize("endpoint", ["tcp://remote:2376", "ssh://remote", None])
def test_remote_or_unknown_docker_engine_rejected(monkeypatch, endpoint):
    def run(command, **kwargs):
        assert command[1:3] == ["context", "inspect"]
        return subprocess.CompletedProcess(command, 0, json.dumps(endpoint), "")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(ValueError, match="LOCAL_UNIX_DOCKER_REQUIRED"):
        images.inspect_local_images()


def test_docker_errors_suppress_raw_output(monkeypatch):
    def run(command, **kwargs):
        return subprocess.CompletedProcess(
            command, 1, "private stdout", "private stderr"
        )

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(RuntimeError, match="^LOCAL_DOCKER_INSPECTION_FAILED$"):
        images.inspect_local_images()


def test_plan_does_not_modify_input(inspected):
    before = copy.deepcopy(inspected)
    images.publication_plan(inspected, COMMIT)
    assert inspected == before
