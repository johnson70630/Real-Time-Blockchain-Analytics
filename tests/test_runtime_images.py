"""Offline source provenance and publication contract tests; no registry access."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import subprocess
import tarfile

import pytest
import yaml

from runtime import images

COMMIT = "a" * 40


def test_cloud_image_pins_match_reviewed_platform_lock_and_java17():
    root = images.ROOT
    lock = json.loads((root / "deploy/runtime/images.lock.json").read_text())
    assert lock["platform"] == "linux/amd64"
    rows = {row["role"]: row for row in lock["sources"]}
    assert set(rows) == {"python", "java17", "kafka", "kafka_java21"}
    for row in rows.values():
        assert row["pinned_reference"].endswith("@" + row["index_digest"])
        for key in ("index_digest", "amd64_manifest_digest", "config_digest"):
            assert len(row[key]) == 71 and row[key].startswith("sha256:")
    producer = (root / "deploy/runtime/Dockerfile.producer").read_text()
    spark = (root / "deploy/runtime/Dockerfile.spark").read_text()
    for dockerfile in (producer, spark):
        assert "ARG PYTHON_BASE=" + rows["python"]["pinned_reference"] in dockerfile
        assert "apt-get upgrade -y" in dockerfile
        assert 'LANG=C.UTF-8 LC_ALL=C.UTF-8 LANGUAGE=""' in dockerfile
    assert "ARG JAVA17_BASE=" + rows["java17"]["pinned_reference"] in spark
    assert rows["java17"]["selected_environment"]["JAVA_VERSION"].startswith("jdk-17.")
    assert "COPY --from=java17 /opt/java/openjdk /opt/java/openjdk" in spark
    assert 'JAVA_TOOL_OPTIONS="-Djava.awt.headless=true"' in spark
    assert "fontconfig" not in spark and "libfreetype6" not in spark
    assert "openjdk-21" not in spark
    compose = yaml.safe_load((root / "deploy/runtime/compose.test.yaml").read_text())
    assert compose["services"]["kafka"]["image"] == images.LOCAL_IMAGES["kafka"]
    assert images.KAFKA_SOURCE.endswith("@" + rows["kafka"]["index_digest"])
    kafka = (root / "deploy/runtime/Dockerfile.kafka").read_text()
    assert "ARG KAFKA_BASE=" + images.KAFKA_SOURCE in kafka
    assert "RUN apk upgrade --no-cache" in kafka
    assert "USER 1000:1000" in kafka
    assert "ARG JAVA21_BASE=" + rows["kafka_java21"]["pinned_reference"] in kafka
    assert "COPY --from=java21 /opt/java/openjdk /opt/java/openjdk" in kafka
    assert rows["kafka_java21"]["selected_environment"]["JAVA_VERSION"] == "jdk-21.0.13+9"
    assert "rm -f /opt/kafka/storage.jsa /opt/kafka/kafka.jsa" in kafka
    assert "sed -i" in kafka
    assert rows["kafka"]["upstream_tag"] == "apache/kafka:" + images.KAFKA_VERSION
    jars = json.loads((root / "deploy/runtime/jars.lock.json").read_text())
    clients = [row for row in jars if row["path"].startswith("org/apache/kafka/kafka-clients/")]
    assert len(clients) == 1
    assert clients[0]["path"] == "org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar"


def committed_jar_repo(tmp_path, lock=None):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    for item in images.BUILD_PATHS:
        path = repo / (item if item.startswith(".") else item + "/input.py")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("committed input\n")
    content = b"synthetic pinned dependency"
    lock = lock or [{"path": "org/example/1/dependency-1.jar", "sha512": hashlib.sha512(content).hexdigest()}]
    (repo / "deploy/runtime/jars.lock.json").write_text(json.dumps(lock))
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=Offline", "-c", "user.email=offline@example.invalid", "commit", "-qm", "fixture"], check=True)
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "dependency-1.jar").write_bytes(content)
    return repo, cache, content


def test_verified_dependency_context_is_repeatable_and_excludes_unlisted_cache_files(tmp_path):
    repo, cache, content = committed_jar_repo(tmp_path)
    (cache / ".env").write_text("untracked private data")
    (cache / "unlisted.jar").write_bytes(b"unreviewed")
    (repo / "deploy/runtime/jars.lock.json").write_text("dirty lock must not be used")
    first, second = tmp_path / "first.tar", tmp_path / "second.tar"
    receipt = images.build_context(repo, first, cache)
    assert images.build_context(repo, second, cache) == receipt
    assert first.read_bytes() == second.read_bytes()
    assert first.stat().st_mode & 0o777 == 0o600
    assert receipt["verified_maven_artifacts"] == 1
    assert receipt["verified_maven_bytes"] == len(content)
    with tarfile.open(first) as archive:
        jar = images.JAR_CONTEXT_PREFIX + "/dependency-1.jar"
        assert archive.extractfile(jar).read() == content
        assert archive.getmember(jar).mode == 0o644
        assert not any("unlisted" in n or ".env" in n for n in archive.getnames())
        assert json.load(archive.extractfile("deploy/runtime/jars.lock.json"))[0]["sha512"] == hashlib.sha512(content).hexdigest()


def test_cached_context_starts_with_global_header_for_docker_stdin_detection(tmp_path):
    repo, cache, _ = committed_jar_repo(tmp_path)
    output = tmp_path / "context.tar"
    receipt = images.build_context(repo, output, cache)
    # Docker's Go tar reader sees only the first 1024 bytes when deciding
    # whether stdin is an archive. A global header is returned immediately;
    # a per-file extended header requires another header outside that window.
    prefix = output.read_bytes()[:1024]
    header = tarfile.TarInfo.frombuf(prefix[:512], "utf-8", "strict")
    assert header.type == tarfile.XGLTYPE
    assert 0 < header.size <= 512
    assert b"comment=" + receipt["source_commit"].encode() + b"\n" in prefix[512:]
    with tarfile.open(output) as archive:
        assert archive.pax_headers["comment"] == receipt["source_commit"]
        assert archive.extractfile(".dockerignore").read() == b"committed input\n"


@pytest.mark.parametrize("kind", ["corrupt", "missing", "symlink", "oversize"])
def test_untrusted_cached_artifacts_fail_before_context_creation(tmp_path, monkeypatch, kind):
    repo, cache, content = committed_jar_repo(tmp_path)
    jar = cache / "dependency-1.jar"
    if kind == "corrupt":
        jar.write_bytes(b"unreviewed")
    elif kind == "missing":
        jar.unlink()
    elif kind == "symlink":
        jar.unlink()
        secret = tmp_path / "private"
        secret.write_bytes(content)
        jar.symlink_to(secret)
    else:
        monkeypatch.setattr(images, "MAX_JAR_CACHE_BYTES", len(content) - 1)
    output = tmp_path / "context.tar"
    with pytest.raises(ValueError):
        images.build_context(repo, output, cache)
    assert not output.exists()


def test_dependency_cache_must_match_committed_lock_even_if_worktree_lock_matches_bad_data(tmp_path):
    repo, cache, _ = committed_jar_repo(tmp_path)
    bad = b"unreviewed"
    (cache / "dependency-1.jar").write_bytes(bad)
    (repo / "deploy/runtime/jars.lock.json").write_text(json.dumps([
        {"path": "org/example/1/dependency-1.jar", "sha512": hashlib.sha512(bad).hexdigest()}
    ]))
    with pytest.raises(ValueError, match="CHECKSUM"):
        images.build_context(repo, tmp_path / "context.tar", cache)


def test_duplicate_dependency_basenames_are_rejected(tmp_path):
    pin = hashlib.sha512(b"synthetic pinned dependency").hexdigest()
    repo, cache, _ = committed_jar_repo(tmp_path, [
        {"path": "one/dependency-1.jar", "sha512": pin},
        {"path": "two/dependency-1.jar", "sha512": pin},
    ])
    with pytest.raises(ValueError, match="LOCK_INVALID"):
        images.build_context(repo, tmp_path / "context.tar", cache)


@pytest.fixture
def inspected():
    result = {}
    for index, role in enumerate(images.LOCAL_IMAGES, start=1):
        result[role] = {
            "id": "sha256:" + str(index) * 64,
            "os": "linux",
            "architecture": "amd64",
            "user": "1000:1000" if role == "kafka" else "10001:10001",
            "revision": COMMIT,
            "title": "blockchain-runtime-" + role,
            "base_reference": images.KAFKA_SOURCE if role == "kafka" else None,
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
        f"{images.REGISTRY}/blockchain-runtime-producer:kafka-{images.KAFKA_VERSION}-{COMMIT}",
    ]
    assert [item["target"] for item in result["images"]] == expected
    assert [item["local_reference"] for item in result["images"]] == list(images.LOCAL_IMAGES.values())
    assert all(item["ecr_deployment_digest"] is None for item in result["images"])
    assert all(item["scan_review"] == "pending" for item in result["images"])
    assert result["images"][2]["source_commit"] == COMMIT
    assert result["images"][2]["upstream_reference"] == images.KAFKA_SOURCE


@pytest.mark.parametrize("role", ["producer", "spark", "kafka"])
def test_rejected_manifest_cannot_be_planned_even_with_claimed_current_labels(inspected, role):
    evidence = json.loads((images.ROOT / "docs/evidence/aws_runtime_ecr_publication_2026-11-16.json").read_text())
    rejected = next(row for row in evidence["images"] if row["role"] == role)
    inspected[role]["id"] = rejected["ecr_deployment_digest"]
    with pytest.raises(ValueError, match="REJECTED_IMAGE_RELEASE"):
        images.publication_plan(inspected, COMMIT)


def test_os_only_kafka_candidate_is_rejected_by_publication_review(inspected):
    evidence = json.loads((images.ROOT / "docs/evidence/aws_runtime_image_remediation_2026-11-16.json").read_text())
    inspected["kafka"]["id"] = evidence["kafka_jre_ca_review"]["rejected_intermediate_manifest"]
    with pytest.raises(ValueError, match="REJECTED_IMAGE_RELEASE"):
        images.publication_plan(inspected, COMMIT)


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
        ("kafka", "base_reference", "apache/kafka:latest"),
        ("kafka", "base_reference", None),
        ("kafka", "revision", "b" * 40),
        ("kafka", "title", "unreviewed"),
        ("kafka", "user", "root"),
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
                    "base_reference",
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
