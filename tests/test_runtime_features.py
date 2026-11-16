"""Prune exact optional modules and reject their return before cloud writes."""

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from deploy.runtime import prune_features
from runtime import features

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def prepared(tmp_path):
    target = tmp_path / "jars"
    target.mkdir()
    rows = []
    for name in (
        "connect-repl/client-1.jar",
        "hive-common-1.jar",
        "spark-hive-thriftserver_2.13-4.0.1.jar",
        "derby-1.jar",
        "jackson-mapper-asl-1.jar",
        "libthrift-0.16.0.jar",
        "libfb303-0.9.3.jar",
    ):
        path = target / name
        path.parent.mkdir(exist_ok=True)
        content = name.encode()
        path.write_bytes(content)
        rows.append(
            {
                "path": name,
                "feature": features.feature(name),
                "sha512": hashlib.sha512(content).hexdigest(),
                "size_bytes": len(content),
            }
        )
    for name in (
        "hive-storage-api-2.8.1.jar",
        "parquet-format-structures-1.15.2.jar",
        "spark-sql_2.13-4.0.1.jar",
    ):
        (target / name).write_bytes(b"shared dependency")
    lock = {
        "schema_version": 1,
        "pyspark_version": "4.0.1",
        "profile": features.PROFILE,
        "excluded_features": list(features.EXCLUDED),
        "removals": rows,
    }
    return target, lock, tmp_path / "receipt.json"


def test_exact_removal_preserves_shared_serialization_and_is_idempotent(prepared):
    target, lock, manifest = prepared
    for _ in range(2):
        prune_features.apply(lock, target=target, manifest=manifest)
    assert not features.inventory(target)
    assert (target / "hive-storage-api-2.8.1.jar").read_bytes() == b"shared dependency"
    assert (target / "parquet-format-structures-1.15.2.jar").read_bytes() == (
        b"shared dependency"
    )
    assert json.loads(manifest.read_text()) == lock


@pytest.mark.parametrize(
    "failure",
    [
        "hash",
        "size",
        "unknown",
        "missing",
        "symlink",
        "connect_symlink",
        "receipt_symlink",
    ],
)
def test_full_validation_precedes_any_removal(prepared, failure, tmp_path):
    target, lock, manifest = prepared
    last = target / lock["removals"][-1]["path"]
    if failure == "hash":
        lock["removals"][-1]["sha512"] = "a" * 128
    elif failure == "size":
        lock["removals"][-1]["size_bytes"] += 1
    elif failure == "unknown":
        (target / "hive-surprise-1.jar").write_bytes(b"unknown")
    elif failure == "missing":
        last.unlink()
    elif failure == "symlink":
        last.unlink()
        last.symlink_to(target / "derby-1.jar")
    elif failure == "connect_symlink":
        directory = target / "connect-repl"
        directory.rename(target.parent / "external")
        directory.symlink_to(target.parent / "external", target_is_directory=True)
    else:
        preserved = tmp_path / "preserved"
        preserved.write_text("preserve")
        manifest.symlink_to(preserved)
    with pytest.raises(ValueError):
        prune_features.apply(lock, target=target, manifest=manifest)
    assert (target / "hive-common-1.jar").exists()
    assert (target / "derby-1.jar").exists()


@pytest.mark.parametrize(
    "failure",
    [
        "absolute",
        "traversal",
        "duplicate",
        "shared",
        "incomplete",
        "wrong_group",
        "checksum",
        "version",
    ],
)
def test_unreviewed_removal_manifest_is_rejected(prepared, failure):
    target, original, manifest = prepared
    lock = copy.deepcopy(original)
    row = lock["removals"][-1]
    if failure == "absolute":
        row["path"] = "/jackson-mapper-asl-1.jar"
    elif failure == "traversal":
        row["path"] = "connect-repl/../jackson-mapper-asl-1.jar"
    elif failure == "duplicate":
        lock["removals"].append(copy.deepcopy(row))
    elif failure == "shared":
        row["path"] = "hive-storage-api-2.8.1.jar"
    elif failure == "incomplete":
        lock["removals"].pop()
    elif failure == "wrong_group":
        row["feature"] = "hive_catalog"
    elif failure == "checksum":
        row["sha512"] = "a" * 64
    else:
        lock["pyspark_version"] = "4.0.2"
    with pytest.raises(ValueError):
        prune_features.apply(lock, target=target, manifest=manifest)
    assert (target / original["removals"][-1]["path"]).exists()


def test_absent_modules_require_a_matching_previous_receipt(prepared):
    target, lock, manifest = prepared
    prune_features.apply(lock, target=target, manifest=manifest)
    manifest.unlink()
    with pytest.raises(ValueError, match="receipt"):
        prune_features.apply(lock, target=target, manifest=manifest)


def test_conflicting_receipt_is_rejected_before_removal(prepared):
    target, lock, manifest = prepared
    manifest.write_text("{}")
    with pytest.raises(ValueError, match="Conflicting"):
        prune_features.apply(lock, target=target, manifest=manifest)
    assert features.inventory(target) == features.validate(lock)


@pytest.mark.parametrize(
    "failure", [None, "missing_receipt", "restored", "renamed_class", "changed_profile"]
)
def test_runtime_rejects_restored_modules_and_classes(prepared, monkeypatch, failure):
    target, lock, manifest = prepared
    prune_features.apply(lock, target=target, manifest=manifest)
    monkeypatch.setattr(features, "JARS", target)
    monkeypatch.setattr(features, "MANIFEST", manifest)
    loader = NS(
        getResources=lambda _: NS(hasMoreElements=lambda: failure == "renamed_class")
    )
    if failure == "missing_receipt":
        manifest.unlink()
    elif failure == "restored":
        (target / "derby-new.jar").write_bytes(b"unexpected")
    elif failure == "changed_profile":
        lock["profile"] = "general_purpose"
        manifest.write_text(json.dumps(lock))
    if failure:
        with pytest.raises(
            RuntimeError, match="PILOT_STREAMING_FEATURE_PROFILE_REQUIRED"
        ):
            features.verify_profile(loader)
    else:
        assert features.verify_profile(loader)["excluded_classes_resolvable"] is False


def test_committed_profile_matches_cloud_build_and_retains_storage_api():
    lock = json.loads((ROOT / "deploy/runtime/spark-features.lock.json").read_text())
    names = features.validate(lock)
    assert len(names) == 63
    assert not any(
        "hive-storage-api" in name or "parquet-format-structures" in name
        for name in names
    )
    assert {"libthrift-0.16.0.jar", "libfb303-0.9.3.jar"} <= names
    assert "spark-connect_2.13-4.0.1.jar" in names
    assert "connect-repl/spark-connect-client-jvm_2.13-4.0.1.jar" in names


@pytest.mark.parametrize("resource", features.FORBIDDEN_CLASSES)
def test_every_excluded_resource_is_checked_including_standalone_rpc(
    prepared, monkeypatch, resource
):
    target, lock, manifest = prepared
    prune_features.apply(lock, target=target, manifest=manifest)
    monkeypatch.setattr(features, "JARS", target)
    monkeypatch.setattr(features, "MANIFEST", manifest)
    loader = NS(
        getResources=lambda name: NS(
            hasMoreElements=lambda: name == resource.replace(".", "/") + ".class"
        )
    )
    with pytest.raises(RuntimeError, match="PILOT_STREAMING_FEATURE_PROFILE_REQUIRED"):
        features.verify_profile(loader)
