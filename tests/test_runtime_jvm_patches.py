"""Integrity and inventory contracts for build-only JVM family replacements."""

import copy
import hashlib
import json
from pathlib import Path

import pytest

from deploy.runtime import patch_jars


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def prepared(tmp_path):
    target, cache = tmp_path / "jars", tmp_path / "cache"
    target.mkdir()
    cache.mkdir()
    replacements = []
    for module in ("netty-common", "netty-handler"):
        before, after = (module + " old").encode(), (module + " patched").encode()
        old, new = module + "-1.jar", module + "-2.jar"
        (target / old).write_bytes(before)
        (cache / new).write_bytes(after)
        replacements.append(
            {
                "old": {"name": old, "sha512": hashlib.sha512(before).hexdigest()},
                "new": {
                    "path": f"io/netty/{module}/2/{new}",
                    "sha512": hashlib.sha512(after).hexdigest(),
                },
            }
        )
    lock = {
        "schema_version": 1,
        "pyspark_version": "4.0.1",
        "families": {"netty": "2", "tcnative": "2", "bouncycastle": "2", "jersey": "2"},
        "replacements": replacements,
    }
    (target / "unrelated.jar").write_bytes(b"preserve")
    return target, cache, lock


def test_replaces_only_hash_matched_files_and_is_idempotent(prepared):
    target, cache, lock = prepared
    patch_jars.apply(lock, target=target, cache=cache)
    patch_jars.apply(lock, target=target, cache=cache)
    assert {p.name for p in target.iterdir()} == {
        "netty-common-2.jar",
        "netty-handler-2.jar",
        "unrelated.jar",
    }
    assert (target / "unrelated.jar").read_bytes() == b"preserve"
    assert (target / "netty-handler-2.jar").read_bytes() == b"netty-handler patched"


@pytest.mark.parametrize(
    "failure",
    [
        "old_hash",
        "cached_hash",
        "cached_missing",
        "cached_symlink",
        "old_symlink",
        "extra_family",
        "partial",
    ],
)
def test_invalid_last_input_never_changes_earlier_installed_files(prepared, failure):
    target, cache, lock = prepared
    before = (target / "netty-common-1.jar").read_bytes()
    if failure == "old_hash":
        (target / "netty-handler-1.jar").write_bytes(b"unknown bundled version")
    elif failure == "cached_hash":
        (cache / "netty-handler-2.jar").write_bytes(b"corrupt")
    elif failure == "cached_missing":
        (cache / "netty-handler-2.jar").unlink()
    elif failure == "cached_symlink":
        (cache / "netty-handler-2.jar").unlink()
        (cache / "netty-handler-2.jar").symlink_to(cache / "netty-common-2.jar")
    elif failure == "old_symlink":
        (target / "netty-handler-1.jar").unlink()
        (target / "netty-handler-1.jar").symlink_to(target / "netty-common-1.jar")
    elif failure == "extra_family":
        (target / "netty-surprise-1.jar").write_bytes(b"unexpected")
    else:
        (target / "netty-handler-2.jar").write_bytes(b"partial prior replacement")
    with pytest.raises(ValueError):
        patch_jars.apply(lock, target=target, cache=cache)
    assert (target / "netty-common-1.jar").read_bytes() == before
    assert not (target / "netty-common-2.jar").exists()


@pytest.mark.parametrize(
    "change",
    [
        "traversal",
        "absolute",
        "duplicate",
        "different_family",
        "wrong_version",
        "other_registry_path",
        "different_artifact",
        "changed_classifier",
        "checksum",
    ],
)
def test_malformed_manifest_is_rejected_before_modifying_anything(prepared, change):
    target, cache, lock = prepared
    lock = copy.deepcopy(lock)
    row = lock["replacements"][-1]
    if change == "traversal":
        row["old"]["name"] = "../netty-handler-1.jar"
    elif change == "absolute":
        row["new"]["path"] = "/io/netty/netty-handler/2/netty-handler-2.jar"
    elif change == "duplicate":
        lock["replacements"].append(copy.deepcopy(row))
    elif change == "different_family":
        row["new"]["path"] = "org/bouncycastle/bcprov/2/bcprov-2.jar"
    elif change == "wrong_version":
        lock["families"]["netty"] = "3"
    elif change == "other_registry_path":
        row["new"]["path"] = "org/example/netty-handler/2/netty-handler-2.jar"
    elif change == "different_artifact":
        row["new"]["path"] = "io/netty/netty-common/2/netty-handler-2.jar"
    elif change == "changed_classifier":
        row["new"]["path"] = "io/netty/netty-handler/2/netty-handler-2-linux-x86_64.jar"
    else:
        row["new"]["sha512"] = "a" * 64
    with pytest.raises(ValueError):
        patch_jars.apply(lock, target=target, cache=cache)
    assert (target / "netty-common-1.jar").exists()
    assert not (target / "netty-common-2.jar").exists()


def test_committed_family_patches_are_covered_by_download_lock():
    lock = json.loads((ROOT / "deploy/runtime/spark-jvm-patches.lock.json").read_text())
    patch_jars.validate(lock)
    downloads = json.loads((ROOT / "deploy/runtime/jars.lock.json").read_text())
    assert all(row["new"] in downloads for row in lock["replacements"])


def test_override_files_and_manifest_are_integrity_checked(prepared, tmp_path):
    target, cache, lock = prepared
    patch_jars.apply(lock, target=target, cache=cache)
    output = tmp_path / "overrides"
    patch_jars.stage_overrides(lock, target=target, output=output)
    assert json.loads((output / "patches.lock.json").read_text()) == lock
    assert (output / "netty-handler-2.jar").read_bytes() == b"netty-handler patched"
    (output / "netty-handler-2.jar").write_bytes(b"corrupt override")
    with pytest.raises(ValueError, match="checksum"):
        patch_jars.stage_overrides(lock, target=target, output=output)


def test_override_manifest_symlink_cannot_overwrite_another_file(prepared, tmp_path):
    target, cache, lock = prepared
    patch_jars.apply(lock, target=target, cache=cache)
    output = tmp_path / "overrides"
    output.mkdir()
    unrelated = tmp_path / "preserved"
    unrelated.write_text("preserve")
    (output / "patches.lock.json").symlink_to(unrelated)
    with pytest.raises(ValueError, match="manifest"):
        patch_jars.stage_overrides(lock, target=target, output=output)
    assert unrelated.read_text() == "preserve"
