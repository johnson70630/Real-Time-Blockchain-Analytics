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
        "schema_version": 2,
        "pyspark_version": "4.0.1",
        "families": {
            "netty": "2",
            "tcnative": "2",
            "bouncycastle": "2",
            "jersey": "2",
            "hadoop": "3.4.3",
            "jackson": "2.18.12",
            "aircompressor": "2.0.3",
            "snappy": "1.1.10.12",
            "guava": "33.7.2-jre",
            "failureaccess": "1.0.3",
            "jline": "3.30.16",
            "zookeeper": "3.9.7",
        },
        "components": {
            "hadoop": "3.4.3",
            "aws_sdk": "2.35.4",
            "wildfly_openssl": "2.2.5.Final",
            "analytics_accelerator": "1.3.1",
        },
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
    patch_jars.validate_downloads(lock, downloads)


@pytest.mark.parametrize(
    "artifact",
    [
        "hadoop-client-api",
        "hadoop-client-runtime",
        "hadoop-aws",
        "bundle",
        "wildfly-openssl",
        "analyticsaccelerator-s3",
    ],
)
@pytest.mark.parametrize("failure", ["missing", "old_version", "duplicate"])
def test_mixed_or_missing_s3_cohort_is_rejected(artifact, failure):
    lock = json.loads((ROOT / "deploy/runtime/spark-jvm-patches.lock.json").read_text())
    downloads = json.loads((ROOT / "deploy/runtime/jars.lock.json").read_text())
    row = next(item for item in downloads if item["path"].split("/")[-3] == artifact)
    if failure == "missing":
        downloads.remove(row)
    elif failure == "old_version":
        version = row["path"].split("/")[-2]
        row["path"] = row["path"].replace(version, "0.0.1")
    else:
        downloads.append(copy.deepcopy(row))
    with pytest.raises(ValueError, match="dependency cohort"):
        patch_jars.validate_downloads(lock, downloads)


def test_unsupported_sdk_pair_fails_before_file_replacement(prepared):
    target, cache, lock = prepared
    lock["components"]["aws_sdk"] = "2.24.6"
    with pytest.raises(ValueError, match="scope"):
        patch_jars.apply(lock, target=target, cache=cache)
    assert (target / "netty-common-1.jar").exists()


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


def test_matched_hadoop_client_is_not_duplicated_into_override_directory(
    prepared, tmp_path
):
    target, cache, lock = prepared
    old, new = b"old API", b"matched new API"
    (target / "hadoop-client-api-3.4.1.jar").write_bytes(old)
    (cache / "hadoop-client-api-3.4.3.jar").write_bytes(new)
    lock["replacements"].append(
        {
            "old": {
                "name": "hadoop-client-api-3.4.1.jar",
                "sha512": hashlib.sha512(old).hexdigest(),
            },
            "new": {
                "path": "org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar",
                "sha512": hashlib.sha512(new).hexdigest(),
            },
        }
    )
    patch_jars.apply(lock, target=target, cache=cache)
    output = tmp_path / "overrides"
    patch_jars.stage_overrides(lock, target=target, output=output)
    assert (target / "hadoop-client-api-3.4.3.jar").read_bytes() == new
    assert not (output / "hadoop-client-api-3.4.3.jar").exists()
    assert (
        json.loads((output / "patches.lock.json").read_text())["components"]["hadoop"]
        == "3.4.3"
    )


@pytest.mark.parametrize(
    "artifact,group,family",
    [
        *[
            (artifact, group.rstrip("/"), "jackson")
            for artifact, group in patch_jars.JACKSON_ARTIFACTS.items()
        ],
        ("aircompressor", "io/airlift", "aircompressor"),
        ("snappy-java", "org/xerial/snappy", "snappy"),
        ("guava", "com/google/guava", "guava"),
        ("failureaccess", "com/google/guava", "failureaccess"),
        ("jline", "org/jline", "jline"),
        ("zookeeper", "org/apache/zookeeper", "zookeeper"),
        ("zookeeper-jute", "org/apache/zookeeper", "zookeeper"),
    ],
)
def test_normal_directory_patches_preserve_legacy_json_and_avoid_duplicate_copies(
    prepared, tmp_path, artifact, group, family
):
    target, cache, lock = prepared
    version = lock["families"][family]
    name = f"{artifact}-{version}.jar"
    old = f"{artifact}-1.0{'-jre' if family == 'guava' else ''}.jar"
    (target / old).write_bytes(b"old component")
    (cache / name).write_bytes(b"patched component")
    lock["replacements"].append(
        {
            "old": {
                "name": old,
                "sha512": hashlib.sha512(b"old component").hexdigest(),
            },
            "new": {
                "path": f"{group}/{artifact}/{version}/{name}",
                "sha512": hashlib.sha512(b"patched component").hexdigest(),
            },
        }
    )
    legacy = target / "jackson-core-asl-1.9.13.jar"
    legacy.write_bytes(b"separate feature removal")
    jline2 = target / "jline-2.14.6.jar"
    jline2.write_bytes(b"separate JLine API")
    patch_jars.apply(lock, cache=cache, target=target)
    output = tmp_path / "overrides"
    patch_jars.stage_overrides(lock, target=target, output=output)
    assert (target / name).read_bytes() == b"patched component"
    assert not (output / name).exists()
    assert legacy.read_bytes() == b"separate feature removal"
    assert jline2.read_bytes() == b"separate JLine API"


def test_legacy_codehaus_json_cannot_be_replaced_as_modern_jackson(prepared):
    target, cache, lock = prepared
    lock["replacements"][-1]["old"]["name"] = "jackson-core-asl-1.9.13.jar"
    with pytest.raises(ValueError, match="Legacy JSON"):
        patch_jars.apply(lock, cache=cache, target=target)
    assert (target / "netty-common-1.jar").exists()


@pytest.mark.parametrize("version", ["33.7.2-android", "33.7.2"])
def test_guava_requires_the_reviewed_jre_variant(version):
    lock = json.loads((ROOT / "deploy/runtime/spark-jvm-patches.lock.json").read_text())
    lock["families"]["guava"] = version
    with pytest.raises(ValueError, match="Guava JRE"):
        patch_jars.validate(lock)


def test_jline2_cannot_be_replaced_as_jline3(prepared):
    target, cache, lock = prepared
    lock["replacements"][-1]["old"]["name"] = "jline-2.14.6.jar"
    with pytest.raises(ValueError, match="separate API"):
        patch_jars.apply(lock, cache=cache, target=target)
    assert (target / "netty-common-1.jar").exists()


@pytest.mark.parametrize(
    "artifact", ["guava", "failureaccess", "jline", "zookeeper", "zookeeper-jute"]
)
@pytest.mark.parametrize("change", ["old_version", "duplicate"])
def test_download_inventory_cannot_reintroduce_old_or_duplicate_client_jars(
    artifact, change
):
    lock = json.loads((ROOT / "deploy/runtime/spark-jvm-patches.lock.json").read_text())
    downloads = json.loads((ROOT / "deploy/runtime/jars.lock.json").read_text())
    row = copy.deepcopy(
        next(r for r in downloads if r["path"].split("/")[-3] == artifact)
    )
    if change == "old_version":
        row["path"] = row["path"].replace(row["path"].split("/")[-2], "0.0.1")
    downloads.append(row)
    with pytest.raises(ValueError, match="JVM download inventory"):
        patch_jars.validate_downloads(lock, downloads)
