"""Build-only replacement of exact reviewed Spark JARs; never run at startup."""

import argparse
import json
import re
import tempfile
from pathlib import Path

if __package__:
    from . import fetch_jars
else:
    import fetch_jars


PREFIXES = (
    "netty-",
    "bcprov-",
    "bcpkix-",
    "bcutil-",
    "jersey-",
    "hadoop-client-api-",
    "hadoop-client-runtime-",
    "jackson-",
    "aircompressor-",
    "snappy-java-",
)
LEGACY_JSON_PREFIXES = ("jackson-core-asl-", "jackson-mapper-asl-")
NORMAL_FAMILIES = {"hadoop", "jackson", "aircompressor", "snappy"}
JACKSON_ARTIFACTS = {
    "jackson-annotations": "com/fasterxml/jackson/core/",
    "jackson-core": "com/fasterxml/jackson/core/",
    "jackson-databind": "com/fasterxml/jackson/core/",
    "jackson-dataformat-yaml": "com/fasterxml/jackson/dataformat/",
    "jackson-datatype-jsr310": "com/fasterxml/jackson/datatype/",
    "jackson-module-scala_2.13": "com/fasterxml/jackson/module/",
}
COMPONENTS = {
    "hadoop": "3.4.3",
    "aws_sdk": "2.35.4",
    "wildfly_openssl": "2.2.5.Final",
    "analytics_accelerator": "1.3.1",
}


def family(name):
    if name.startswith(LEGACY_JSON_PREFIXES):
        raise ValueError("Legacy JSON belongs to the separate feature-removal profile")
    if name.startswith("jackson-"):
        return "jackson"
    if name.startswith("aircompressor-"):
        return "aircompressor"
    if name.startswith("snappy-java-"):
        return "snappy"
    if name.startswith("netty-tcnative-"):
        return "tcnative"
    if name.startswith("netty-"):
        return "netty"
    if name.startswith(("bcprov-", "bcpkix-", "bcutil-")):
        return "bouncycastle"
    if name.startswith("jersey-"):
        return "jersey"
    if name.startswith(("hadoop-client-api-", "hadoop-client-runtime-")):
        return "hadoop"
    raise ValueError("Unreviewed JVM family")


def validate(lock):
    if (
        lock.get("schema_version") != 2
        or lock.get("pyspark_version") != "4.0.1"
        or set(lock.get("families", {}))
        != {
            "netty",
            "tcnative",
            "bouncycastle",
            "jersey",
            "hadoop",
            "jackson",
            "aircompressor",
            "snappy",
        }
        or lock.get("components") != COMPONENTS
        or lock["families"]["hadoop"] != COMPONENTS["hadoop"]
        or not 1 <= len(lock.get("replacements", [])) <= 100
    ):
        raise ValueError("Invalid JVM patch scope")
    old_names, new_names = set(), set()
    for row in lock["replacements"]:
        old, new = row["old"], row["new"]
        path, name = new["path"], old["name"]
        if (
            not re.fullmatch(r"[A-Za-z0-9_.-]+\.jar", name)
            or ".." in name
            or not re.fullmatch(r"[A-Za-z0-9_./-]+\.jar", path)
            or len(path.split("/")) < 4
            or ".." in path
            or path.startswith("/")
            or not re.fullmatch("[0-9a-f]{128}", old["sha512"])
            or not re.fullmatch("[0-9a-f]{128}", new["sha512"])
        ):
            raise ValueError("Invalid JVM patch path or checksum")
        destination = path.rsplit("/", 1)[-1]
        group = family(name)
        version = lock["families"][group]
        artifact = path.split("/")[-3]
        start = f"{artifact}-{version}"
        suffix = destination.removeprefix(start)
        prefix = {
            "netty": "io/netty/",
            "tcnative": "io/netty/",
            "bouncycastle": "org/bouncycastle/",
            "jersey": "org/glassfish/jersey/",
            "hadoop": "org/apache/hadoop/",
            "jackson": JACKSON_ARTIFACTS.get(artifact),
            "aircompressor": "io/airlift/",
            "snappy": "org/xerial/snappy/",
        }[group]
        if (
            prefix is None
            or group != family(destination)
            or name == destination
            or not destination.startswith(start)
            or not re.fullmatch(r"(?:-[A-Za-z0-9_-]+)?\.jar", suffix)
            or not re.fullmatch(
                re.escape(artifact) + r"-[0-9][A-Za-z0-9.]*" + re.escape(suffix), name
            )
            or not path.startswith(prefix)
            or not path.endswith(f"/{version}/{destination}")
            or f"-{version}" not in destination
            or name in old_names
            or destination in new_names
        ):
            raise ValueError("Duplicate or inconsistent JVM family replacement")
        old_names.add(name)
        new_names.add(destination)
    if old_names & new_names:
        raise ValueError("Overlapping JVM replacement names")
    return old_names, new_names


def apply(lock, *, cache, target=None):
    old_names, new_names = validate(lock)
    if target is None:
        import pyspark

        if pyspark.__version__ != lock["pyspark_version"]:
            raise ValueError("Reviewed PySpark version required")
        target = Path(pyspark.__file__).parent / "jars"
    if (
        target.is_symlink()
        or not target.is_dir()
        or cache.is_symlink()
        or not cache.is_dir()
    ):
        raise ValueError("Regular JVM target and cache directories required")
    actual = {
        p.name
        for p in target.iterdir()
        if p.name.startswith(PREFIXES)
        and p.name.endswith(".jar")
        and not p.name.startswith(LEGACY_JSON_PREFIXES)
    }
    if actual == new_names:
        for row in lock["replacements"]:
            fetch_jars.verified(
                target / row["new"]["path"].rsplit("/", 1)[-1], row["new"]["sha512"]
            )
        return
    if actual != old_names:
        raise ValueError("Unexpected or partially patched JVM family inventory")
    # Validate every old file and every cached replacement before changing any
    # installed JAR. Never glob-delete, modify shaded bundles or hide metadata.
    with tempfile.TemporaryDirectory(
        prefix=".reviewed-jvm-", dir=target.parent
    ) as directory:
        staged = Path(directory)
        for row in lock["replacements"]:
            old, new = row["old"], row["new"]
            fetch_jars.verified(target / old["name"], old["sha512"])
            name = new["path"].rsplit("/", 1)[-1]
            source = cache / name
            fetch_jars.verified(source, new["sha512"])
            with source.open("rb") as handle:
                fetch_jars.write_verified(handle, staged / name, new["sha512"])
        for name in sorted(new_names):
            (staged / name).replace(target / name)
        for name in sorted(old_names):
            (target / name).unlink()
    print(f"SPARK_JVM_PATCHES_APPLIED replacements={len(new_names)}")


def validate_downloads(lock, downloads):
    """Reject an SDK/connector mismatch before changing the bundled clients."""
    validate(lock)
    required = {
        "hadoop-client-api": ("org/apache/hadoop", COMPONENTS["hadoop"]),
        "hadoop-client-runtime": ("org/apache/hadoop", COMPONENTS["hadoop"]),
        "hadoop-aws": ("org/apache/hadoop", COMPONENTS["hadoop"]),
        "bundle": ("software/amazon/awssdk", COMPONENTS["aws_sdk"]),
        "wildfly-openssl": ("org/wildfly/openssl", COMPONENTS["wildfly_openssl"]),
        "analyticsaccelerator-s3": (
            "software/amazon/s3/analyticsaccelerator",
            COMPONENTS["analytics_accelerator"],
        ),
    }
    for artifact, (group, version) in required.items():
        expected = f"{group}/{artifact}/{version}/{artifact}-{version}.jar"
        matches = [row for row in downloads if row["path"].split("/")[-3] == artifact]
        if len(matches) != 1 or matches[0]["path"] != expected:
            raise ValueError("Unmatched Hadoop/S3 dependency cohort")
    if any(row["new"] not in downloads for row in lock["replacements"]):
        raise ValueError("JVM replacement missing from download lock")


def stage_overrides(lock, *, target, output):
    """Give patched families explicit precedence over unmodified shaded bundles."""
    validate(lock)
    # Hadoop, Jackson and compression libraries use the normal JAR directory.
    # Startup verifies their actual origins; avoid redundant scanner occurrences.
    overrides = [
        row
        for row in lock["replacements"]
        if family(row["old"]["name"]) not in NORMAL_FAMILIES
    ]
    names = {row["new"]["path"].rsplit("/", 1)[-1] for row in overrides}
    if target.is_symlink() or not target.is_dir() or output.is_symlink():
        raise ValueError("Regular JVM override directories required")
    output.mkdir(parents=True, exist_ok=True)
    if any(p.name not in names | {"patches.lock.json"} for p in output.iterdir()):
        raise ValueError("Unexpected JVM override content")
    for row in overrides:
        new = row["new"]
        name = new["path"].rsplit("/", 1)[-1]
        source, destination = target / name, output / name
        fetch_jars.verified(source, new["sha512"])
        if destination.exists() or destination.is_symlink():
            fetch_jars.verified(destination, new["sha512"])
        else:
            with source.open("rb") as handle:
                fetch_jars.write_verified(handle, destination, new["sha512"])
    manifest = output / "patches.lock.json"
    if manifest.is_symlink():
        raise ValueError("Regular JVM override manifest required")
    manifest.write_text(json.dumps(lock, indent=2) + "\n")
    manifest.chmod(0o644)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lock", type=Path)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--overrides", type=Path, required=True)
    parser.add_argument("--downloads", type=Path, required=True)
    args = parser.parse_args()
    lock = json.loads(args.lock.read_text())
    validate_downloads(lock, json.loads(args.downloads.read_text()))
    apply(lock, cache=args.cache)
    import pyspark

    stage_overrides(
        lock, target=Path(pyspark.__file__).parent / "jars", output=args.overrides
    )
