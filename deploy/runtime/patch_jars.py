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


PREFIXES = ("netty-", "bcprov-", "bcpkix-", "bcutil-", "jersey-")


def family(name):
    if name.startswith("netty-tcnative-"):
        return "tcnative"
    if name.startswith("netty-"):
        return "netty"
    if name.startswith(("bcprov-", "bcpkix-", "bcutil-")):
        return "bouncycastle"
    if name.startswith("jersey-"):
        return "jersey"
    raise ValueError("Unreviewed JVM family")


def validate(lock):
    if (
        lock.get("schema_version") != 1
        or lock.get("pyspark_version") != "4.0.1"
        or set(lock.get("families", {}))
        != {"netty", "tcnative", "bouncycastle", "jersey"}
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
        }[group]
        if (
            group != family(destination)
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
        if p.name.startswith(PREFIXES) and p.name.endswith(".jar")
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


def stage_overrides(lock, *, target, output):
    """Give patched families explicit precedence over unmodified shaded bundles."""
    _, names = validate(lock)
    if target.is_symlink() or not target.is_dir() or output.is_symlink():
        raise ValueError("Regular JVM override directories required")
    output.mkdir(parents=True, exist_ok=True)
    if any(p.name not in names | {"patches.lock.json"} for p in output.iterdir()):
        raise ValueError("Unexpected JVM override content")
    for row in lock["replacements"]:
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
    args = parser.parse_args()
    lock = json.loads(args.lock.read_text())
    apply(lock, cache=args.cache)
    import pyspark

    stage_overrides(
        lock, target=Path(pyspark.__file__).parent / "jars", output=args.overrides
    )
