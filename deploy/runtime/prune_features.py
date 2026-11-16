"""Build-only removal of exact, hash-reviewed optional Spark modules."""

import argparse
import json
from pathlib import Path

if __package__:
    from . import fetch_jars
    from runtime import features
else:
    import fetch_jars
    import runtime_features as features


def apply(lock, *, target=None, manifest):
    names = features.validate(lock)
    if target is None:
        import pyspark

        if pyspark.__version__ != lock["pyspark_version"]:
            raise ValueError("Reviewed PySpark version required")
        target = Path(pyspark.__file__).parent / "jars"
    actual = features.inventory(target)
    if manifest.is_symlink() or not manifest.parent.is_dir():
        raise ValueError("Regular streaming profile destination required")
    if manifest.exists() and (
        not manifest.is_file() or json.loads(manifest.read_text()) != lock
    ):
        raise ValueError("Conflicting streaming profile receipt")
    if actual:
        if actual != names:
            raise ValueError("Unexpected or partially pruned feature inventory")
        # Check every input before removing any file. Keep hive-storage-api and
        # Parquet's relocated Thrift intact; standalone RPC modules are unused
        # by this in-memory streaming profile.
        for row in lock["removals"]:
            path = target / row["path"]
            if path.stat().st_size != row["size_bytes"]:
                raise ValueError("Feature JAR size mismatch")
            fetch_jars.verified(path, row["sha512"])
        for name in sorted(names):
            (target / name).unlink()
    elif not manifest.is_file() or json.loads(manifest.read_text()) != lock:
        raise ValueError("Missing modules without a matching pruning receipt")
    manifest.write_text(json.dumps(lock, indent=2) + "\n")
    manifest.chmod(0o644)
    print(f"SPARK_STREAMING_FEATURE_PROFILE_APPLIED removed_jars={len(names)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lock", type=Path)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    apply(json.loads(args.lock.read_text()), manifest=args.manifest)
