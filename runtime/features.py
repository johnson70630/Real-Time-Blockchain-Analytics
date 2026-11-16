"""Cloud-only classic streaming profile; optional SQL servers are excluded."""

import json
import re
from pathlib import Path

PROFILE = "local_streaming_pilot"
EXCLUDED = (
    "spark_connect",
    "hive_catalog",
    "hive_thriftserver",
    "embedded_derby",
    "legacy_codehaus_json",
)
MANIFEST = Path("/opt/pilot/streaming-profile.json")
JARS = Path("/usr/local/lib/python3.12/site-packages/pyspark/jars")
FORBIDDEN_CLASSES = (
    "org.apache.spark.sql.connect.service.SparkConnectService",
    "org.apache.spark.sql.hive.HiveSessionStateBuilder",
    "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
    "org.apache.hadoop.hive.metastore.HiveMetaStoreClient",
    "org.apache.derby.iapi.jdbc.AutoloadedDriver",
    "org.codehaus.jackson.map.ObjectMapper",
)


def feature(path):
    if path.startswith("connect-repl/") or path.startswith("spark-connect_"):
        return "spark_connect"
    if path.startswith(
        (
            "spark-hive-thriftserver_",
            "hive-beeline-",
            "hive-cli-",
            "hive-jdbc-",
            "hive-service-rpc-",
        )
    ):
        return "hive_thriftserver"
    if path.startswith("spark-hive_") or (
        path.startswith("hive-") and not path.startswith("hive-storage-api-")
    ):
        return "hive_catalog"
    if path.startswith("derby"):
        return "embedded_derby"
    if path.startswith(("jackson-core-asl-", "jackson-mapper-asl-")):
        return "legacy_codehaus_json"
    return None


def validate(lock):
    if (
        lock.get("schema_version") != 1
        or lock.get("pyspark_version") != "4.0.1"
        or lock.get("profile") != PROFILE
        or lock.get("excluded_features") != list(EXCLUDED)
        or not isinstance(lock.get("removals"), list)
        or not 1 <= len(lock["removals"]) <= 200
    ):
        raise ValueError("Invalid streaming feature profile")
    names, groups = set(), set()
    for row in lock["removals"]:
        name = row.get("path", "")
        if (
            not re.fullmatch(r"(?:connect-repl/)?[A-Za-z0-9_.-]+\.jar", name)
            or ".." in name
            or name in names
            or row.get("feature") != feature(name)
            or row.get("feature") not in EXCLUDED
            or not re.fullmatch(r"[0-9a-f]{128}", row.get("sha512", ""))
            or type(row.get("size_bytes")) is not int
            or not 0 < row["size_bytes"] <= 100_000_000
        ):
            raise ValueError("Invalid streaming feature removal")
        names.add(name)
        groups.add(row["feature"])
    if groups != set(EXCLUDED):
        raise ValueError("Incomplete streaming feature profile")
    return names


def inventory(target):
    """Inspect excluded modules without following directory or file symlinks."""
    if target.is_symlink() or not target.is_dir():
        raise ValueError("Regular Spark JAR directory required")
    selected = set()
    for entry in target.iterdir():
        if entry.name == "connect-repl":
            if entry.is_symlink() or not entry.is_dir():
                raise ValueError("Regular Connect directory required")
            for child in entry.iterdir():
                if child.is_symlink() or not child.is_file() or child.suffix != ".jar":
                    raise ValueError("Unexpected Connect directory content")
                selected.add("connect-repl/" + child.name)
        elif feature(entry.name):
            if entry.is_symlink() or not entry.is_file() or entry.suffix != ".jar":
                raise ValueError("Regular excluded JAR required")
            selected.add(entry.name)
    return selected


def verify_profile(loader):
    """Reject restored modules and resolvable excluded classes before S3 work."""
    try:
        if MANIFEST.is_symlink() or not MANIFEST.is_file():
            raise ValueError("Regular streaming profile required")
        lock = json.loads(MANIFEST.read_text())
        validate(lock)
        if inventory(JARS):
            raise ValueError("Excluded modules restored")
        for name in FORBIDDEN_CLASSES:
            if loader.getResources(name.replace(".", "/") + ".class").hasMoreElements():
                raise ValueError("Excluded class still resolvable")
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise RuntimeError("PILOT_STREAMING_FEATURE_PROFILE_REQUIRED") from exc
    return {
        "profile": PROFILE,
        "excluded_features": list(EXCLUDED),
        "removed_jars": len(lock["removals"]),
        "excluded_classes_resolvable": False,
    }
