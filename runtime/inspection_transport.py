"""Read-only AWS CLI bridge for the existing pilot metadata; no SDK dependency.

Construction is inert. Production creates this client only after the inspection
approval and host checks. There is no generic AWS action/argument entrypoint.
"""

import hashlib
import io
import os
from pathlib import Path
import resource
import stat
import subprocess
import tempfile

from runtime.contracts import BUCKET, PILOT_PREFIX
from runtime.recovery import RecoveryBlocked, _require
from runtime.recovery_metadata import MAX_KEYS, MAX_OBJECT, parse_json

MAX_REPLY = 2 * 1024 * 1024
REGION = "us-west-2"
ACCOUNT = "209479274979"
INSTANCE = "i-0ca1671a59751acc5"
ROLE_ARN = f"arn:aws:sts::{ACCOUNT}:assumed-role/blockchain-runtime-ec2/{INSTANCE}"
DIRECTORIES = {
    f"{PILOT_PREFIX}/checkpoints/swaps_bronze/commits/",
    f"{PILOT_PREFIX}/checkpoints/swaps_bronze/offsets/",
    f"{PILOT_PREFIX}/bronze/swaps/_spark_metadata/",
}


def metadata_key(key):
    from runtime.recovery_metadata import READ_KEY

    if type(key) is not str:
        return False
    for full, relative in (
        (f"{PILOT_PREFIX}/checkpoints/swaps_bronze/", "checkpoint/"),
        (f"{PILOT_PREFIX}/bronze/swaps/", "bronze/"),
    ):
        if key.startswith(full):
            return READ_KEY.fullmatch(relative + key[len(full) :]) is not None
    return False


def child_limits():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_REPLY, MAX_REPLY))


def private_directory(path):
    _require(not path.is_symlink() and path.is_dir(), "INSPECTION_SCRATCH_UNSAFE")
    info = path.stat()
    _require(
        info.st_uid == os.geteuid() and not info.st_mode & 0o077,
        "INSPECTION_SCRATCH_UNSAFE",
    )


def safe_binary(path):
    path = Path(path)
    _require(path.is_absolute(), "INSPECTION_BINARY_UNSAFE")
    resolved = path.resolve(strict=True)
    for item in [resolved, *resolved.parents]:
        info = item.stat()
        _require(
            info.st_uid == 0 and not info.st_mode & 0o022, "INSPECTION_BINARY_UNSAFE"
        )
    _require(
        resolved.is_file() and os.access(resolved, os.X_OK), "INSPECTION_BINARY_UNSAFE"
    )
    return str(path)


class AwsCliMetadataClient:
    def __init__(self, scratch, binary="/usr/bin/aws", guard=lambda: None):
        self.scratch, self.binary, self.guard = Path(scratch), binary, guard
        self.observations = {}
        self.env = {
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "HOME": "/root",
            "AWS_REGION": REGION,
            "AWS_DEFAULT_REGION": REGION,
            "AWS_CONFIG_FILE": "/dev/null",
            "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
            "AWS_EC2_METADATA_DISABLED": "false",
            "AWS_PAGER": "",
            "AWS_CLI_AUTO_PROMPT": "off",
            "AWS_MAX_ATTEMPTS": "1",
            "LC_ALL": "C.UTF-8",
        }

    def _invoke(self, operation, options, output=None):
        self.guard()
        private_directory(self.scratch)
        # _invoke is private and still checks the exact verb family. Public
        # methods below validate every variable argument and never accept flags.
        _require(
            operation
            in {
                ("sts", "get-caller-identity"),
                ("s3api", "list-objects-v2"),
                ("s3api", "head-object"),
                ("s3api", "get-object"),
            },
            "INSPECTION_OPERATION_FORBIDDEN",
        )
        command = [self.binary, *operation, *options]
        if output is not None:
            _require(
                operation == ("s3api", "get-object") and output.parent == self.scratch,
                "INSPECTION_OUTPUT_UNSAFE",
            )
            command.append(str(output))
        command += [
            "--region",
            REGION,
            "--output",
            "json",
            "--no-cli-pager",
            "--cli-connect-timeout",
            "5",
            "--cli-read-timeout",
            "10",
            "--no-paginate",
        ]
        try:
            with (
                tempfile.TemporaryFile(dir=self.scratch) as stdout,
                tempfile.TemporaryFile(dir=self.scratch) as stderr,
            ):
                result = subprocess.run(
                    command,
                    env=self.env,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    timeout=20,
                    check=False,
                    preexec_fn=child_limits,
                )
                _require(result.returncode == 0, "INSPECTION_AWS_CLI_FAILED")
                stdout.seek(0)
                reply = stdout.read(MAX_REPLY + 1)
                _require(len(reply) <= MAX_REPLY, "INSPECTION_REPLY_TOO_LARGE")
            self.guard()
            return parse_json(reply)
        except RecoveryBlocked:
            raise
        except Exception:
            raise RecoveryBlocked("INSPECTION_AWS_CLI_FAILED") from None

    def identity(self):
        reply = self._invoke(("sts", "get-caller-identity"), [])
        _require(
            reply.get("Account") == ACCOUNT and reply.get("Arn") == ROLE_ARN,
            "INSPECTION_WORKLOAD_ROLE_CHANGED",
        )
        return {"account_matches": True, "workload_role_matches": True}

    def list_objects_v2(self, *, Bucket, Prefix, MaxKeys, Delimiter):
        _require(
            Bucket == BUCKET
            and Prefix in DIRECTORIES
            and MaxKeys == MAX_KEYS
            and Delimiter == "/",
            "INSPECTION_OPERATION_FORBIDDEN",
        )
        return self._invoke(
            ("s3api", "list-objects-v2"),
            [
                "--bucket",
                BUCKET,
                "--prefix",
                Prefix,
                "--max-keys",
                str(MAX_KEYS),
                "--delimiter",
                "/",
            ],
        )

    def head_object(self, *, Bucket, Key):
        _require(
            Bucket == BUCKET and metadata_key(Key), "INSPECTION_OPERATION_FORBIDDEN"
        )
        reply = self._invoke(
            ("s3api", "head-object"), ["--bucket", BUCKET, "--key", Key]
        )
        from datetime import datetime, timezone

        try:
            stamp = datetime.fromisoformat(reply["LastModified"].replace("Z", "+00:00"))
            _require(stamp.tzinfo is not None, "INSPECTION_METADATA_INVALID")
            version = reply.get("VersionId")
            _require(
                version is None or (type(version) is str and 0 < len(version) <= 1024),
                "INSPECTION_METADATA_INVALID",
            )
            selected = {
                "etag": reply["ETag"],
                "content_length": reply["ContentLength"],
                "last_modified_utc": stamp.astimezone(timezone.utc).isoformat(),
                "version_id_sha256": hashlib.sha256(version.encode()).hexdigest()
                if version
                else None,
            }
            _require(
                Key not in self.observations or self.observations[Key] == selected,
                "INSPECTION_METADATA_CHANGED",
            )
            self.observations[Key] = selected
        except RecoveryBlocked:
            raise
        except Exception:
            raise RecoveryBlocked("INSPECTION_METADATA_INVALID") from None
        return reply

    def get_object(self, *, Bucket, Key, IfMatch, Range):
        import re

        _require(
            Bucket == BUCKET
            and metadata_key(Key)
            and type(IfMatch) is str
            and re.fullmatch(r'"[A-Fa-f0-9]{32}(?:-[0-9]{1,8})?"', IfMatch),
            "INSPECTION_OPERATION_FORBIDDEN",
        )
        match = (
            re.fullmatch(r"bytes=0-([0-9]{1,7})", Range) if type(Range) is str else None
        )
        _require(
            match is not None and int(match.group(1)) < MAX_OBJECT,
            "INSPECTION_OPERATION_FORBIDDEN",
        )
        fd, name = tempfile.mkstemp(prefix="metadata-", dir=self.scratch)
        os.close(fd)
        target = Path(name)
        try:
            reply = self._invoke(
                ("s3api", "get-object"),
                [
                    "--bucket",
                    BUCKET,
                    "--key",
                    Key,
                    "--if-match",
                    IfMatch,
                    "--range",
                    Range,
                ],
                output=target,
            )
            _require(
                not target.is_symlink() and target.is_file(), "INSPECTION_OUTPUT_UNSAFE"
            )
            with target.open("rb") as handle:
                value = handle.read(MAX_OBJECT + 1)
            _require(
                len(value) == int(match.group(1)) + 1, "INSPECTION_REPLY_TOO_LARGE"
            )
            return {"ETag": reply.get("ETag"), "Body": io.BytesIO(value)}
        finally:
            target.unlink(missing_ok=True)


def retained_disk_identity(root, expected_cluster, topic=None):
    """Reads only meta.properties and partition.metadata, never Kafka segments."""
    from runtime.contracts import TOPIC
    from runtime.recovery import _topic_identity

    root = Path(root)
    topic = TOPIC if topic is None else topic
    _require(
        topic in {TOPIC, "synthetic-pilot-events"}, "INSPECTION_DISK_METADATA_INVALID"
    )
    names = ("meta.properties", topic + "-0/partition.metadata")
    raw = {}
    for name in names:
        path = root / name
        for item in [path.parent, *path.parent.parents]:
            _require(
                not item.is_symlink() and item.is_dir(), "INSPECTION_DISK_PATH_UNSAFE"
            )
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as handle:
            _require(
                stat.S_ISREG(os.fstat(handle.fileno()).st_mode),
                "INSPECTION_DISK_PATH_UNSAFE",
            )
            value = handle.read(4097)
        _require(0 < len(value) <= 4096, "INSPECTION_DISK_METADATA_INVALID")
        raw[name] = value

    def properties(value, separator):
        result = {}
        for line in value.decode("utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "!")):
                continue
            key, sep, data = line.partition(separator)
            key, data = key.strip(), data.strip()
            _require(
                sep and key not in result and "\\" not in line,
                "INSPECTION_DISK_METADATA_INVALID",
            )
            result[key] = data
        return result

    meta = properties(raw[names[0]], "=")
    part = properties(raw[names[1]], ":")
    _require(
        set(meta)
        in (
            {"version", "cluster.id", "node.id"},
            {"version", "cluster.id", "node.id", "directory.id"},
        )
        and meta["version"] == "1"
        and meta["node.id"] == "1"
        and meta["cluster.id"] == expected_cluster,
        "INSPECTION_CLUSTER_IDENTITY_CHANGED",
    )
    _require(
        set(part) == {"version", "topic_id"} and part["version"] == "0",
        "INSPECTION_DISK_METADATA_INVALID",
    )
    if "directory.id" in meta:
        _topic_identity(meta["directory.id"])
    return {
        "cluster_id": meta["cluster.id"],
        "topic_id": _topic_identity(part["topic_id"]),
        "partition": 0,
        "node_id": 1,
        "metadata_sha256": {
            name: hashlib.sha256(value).hexdigest() for name, value in raw.items()
        },
    }
