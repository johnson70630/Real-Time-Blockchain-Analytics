"""Bounded read-only metadata collection. Import creates no clients or connections.

S3 clients and broker observations must be supplied by a separately authorized
caller. Production observations are never promoted to a trusted baseline here.
"""

import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time
from urllib.parse import unquote, urlsplit

from runtime.contracts import BUCKET, PILOT_PREFIX, TOPIC
from runtime.recovery import RecoveryBlocked, assess_recovery, _require

MAX_OBJECT = 1024 * 1024
MAX_TOTAL = 8 * MAX_OBJECT
MAX_KEYS = 1000
NUMBER = r"(?:0|[1-9][0-9]{0,18})"
LOG_NAME = re.compile(NUMBER + r"(?:\.compact)?")
READ_KEY = re.compile(
    r"(?:checkpoint/metadata|checkpoint/(?:commits|offsets)/"
    + NUMBER
    + r"|bronze/_spark_metadata/"
    + NUMBER
    + r"(?:\.compact)?)"
)
LIST_KEYS = {"checkpoint/commits", "checkpoint/offsets", "bronze/_spark_metadata"}
SAFE_CODES = frozenset(
    """
BROKER_METADATA_INVALID CHECKPOINT_COMMIT_INVALID CHECKPOINT_METADATA_INVALID
CHECKPOINT_OFFSETS_INVALID CHECKPOINT_OUTSIDE_RETAINED_KAFKA CHECKPOINT_QUERY_ID_INVALID
DURABLE_RECOVERY_METADATA_REQUIRED EXISTING_WRITER_RECOVERY_BLOCKED KAFKA_IDENTITY_REQUIRED
METADATA_BATCH_AMBIGUOUS METADATA_CLOCK_INVALID METADATA_COLLECTION_FAILED
METADATA_DIRECTORY_UNSAFE METADATA_DUPLICATE_JSON_KEY METADATA_FILE_UNSAFE
METADATA_JSON_INVALID METADATA_KEY_FORBIDDEN METADATA_LIST_CHANGED METADATA_LIST_INVALID
METADATA_LIST_TRUNCATED METADATA_LOG_NAME_INVALID METADATA_LOG_VERSION_INVALID
METADATA_OBJECT_CHANGED METADATA_OBJECT_TOO_LARGE_OR_EMPTY METADATA_OBSERVATION_STALE
METADATA_TOTAL_TOO_LARGE NON_APPEND_SINK_METADATA RECOVERY_IDENTITY_OR_OFFSETS_CHANGED
RECOVERY_PARTIAL_BATCH_REQUIRES_REVIEW SINK_METADATA_PATH_CHANGED RECOVERY_SCHEMA_INVALID
RECOVERY_SCOPE_INVALID RECOVERY_BATCH_INVALID RECOVERY_OFFSETS_INVALID RECOVERY_PATHS_INVALID
SINK_CHECKPOINT_BATCH_MISMATCH
""".split()
)


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RecoveryBlocked("METADATA_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def parse_json(raw):
    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError()
        return number

    try:
        return json.loads(
            raw,
            object_pairs_hook=_object,
            parse_float=finite_float,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
    except RecoveryBlocked:
        raise
    except (ValueError, TypeError, RecursionError):
        raise RecoveryBlocked("METADATA_JSON_INVALID") from None


def _log(raw, allow_empty=False):
    lines = raw.splitlines()
    _require(
        len(lines) >= (1 if allow_empty else 2) and lines[0] == b"v1",
        "METADATA_LOG_VERSION_INVALID",
    )
    return [parse_json(line) for line in lines[1:]]


def _safe_directory(path):
    for part in [path, *path.parents]:
        _require(not part.is_symlink() and part.is_dir(), "METADATA_DIRECTORY_UNSAFE")


class LocalMetadataSource:
    """Only fixed metadata files beneath a synthetic test volume; never writes."""

    def __init__(self, root):
        self.root = Path(root).absolute()
        _safe_directory(self.root)
        self.output = str(self.root / "bronze")
        self.checkpoint = str(self.root / "checkpoint")

    def _path(self, key):
        _require(
            READ_KEY.fullmatch(key) is not None or key in LIST_KEYS,
            "METADATA_KEY_FORBIDDEN",
        )
        path = self.root / key
        _safe_directory(path.parent)
        _require(not path.is_symlink(), "METADATA_FILE_UNSAFE")
        return path

    def list(self, key):
        _require(key in LIST_KEYS, "METADATA_KEY_FORBIDDEN")
        path = self._path(key)
        _safe_directory(path)
        names = []
        with os.scandir(path) as entries:
            for entry in entries:
                _require(len(names) < MAX_KEYS, "METADATA_LIST_TRUNCATED")
                _require(entry.is_file(follow_symlinks=False), "METADATA_FILE_UNSAFE")
                names.append(entry.name)
        return sorted(names)

    def read(self, key):
        _require(READ_KEY.fullmatch(key) is not None, "METADATA_KEY_FORBIDDEN")
        fd = os.open(self._path(key), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as handle:
            _require(
                stat.S_ISREG(os.fstat(handle.fileno()).st_mode), "METADATA_FILE_UNSAFE"
            )
            value = handle.read(MAX_OBJECT + 1)
        _require(0 < len(value) <= MAX_OBJECT, "METADATA_OBJECT_TOO_LARGE_OR_EMPTY")
        return value


class S3MetadataSource:
    """Injected client, fixed existing pilot prefixes; only List/Head/Get methods."""

    def __init__(self, client):
        self.client = client
        self.output = f"s3a://{BUCKET}/{PILOT_PREFIX}/bronze/swaps"
        self.checkpoint = f"s3a://{BUCKET}/{PILOT_PREFIX}/checkpoints/swaps_bronze"

    def _key(self, key):
        _require(
            READ_KEY.fullmatch(key) is not None or key in LIST_KEYS,
            "METADATA_KEY_FORBIDDEN",
        )
        root, rest = key.split("/", 1)
        prefix = "bronze/swaps" if root == "bronze" else "checkpoints/swaps_bronze"
        return f"{PILOT_PREFIX}/{prefix}/{rest}"

    def list(self, key):
        _require(key in LIST_KEYS, "METADATA_KEY_FORBIDDEN")
        prefix = self._key(key) + "/"
        response = self.client.list_objects_v2(
            Bucket=BUCKET, Prefix=prefix, MaxKeys=MAX_KEYS, Delimiter="/"
        )
        _require(
            response.get("IsTruncated") is False and not response.get("CommonPrefixes"),
            "METADATA_LIST_TRUNCATED",
        )
        entries = response.get("Contents", [])
        _require(
            type(entries) is list and len(entries) <= MAX_KEYS,
            "METADATA_LIST_TRUNCATED",
        )
        names = []
        for entry in entries:
            full = entry["Key"]
            _require(
                type(full) is str and full.startswith(prefix), "METADATA_KEY_FORBIDDEN"
            )
            name = full[len(prefix) :]
            _require(name and "/" not in name, "METADATA_KEY_FORBIDDEN")
            names.append(name)
        return sorted(names)

    def read(self, key):
        _require(READ_KEY.fullmatch(key) is not None, "METADATA_KEY_FORBIDDEN")
        full = self._key(key)
        head = self.client.head_object(Bucket=BUCKET, Key=full)
        size, etag = head["ContentLength"], head["ETag"]
        _require(
            type(size) is int and 0 < size <= MAX_OBJECT and type(etag) is str and etag,
            "METADATA_OBJECT_TOO_LARGE_OR_EMPTY",
        )
        response = self.client.get_object(
            Bucket=BUCKET, Key=full, IfMatch=etag, Range=f"bytes=0-{size - 1}"
        )
        body = response["Body"]
        try:
            value = body.read(MAX_OBJECT + 1)
        finally:
            body.close()
        _require(
            type(value) is bytes
            and len(value) == size
            and response.get("ETag") == etag,
            "METADATA_OBJECT_CHANGED",
        )
        return value


class KafkaMetadataProbe:
    """Explicit invocation only; fixed internal broker and no topic auto-creation."""

    def __init__(self, topic):
        _require(topic in {TOPIC, "synthetic-pilot-events"}, "BROKER_METADATA_INVALID")
        self.topic = topic

    def __call__(self):
        from confluent_kafka import Consumer, TopicCollection, TopicPartition
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": "kafka:9092"})
        consumer = Consumer(
            {
                "bootstrap.servers": "kafka:9092",
                "group.id": "pilot-recovery-metadata",
                "allow.auto.create.topics": False,
                "enable.auto.commit": False,
            }
        )
        try:
            cluster = admin.describe_cluster(request_timeout=8).result(10)
            topic = admin.describe_topics(
                TopicCollection([self.topic]), request_timeout=8
            )[self.topic].result(10)
            _require(
                topic.name == self.topic
                and topic.is_internal is False
                and [p.id for p in topic.partitions] == [0],
                "BROKER_METADATA_INVALID",
            )
            low, high = consumer.get_watermark_offsets(
                TopicPartition(self.topic, 0), timeout=8, cached=False
            )
            return {
                "cluster_id": cluster.cluster_id,
                "topic_id": str(topic.topic_id),
                "topic": self.topic,
                "begin_offsets": {"0": low},
                "end_offsets": {"0": high},
            }
        finally:
            consumer.close()


def _numbers(names, compact=False):
    _require(
        type(names) is list
        and len(names) <= MAX_KEYS
        and len(set(names)) == len(names),
        "METADATA_LIST_INVALID",
    )
    result = {}
    for name in names:
        _require(type(name) is str, "METADATA_LOG_NAME_INVALID")
        if re.fullmatch(r"\." + NUMBER + r"(?:\.compact)?\.crc", name):
            continue
        _require(
            LOG_NAME.fullmatch(name) is not None
            and (compact or not name.endswith(".compact")),
            "METADATA_LOG_NAME_INVALID",
        )
        number = int(name.split(".", 1)[0])
        _require(number not in result, "METADATA_BATCH_AMBIGUOUS")
        result[number] = name
    _require(result, "DURABLE_RECOVERY_METADATA_REQUIRED")
    return result


class MetadataCollector:
    def __init__(
        self, source, broker, storage_identity, writer_active, clock=time.monotonic
    ):
        self.source, self.broker = source, broker
        self.storage_identity, self.writer_active, self.clock = (
            storage_identity,
            writer_active,
            clock,
        )

    def collect(self):
        try:
            return self._collect()
        except RecoveryBlocked as error:
            raise RecoveryBlocked(
                str(error) if str(error) in SAFE_CODES else "METADATA_COLLECTION_FAILED"
            ) from None
        except Exception:
            raise RecoveryBlocked("METADATA_COLLECTION_FAILED") from None

    def _collect(self):
        started = self.clock()
        _require(
            type(started) in (int, float) and math.isfinite(started),
            "METADATA_CLOCK_INVALID",
        )
        _require(self.writer_active() is False, "EXISTING_WRITER_RECOVERY_BLOCKED")
        storage = self.storage_identity()
        broker = self.broker()
        _require(
            type(broker) is dict
            and set(broker)
            == {"cluster_id", "topic_id", "topic", "begin_offsets", "end_offsets"},
            "BROKER_METADATA_INVALID",
        )
        # Copy snapshots: mutable fake/client mappings cannot change earlier evidence.
        broker = parse_json(canonical(broker))
        _require(
            not isinstance(self.source, S3MetadataSource) or broker["topic"] == TOPIC,
            "BROKER_METADATA_INVALID",
        )
        cache, total = {}, 0

        def read(key):
            nonlocal total
            value = self.source.read(key)
            _require(
                type(value) is bytes and 0 < len(value) <= MAX_OBJECT,
                "METADATA_OBJECT_TOO_LARGE_OR_EMPTY",
            )
            total += len(value)
            _require(total <= MAX_TOTAL, "METADATA_TOTAL_TOO_LARGE")
            cache[key] = value
            return value

        listings = {name: self.source.list(name) for name in sorted(LIST_KEYS)}
        commits = _numbers(listings["checkpoint/commits"])
        offsets = _numbers(listings["checkpoint/offsets"])
        sinks = _numbers(listings["bronze/_spark_metadata"], compact=True)
        batch = max(commits)
        _require(
            max(offsets) == batch and max(sinks) == batch,
            "RECOVERY_PARTIAL_BATCH_REQUIRES_REVIEW",
        )
        query = parse_json(read("checkpoint/metadata"))
        _require(
            type(query) is dict and set(query) == {"id"}, "CHECKPOINT_METADATA_INVALID"
        )
        commit = _log(read("checkpoint/commits/" + commits[batch]))
        _require(
            len(commit) == 1 and type(commit[0]) is dict, "CHECKPOINT_COMMIT_INVALID"
        )
        offset = _log(read("checkpoint/offsets/" + offsets[batch]))
        _require(
            len(offset) == 2
            and type(offset[0]) is dict
            and type(offset[1]) is dict
            and set(offset[1]) == {broker["topic"]},
            "CHECKPOINT_OFFSETS_INVALID",
        )
        sink = _log(read("bronze/_spark_metadata/" + sinks[batch]), allow_empty=True)
        for item in sink:
            _require(
                type(item) is dict
                and item.get("action") == "add"
                and type(item.get("path")) is str,
                "NON_APPEND_SINK_METADATA",
            )
            output = self.source.output
            expected = Path(output).as_uri() if output.startswith("/") else output
            uri = urlsplit(unquote(item["path"]))
            prefix = urlsplit(expected)
            _require(
                uri.scheme == prefix.scheme
                and uri.netloc == prefix.netloc
                and not uri.query
                and not uri.fragment
                and uri.path.startswith(prefix.path + "/")
                and ".." not in uri.path.split("/"),
                "SINK_METADATA_PATH_CHANGED",
            )
        snapshot = {
            "schema_version": 1,
            "scope": {
                "storage_identity": storage,
                "cluster_id": broker["cluster_id"],
                "topic": broker["topic"],
                "topic_id": broker["topic_id"],
                "output": self.source.output,
                "checkpoint": self.source.checkpoint,
                "query_id": query["id"],
            },
            "checkpoint_batch": batch,
            "sink_batch": batch,
            "checkpoint_end_offsets": offset[1][broker["topic"]],
            "kafka_begin_offsets": broker["begin_offsets"],
            "kafka_end_offsets": broker["end_offsets"],
            "checkpoint_metadata_exists": True,
            "checkpoint_commit_exists": True,
            "sink_metadata_exists": True,
            "writer_active": False,
        }
        assess_recovery(snapshot, snapshot)
        for key, value in cache.items():
            current = self.source.read(key)
            total += len(current)
            _require(total <= MAX_TOTAL and current == value, "METADATA_OBJECT_CHANGED")
        _require(
            all(self.source.list(name) == names for name, names in listings.items()),
            "METADATA_LIST_CHANGED",
        )
        _require(
            canonical(self.broker()) == canonical(broker)
            and self.storage_identity() == storage,
            "RECOVERY_IDENTITY_OR_OFFSETS_CHANGED",
        )
        _require(self.writer_active() is False, "EXISTING_WRITER_RECOVERY_BLOCKED")
        finished = self.clock()
        _require(
            type(finished) in (int, float)
            and math.isfinite(finished)
            and 0 <= finished - started <= 30,
            "METADATA_OBSERVATION_STALE",
        )
        return {
            "schema_version": 1,
            "snapshot": snapshot,
            "snapshot_sha256": digest(snapshot),
            "observed_at_monotonic": finished,
            "metadata_sha256": {
                k: hashlib.sha256(v).hexdigest() for k, v in cache.items()
            },
            "stable_read_verified": True,
            "baseline_approved": False,
            "cloud_start_authorized": False,
        }
