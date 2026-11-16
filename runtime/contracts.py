"""Credential-free release and environment contracts for the bounded pilot."""

from __future__ import annotations

import re
from contextlib import contextmanager
from pathlib import Path

REGION = "us-west-2"
BUCKET = "johnson-blockchain-analytics-prod"
PILOT_PREFIX = "blockchain-analytics/pilot/runtime/11-6c-v1"
TOPIC = "pilot_uniswap_v3_events"
MOUNT = Path("/var/lib/blockchain-runtime/kafka")
RUN = Path("/run/blockchain-runtime")
REGISTRY = "209479274979.dkr.ecr.us-west-2.amazonaws.com"
# The 2026-11-16 reviews rejected these exact manifests. Keep the rejection
# even after replacing local build tags; a different digest still needs review.
REJECTED_IMAGE_DIGESTS = frozenset({
    "sha256:daa2cab53f201ac58435a557a2e1d2d006a4a442dc0a0c7c22fb9a81167f88e0",
    "sha256:85c0b8ebf842847215698fe24efe05db04d0b6525d587955eb58209897843c38",
    "sha256:5862db4a63a6dd7d46fd14771b10a1b39e069c2c47f17d8e4640f960720a0ead",
    # Unpublished 83d4363 Kafka candidate: OS patches left the old JRE CA store.
    "sha256:5214d6b84fe627a56fac141020c71bf877bb62a208a3b1fd6c44358ad85551ce",
    # Unpublished 8805edd app candidates: Python/JVM library review failed.
    "sha256:4f3eba5a4885ad26e8ac2699ef103d0eba4e447922be4089f657593d72b46772",
    "sha256:7d943ca6e3598bcab11f3729610c846f4d4d0736e3702b6b19e1faa8939288a9",
    # 43d3b4b Spark retained the same unresolved library findings.
    "sha256:9a86cb4ac24c570a5683ed9f1a2ab94e46b2877dfef4470a6c53f31143653e43",
    # Unpublished family-patch prototype: an older AWS-bundled Netty shadowed it.
    "sha256:31394fda18b6324806d91302ed273e9b7dc579ac10e3a8964b1afe43c85879e6",
    # f91dc41 Spark still bundled the vulnerable Hadoop 3.4.1 / Avro copy.
    "sha256:95ea3fce6ff6acad3b6a4765e7a7563eba3ba35183491fe3633d661fb4081872",
    # 1b3fb7d Spark retained Connect, Derby and legacy Jackson vulnerabilities.
    "sha256:652b347f1455dbd397336fc2334a3e5d90c95ea10229dc49a4459a68675cd5de",
    # 3dfef13 Spark retained the unresolved relocated SDK Netty finding.
    "sha256:4da1dc74633cfab7d585cd986c91b73250a469217e3534fa327470a4ae31bb6f",
    # f209652 Spark retained the old standalone JSON/compression dependencies.
    "sha256:5b652614cbcfec3514c6f437ab32878a9ebdd55bbc70b7f07734d59b116f5339",
    # 9285013 Spark retained the earlier Guava/JLine/ZooKeeper client libraries.
    "sha256:004307f9a5a3986cd6c2e70fb3147fbce23e2953dff1eadf95ccb87cf6a547ba",
})


def validate_release(release: dict) -> None:
    """Reject placeholders, mutable images, changed scope, and unbounded runs."""
    expected = {
        "instance_id",
        "kafka_volume_id",
        "kafka_filesystem_uuid",
        "cluster_id",
        "producer_image",
        "spark_image",
        "kafka_image",
        "secret_arn",
        "pilot_prefix",
        "max_runtime_seconds",
    }
    if set(release) != expected:
        raise ValueError("RELEASE_FIELDS_INVALID")
    for field, pattern in {
        "instance_id": r"i-[0-9a-f]{17}",
        "kafka_volume_id": r"vol-[0-9a-f]{17}",
        "kafka_filesystem_uuid": r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}",
        "cluster_id": r"[A-Za-z0-9_-]{22}",
        "secret_arn": r"arn:aws:secretsmanager:us-west-2:209479274979:secret:[A-Za-z0-9/_+=.@-]+-[A-Za-z0-9]{6}",
    }.items():
        if not isinstance(release[field], str) or not re.fullmatch(
            pattern, release[field]
        ):
            raise ValueError(f"RELEASE_{field.upper()}_INVALID")
    for field, repository in {
        "producer_image": "producer",
        "kafka_image": "producer",
        "spark_image": "spark",
    }.items():
        pattern = re.escape(f"{REGISTRY}/blockchain-runtime-{repository}@sha256:")
        if not isinstance(release[field], str) or not re.fullmatch(
            pattern + "[0-9a-f]{64}", release[field]
        ):
            raise ValueError("ECR_DIGEST_REQUIRED")
        if release[field].split("@", 1)[1] in REJECTED_IMAGE_DIGESTS:
            raise ValueError("REJECTED_IMAGE_RELEASE")
    duration = release["max_runtime_seconds"]
    if type(duration) is not int or not 300 <= duration <= 7200:
        raise ValueError("PILOT_DURATION_MUST_BE_300_TO_7200_SECONDS")
    if release["pilot_prefix"] != PILOT_PREFIX:
        raise ValueError("PILOT_PREFIX_INVALID")


def validate_environment(env) -> None:
    """Keep all cloud application writes in the approved pilot namespace."""
    for key, value in {
        "STORAGE_MODE": "s3",
        "DATA_LAKE_BUCKET": BUCKET,
        "DATA_LAKE_PREFIX": PILOT_PREFIX,
        "AWS_REGION": REGION,
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_TOPIC": TOPIC,
        "CHAIN": "arbitrum",
        "ENABLED_PROTOCOLS": "uniswap_v3",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "LANGUAGE": "",
    }.items():
        if env.get(key) != value:
            raise ValueError(f"PILOT_{key}_INVALID")
    if any(
        env.get(key)
        for key in (
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_PROFILE",
            "ALCHEMY_WEBSOCKET_URL",
        )
    ):
        raise ValueError("STATIC_CREDENTIALS_FORBIDDEN")


@contextmanager
def writer_lock(path: Path):
    """One writer on the sole approved host; never unlink the locked inode."""
    import fcntl

    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("CHECKPOINT_ALREADY_OWNED") from None
        yield
