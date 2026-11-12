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
