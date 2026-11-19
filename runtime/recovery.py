"""Pure consistency checks for a proposed restart; never authorizes execution.

Snapshots must come from a trusted baseline and fresh, separately approved live
inspection. Passing fabricated/offline snapshots proves only this contract.
"""

import base64
import re
from uuid import UUID

SCOPE_FIELDS = {
    "storage_identity",
    "cluster_id",
    "topic",
    "topic_id",
    "output",
    "checkpoint",
    "query_id",
}
STATE_FIELDS = {
    "schema_version",
    "scope",
    "checkpoint_batch",
    "sink_batch",
    "checkpoint_end_offsets",
    "kafka_begin_offsets",
    "kafka_end_offsets",
    "checkpoint_metadata_exists",
    "checkpoint_commit_exists",
    "sink_metadata_exists",
    "writer_active",
}


class RecoveryBlocked(ValueError):
    """Fixed reason codes only; never include snapshot values."""


def _require(condition, code):
    if not condition:
        raise RecoveryBlocked(code)


def _topic_identity(value):
    # librdkafka's Uuid text uses standard Base64 (+/); Kafka metadata commonly
    # uses URL-safe Base64 (-_). Compare the UUID bytes through one canonical form.
    _require(
        re.fullmatch(r"[A-Za-z0-9_+/-]{22}", value) is not None,
        "KAFKA_IDENTITY_REQUIRED",
    )
    decoded = base64.b64decode(value + "==", altchars=b"-_", validate=True)
    canonical = base64.urlsafe_b64encode(decoded).decode().rstrip("=")
    _require(
        len(decoded) == 16
        and any(decoded)
        and canonical == value.replace("+", "-").replace("/", "_"),
        "KAFKA_IDENTITY_REQUIRED",
    )
    return canonical


def _validate(state):
    _require(
        type(state) is dict and set(state) == STATE_FIELDS, "RECOVERY_SCHEMA_INVALID"
    )
    _require(
        type(state["schema_version"]) is int and state["schema_version"] == 1,
        "RECOVERY_SCHEMA_INVALID",
    )
    scope = state["scope"]
    _require(
        type(scope) is dict and set(scope) == SCOPE_FIELDS, "RECOVERY_SCOPE_INVALID"
    )
    _require(
        all(
            type(v) is str
            and 0 < len(v) <= 1024
            and all(ord(c) >= 32 and ord(c) != 127 for c in v)
            for v in scope.values()
        ),
        "RECOVERY_SCOPE_INVALID",
    )
    _require(
        re.fullmatch(r"[A-Za-z0-9_-]{22}", scope["cluster_id"]) is not None
        and scope["cluster_id"] != "A" * 22,
        "KAFKA_IDENTITY_REQUIRED",
    )
    scope = dict(scope)
    scope["topic_id"] = _topic_identity(scope["topic_id"])
    try:
        query_id = UUID(scope["query_id"])
    except ValueError:
        raise RecoveryBlocked("CHECKPOINT_QUERY_ID_INVALID") from None
    _require(
        query_id.int != 0 and str(query_id) == scope["query_id"],
        "CHECKPOINT_QUERY_ID_INVALID",
    )
    _require(scope["output"] != scope["checkpoint"], "RECOVERY_PATHS_INVALID")
    for flag in (
        "checkpoint_metadata_exists",
        "checkpoint_commit_exists",
        "sink_metadata_exists",
    ):
        _require(state[flag] is True, "DURABLE_RECOVERY_METADATA_REQUIRED")
    _require(state["writer_active"] is False, "EXISTING_WRITER_RECOVERY_BLOCKED")
    for name in ("checkpoint_batch", "sink_batch"):
        _require(
            type(state[name]) is int and state[name] >= 0, "RECOVERY_BATCH_INVALID"
        )
    _require(
        state["checkpoint_batch"] == state["sink_batch"],
        "SINK_CHECKPOINT_BATCH_MISMATCH",
    )
    for name in ("checkpoint_end_offsets", "kafka_begin_offsets", "kafka_end_offsets"):
        offsets = state[name]
        _require(
            type(offsets) is dict
            and set(offsets) == {"0"}
            and type(offsets["0"]) is int
            and offsets["0"] >= 0,
            "RECOVERY_OFFSETS_INVALID",
        )
    _require(
        state["kafka_begin_offsets"]["0"]
        <= state["checkpoint_end_offsets"]["0"]
        <= state["kafka_end_offsets"]["0"],
        "CHECKPOINT_OUTSIDE_RETAINED_KAFKA",
    )
    return scope


def assess_recovery(baseline, observed):
    """Require unchanged identity/progress before starting either source or writer.

    The source must stay stopped during inspection. Kafka retention may advance
    the low watermark, but cannot discard records after the committed offset.
    A recreated topic with identical numeric offsets must still be rejected.
    """
    baseline_scope = _validate(baseline)
    observed_scope = _validate(observed)
    _require(baseline_scope == observed_scope, "RECOVERY_IDENTITY_CHANGED")
    for field in (
        "checkpoint_batch",
        "sink_batch",
        "checkpoint_end_offsets",
        "kafka_end_offsets",
    ):
        _require(baseline[field] == observed[field], "RECOVERY_PROGRESS_CHANGED")
    _require(
        observed["kafka_begin_offsets"]["0"] >= baseline["kafka_begin_offsets"]["0"],
        "KAFKA_RETENTION_MOVED_BACKWARDS",
    )
    return {
        "status": "OFFLINE_RECOVERY_CONTRACT_PASS",
        "pending_kafka_records": observed["kafka_end_offsets"]["0"]
        - observed["checkpoint_end_offsets"]["0"],
        "live_checks_required": True,
        "cloud_start_authorized": False,
    }
