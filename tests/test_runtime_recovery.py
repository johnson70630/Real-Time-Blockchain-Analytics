"""Recovery preserves identity/progress and never grants a cloud start."""

from copy import deepcopy
import json

import pytest

from runtime.recovery import RecoveryBlocked, assess_recovery


@pytest.fixture
def baseline():
    return {
        "schema_version": 1,
        "scope": {
            "storage_identity": "synthetic-retained-volume",
            "cluster_id": "MkU3OEVBNTcwNTJENDM2Qk",
            "topic": "synthetic-pilot-events",
            "topic_id": "AAAAAAAAAAEAAAAAAAAAAg",
            "output": "/test-data/bronze",
            "checkpoint": "/test-data/checkpoint",
            "query_id": "12345678-1234-1234-1234-123456789abc",
        },
        "checkpoint_batch": 13,
        "sink_batch": 13,
        "checkpoint_end_offsets": {"0": 5},
        "kafka_begin_offsets": {"0": 0},
        "kafka_end_offsets": {"0": 8},
        "checkpoint_metadata_exists": True,
        "checkpoint_commit_exists": True,
        "sink_metadata_exists": True,
        "writer_active": False,
    }


def test_retained_state_and_safe_retention_advance(baseline):
    observed = deepcopy(baseline)
    observed["kafka_begin_offsets"]["0"] = 5
    original = deepcopy(observed)
    assert assess_recovery(baseline, observed) == {
        "status": "OFFLINE_RECOVERY_CONTRACT_PASS",
        "pending_kafka_records": 3,
        "live_checks_required": True,
        "cloud_start_authorized": False,
    }
    assert observed == original


@pytest.mark.parametrize("native", ["NH7b0fLSTnKFKjVemmxT/A", "/" * 21 + "w"])
def test_native_and_urlsafe_topic_uuid_formats_match_without_mutation(baseline, native):
    observed = deepcopy(baseline)
    observed["scope"]["topic_id"] = native
    baseline["scope"]["topic_id"] = native.replace("/", "_").replace("+", "-")
    assert assess_recovery(baseline, observed)["cloud_start_authorized"] is False
    assert observed["scope"]["topic_id"] == native


@pytest.mark.parametrize(
    "field",
    [
        "storage_identity",
        "cluster_id",
        "topic",
        "topic_id",
        "output",
        "checkpoint",
        "query_id",
    ],
)
def test_changed_identity_rejected_even_when_offsets_match(baseline, field):
    observed = deepcopy(baseline)
    observed["scope"][field] = {
        "cluster_id": "AAAAAAAAAAEAAAAAAAAAAw",
        "topic_id": "AAAAAAAAAAEAAAAAAAAAAw",
        "query_id": "12345678-1234-1234-1234-123456789abd",
    }.get(field, "different")
    with pytest.raises(RecoveryBlocked, match="RECOVERY_IDENTITY_CHANGED"):
        assess_recovery(baseline, observed)


@pytest.mark.parametrize(
    "field",
    ["checkpoint_metadata_exists", "checkpoint_commit_exists", "sink_metadata_exists"],
)
@pytest.mark.parametrize("value", [False, 1, "true"])
def test_missing_or_nonboolean_metadata_blocks_recovery(baseline, field, value):
    baseline[field] = value
    with pytest.raises(RecoveryBlocked, match="DURABLE_RECOVERY_METADATA_REQUIRED"):
        assess_recovery(baseline, baseline)


@pytest.mark.parametrize("value", [True, 0, "false", None])
def test_running_or_unknown_writer_blocks_recovery(baseline, value):
    baseline["writer_active"] = value
    with pytest.raises(RecoveryBlocked, match="EXISTING_WRITER_RECOVERY_BLOCKED"):
        assess_recovery(baseline, baseline)


@pytest.mark.parametrize("low,high,committed", [(6, 8, 5), (0, 4, 5), (9, 8, 5)])
def test_retention_gap_or_checkpoint_beyond_broker_blocks_recovery(
    baseline, low, high, committed
):
    baseline["kafka_begin_offsets"]["0"] = low
    baseline["kafka_end_offsets"]["0"] = high
    baseline["checkpoint_end_offsets"]["0"] = committed
    with pytest.raises(RecoveryBlocked, match="CHECKPOINT_OUTSIDE_RETAINED_KAFKA"):
        assess_recovery(baseline, baseline)


@pytest.mark.parametrize(
    "field,value",
    [
        ("checkpoint_batch", True),
        ("checkpoint_batch", -1),
        ("sink_batch", 14),
        ("schema_version", True),
        ("checkpoint_end_offsets", {"0": True}),
        ("kafka_begin_offsets", {"0": -1}),
        ("kafka_end_offsets", {"0": "8"}),
        ("kafka_end_offsets", {"0": 8, "1": 1}),
        ("scope", []),
        ("scope", {}),
    ],
)
def test_ambiguous_or_changed_contract_blocks_recovery(baseline, field, value):
    baseline[field] = value
    with pytest.raises(RecoveryBlocked):
        assess_recovery(baseline, baseline)


@pytest.mark.parametrize(
    "field", ["checkpoint_batch", "checkpoint_end_offsets", "kafka_end_offsets"]
)
def test_progress_changes_require_review_before_start(baseline, field):
    observed = deepcopy(baseline)
    if field == "checkpoint_batch":
        observed["checkpoint_batch"] = observed["sink_batch"] = 14
    else:
        observed[field]["0"] += 1
    with pytest.raises(RecoveryBlocked, match="RECOVERY_PROGRESS_CHANGED"):
        assess_recovery(baseline, observed)


def test_retention_cannot_move_backwards(baseline):
    observed = deepcopy(baseline)
    baseline["kafka_begin_offsets"]["0"] = 2
    with pytest.raises(RecoveryBlocked, match="KAFKA_RETENTION_MOVED_BACKWARDS"):
        assess_recovery(baseline, observed)


@pytest.mark.parametrize(
    "field,value",
    [
        ("query_id", "not-an-id"),
        ("query_id", "00000000-0000-0000-0000-000000000000"),
        ("cluster_id", ""),
        ("topic_id", "A" * 22),
        ("topic_id", "AAAAAAAAAAEAAAAAAAAAAh"),
        ("topic", "untrusted\nvalue"),
    ],
)
def test_unknown_identity_has_only_fixed_error_output(baseline, field, value, capsys):
    baseline["scope"][field] = value
    with pytest.raises(RecoveryBlocked) as caught:
        assess_recovery(baseline, baseline)
    assert value not in str(caught.value) if value else True
    assert not capsys.readouterr().out


def test_missing_and_unknown_fields_fail_closed(baseline):
    observed = deepcopy(baseline)
    del observed["checkpoint_commit_exists"]
    with pytest.raises(RecoveryBlocked, match="RECOVERY_SCHEMA_INVALID"):
        assess_recovery(baseline, observed)
    observed = deepcopy(baseline)
    observed["untrusted_payload"] = "never-forwarded"
    with pytest.raises(RecoveryBlocked, match="RECOVERY_SCHEMA_INVALID"):
        assess_recovery(baseline, observed)
    assert "never-forwarded" not in json.dumps(assess_recovery(baseline, baseline))
