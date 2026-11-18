"""Real synthetic Parquet plus fake S3; no cloud or provider access."""

from datetime import datetime, timezone
import io
import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from runtime.contracts import BUCKET, PILOT_PREFIX, TOPIC
from runtime.pilot_acceptance import validate_sample
from runtime.pilot_sample import sample


NOW = datetime(2026, 11, 18, 23, 0, tzinfo=timezone.utc)


def parquet_bytes(change=None):
    values = {
        "kafka_offset": [0, 1],
        "kafka_partition": [0, 0],
        "transaction_hash": ["0x" + "a" * 64, "0x" + "b" * 64],
        "log_index": [0, 1],
        "block_timestamp": [NOW, NOW],
        "json_value": [json.dumps({"chain": "arbitrum", "protocol": "uniswap_v3"})] * 2,
    }
    if change:
        values.update(change)
    stream = io.BytesIO()
    pq.write_table(pa.table(values), stream)
    return stream.getvalue()


@pytest.fixture
def s3(tmp_path):
    part = (
        PILOT_PREFIX + "/bronze/swaps/protocol=uniswap_v3/chain=arbitrum/part-0.parquet"
    )
    meta = PILOT_PREFIX + "/bronze/swaps/_spark_metadata/0"
    commit = PILOT_PREFIX + "/checkpoints/swaps_bronze/commits/0"
    offset = PILOT_PREFIX + "/checkpoints/swaps_bronze/offsets/0"
    data = {
        part: parquet_bytes(),
        meta: (
            "v1\n"
            + json.dumps({"action": "add", "path": "s3a://" + BUCKET + "/" + part})
            + "\n"
        ).encode(),
        commit: b"v1\n{}\n",
        offset: ("v1\n{}\n" + json.dumps({TOPIC: {"0": 2}})).encode(),
    }
    operator = SimpleNamespace(
        release={"pilot_prefix": PILOT_PREFIX}, state=tmp_path, guard=lambda: None
    )

    def call(args, **kw):
        assert args[0:2] == ["aws", "s3api"]
        assert args[args.index("--bucket") + 1] == BUCKET
        if args[2] == "list-objects-v2":
            prefix = args[args.index("--prefix") + 1]
            return json.dumps(
                {
                    "Contents": [{"Key": x} for x in data if x.startswith(prefix)],
                    "IsTruncated": False,
                }
            )
        key = args[args.index("--key") + 1]
        if args[2] == "head-object":
            return json.dumps({"ContentLength": len(data[key])})
        target = args[args.index("--key") + 2]
        __import__("pathlib").Path(target).write_bytes(data[key])
        return "{}"

    operator.call = call
    return operator, data, part, meta, commit, offset


def test_guest_sample_and_local_parquet_offset_acceptance(s3):
    operator, *_ = s3
    result = sample(operator)
    checked = validate_sample(result, NOW)
    assert checked["rows"] == 2 and checked["duplicate_kafka_offsets"] == 0
    assert checked["ok"] is True
    assert checked["committed_offset_range_complete"] is True
    assert "_artifact_payloads" not in checked
    with pytest.raises(FileExistsError):
        sample(operator)


def test_compacted_sink_metadata_checks_combined_offset_range(s3):
    operator, data, part, meta, commit, offset = s3
    data[meta[:-1] + "9.compact"] = data.pop(meta)
    data[commit[:-1] + "9"] = data.pop(commit)
    data[offset[:-1] + "9"] = data.pop(offset)
    result = sample(operator)
    assert result["compact_metadata"] and result["committed_batch"] == 9
    assert result["start_offsets"] == {"0": 0}
    assert validate_sample(result, NOW)["rows"] == 2


@pytest.mark.parametrize(
    "mutation,code",
    [
        ("bucket", "SAMPLE_URI_CHANGED"),
        ("partition", "SAMPLE_PARTITION_CHANGED"),
        ("delete", "NON_APPEND_SINK_METADATA"),
    ],
)
def test_invalid_metadata_is_rejected_before_parquet_export(s3, mutation, code):
    operator, data, part, meta, *_ = s3
    row = {"action": "add", "path": "s3a://" + BUCKET + "/" + part}
    if mutation == "bucket":
        row["path"] = row["path"].replace(BUCKET, "other-bucket")
    elif mutation == "partition":
        row["path"] = row["path"].replace("chain=arbitrum", "chain=ethereum")
    else:
        row["action"] = "delete"
    data[meta] = ("v1\n" + json.dumps(row)).encode()
    with pytest.raises(RuntimeError, match=code):
        sample(operator)
    assert not list((operator.state / "qa").glob("*.parquet"))


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"kafka_offset": [0, 0]}, "OFFSET_GAP_OR_DUPLICATE"),
        ({"kafka_offset": [0, 2]}, "OFFSET_GAP_OR_DUPLICATE"),
        ({"kafka_partition": [0, 1]}, "KAFKA_METADATA_CHANGED"),
        (
            {"json_value": ['{"chain":"ethereum","protocol":"uniswap_v3"}'] * 2},
            "CHAIN_OR_PROTOCOL_CHANGED",
        ),
        ({"transaction_hash": ["wrong", "wrong"]}, "TRANSACTION_LOG_ID_CHANGED"),
        (
            {"block_timestamp": [datetime(2020, 1, 1), datetime(2020, 1, 1)]},
            "BLOCK_TIMESTAMP_OUTSIDE_WINDOW",
        ),
    ],
)
def test_corrupt_public_data_never_passes_acceptance(s3, changes, code):
    operator, data, part, *_ = s3
    data[part] = parquet_bytes(changes)
    with pytest.raises(RuntimeError, match=code):
        validate_sample(sample(operator), NOW)


def test_payload_tampering_and_extra_fields_are_not_forwarded(s3):
    operator, *_ = s3
    result = sample(operator)
    result["untrusted_extra"] = "must not reach output"
    assert "untrusted_extra" not in validate_sample(result, NOW)
    result["_artifact_payloads"][0]["sha256"] = "0" * 64
    with pytest.raises(RuntimeError, match="SAMPLE_HASH_CHANGED"):
        validate_sample(result, NOW)


def test_oversized_head_fails_before_download(s3):
    operator, *_ = s3
    original = operator.call
    operator.call = lambda args, **kw: (
        json.dumps({"ContentLength": 99 * 1024 * 1024})
        if args[2] == "head-object"
        else original(args, **kw)
    )
    with pytest.raises(RuntimeError, match="SAMPLE_OBJECT_TOO_LARGE"):
        sample(operator)


def test_retained_checkpoint_range_skips_batch_without_predecessor_offset(s3):
    operator, data, part, meta, commit, offset = s3
    data[part] = parquet_bytes({"kafka_offset": [2, 3]})
    metadata = data.pop(meta)
    data[meta[:-1] + "1"] = metadata
    data[meta[:-1] + "2"] = metadata
    data.pop(commit)
    data[commit[:-1] + "1"] = data[commit[:-1] + "2"] = b"v1\n{}"
    data.pop(offset)
    data[offset[:-1] + "1"] = ("v1\n{}\n" + json.dumps({TOPIC: {"0": 2}})).encode()
    data[offset[:-1] + "2"] = ("v1\n{}\n" + json.dumps({TOPIC: {"0": 4}})).encode()
    result = sample(operator)
    assert result["committed_batch"] == 2
    assert result["start_offsets"] == {"0": 2}
    assert validate_sample(result, NOW)["committed_offset_range_complete"]
