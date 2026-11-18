"""Local public Parquet validation; never prints source rows."""

import base64
from datetime import datetime, timezone
import hashlib
import io
import json
import re


def validate_sample(result, now=None):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from runtime.pilot_operator import require

    result = dict(result)
    entries = result.pop("_artifact_payloads", [])
    require(0 < len(entries) <= 32, "PUBLIC_SAMPLE_REQUIRED")
    tables, total, uncompressed, rows = [], 0, 0, 0
    for item in entries:
        require(
            isinstance(item["base64"], str) and len(item["base64"]) <= 23 * 1024 * 1024,
            "SAMPLE_ENCODING_TOO_LARGE",
        )
        data = base64.b64decode(item["base64"], validate=True)
        total += len(data)
        require(
            len(data) <= 16 * 1024 * 1024 and total <= 64 * 1024 * 1024,
            "SAMPLE_TOO_LARGE",
        )
        require(
            hashlib.sha256(data).hexdigest() == item["sha256"], "SAMPLE_HASH_CHANGED"
        )
        parquet = pq.ParquetFile(io.BytesIO(data))
        rows += parquet.metadata.num_rows
        uncompressed += sum(
            parquet.metadata.row_group(i).total_byte_size
            for i in range(parquet.metadata.num_row_groups)
        )
        require(
            rows <= 1_000_000 and uncompressed <= 256 * 1024 * 1024,
            "SAMPLE_EXPANSION_TOO_LARGE",
        )
        tables.append(parquet.read())
    table = pa.concat_tables(tables)
    require(
        {
            "kafka_offset",
            "kafka_partition",
            "transaction_hash",
            "log_index",
            "block_timestamp",
            "json_value",
        }
        <= set(table.column_names),
        "BRONZE_SCHEMA_CHANGED",
    )
    offsets, partitions = (
        table.column("kafka_offset").to_pylist(),
        table.column("kafka_partition").to_pylist(),
    )
    start, end = result["start_offsets"], result["end_offsets"]
    require(
        set(start) == set(end) == {"0"}
        and all(type(x) is int and x >= 0 for x in [*start.values(), *end.values()]),
        "OFFSET_PARTITIONS_CHANGED",
    )
    require(
        offsets
        and all(type(x) is int for x in offsets)
        and all(type(x) is int for x in partitions)
        and set(partitions) == {0},
        "KAFKA_METADATA_CHANGED",
    )
    require(
        len(offsets) == len(set(offsets)) == end["0"] - start["0"]
        and min(offsets) == start["0"]
        and max(offsets) == end["0"] - 1,
        "OFFSET_GAP_OR_DUPLICATE",
    )
    events = [json.loads(x) for x in table.column("json_value").to_pylist()]
    require(
        all(
            x.get("chain") == "arbitrum" and x.get("protocol") == "uniswap_v3"
            for x in events
        ),
        "CHAIN_OR_PROTOCOL_CHANGED",
    )
    hashes, indices = (
        table.column("transaction_hash").to_pylist(),
        table.column("log_index").to_pylist(),
    )
    require(
        all(isinstance(x, str) and re.fullmatch("0x[0-9a-fA-F]{64}", x) for x in hashes)
        and all(type(x) is int and x >= 0 for x in indices),
        "TRANSACTION_LOG_ID_CHANGED",
    )
    stamps = table.column("block_timestamp").to_pylist()
    now = now or datetime.now(timezone.utc)
    require(
        all(
            isinstance(x, datetime)
            and abs(
                (
                    now - (x.replace(tzinfo=timezone.utc) if x.tzinfo is None else x)
                ).total_seconds()
            )
            < 7200
            for x in stamps
        ),
        "BLOCK_TIMESTAMP_OUTSIDE_WINDOW",
    )
    metadata = {
        name: result[name]
        for name in (
            "committed_batch",
            "checkpoint_commit_exists",
            "start_offsets",
            "end_offsets",
            "compact_metadata",
            "artifact_count",
            "artifact_bytes",
        )
    }
    require(
        type(metadata["committed_batch"]) is int
        and metadata["committed_batch"] >= 0
        and metadata["checkpoint_commit_exists"] is True
        and type(metadata["compact_metadata"]) is bool
        and metadata["artifact_count"] == len(entries)
        and metadata["artifact_bytes"] == total,
        "SAMPLE_METADATA_CHANGED",
    )
    return {
        **metadata,
        "ok": True,
        "parquet_read_verified": True,
        "rows": table.num_rows,
        "all_chain_labels_arbitrum": True,
        "committed_offset_range_complete": True,
        "duplicate_kafka_offsets": 0,
        "duplicate_transaction_log_identities": len(hashes)
        - len(set(zip(hashes, indices))),
    }
