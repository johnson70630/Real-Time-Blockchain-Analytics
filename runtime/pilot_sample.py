"""Bounded public Bronze evidence; called only by the approved guest operator."""

import base64
import hashlib
import json
from urllib.parse import unquote, urlsplit

from runtime.contracts import BUCKET, TOPIC


def sample(operator):
    from runtime.pilot_operator import exclusive, require

    operator.guard()
    prefix = operator.release["pilot_prefix"]
    directory = operator.state / "qa"
    directory.mkdir(mode=0o700)
    downloaded = 0

    def listing(key):
        value = json.loads(
            operator.call(
                [
                    "aws",
                    "s3api",
                    "list-objects-v2",
                    "--bucket",
                    BUCKET,
                    "--prefix",
                    key,
                    "--max-keys",
                    "1000",
                    "--output",
                    "json",
                ]
            )
        )
        require(not value.get("IsTruncated"), "SAMPLE_LIST_TRUNCATED")
        return {x["Key"] for x in value.get("Contents", [])}

    def get(key, name, limit):
        nonlocal downloaded
        require(key.startswith(prefix + "/"), "SAMPLE_PREFIX_CHANGED")
        head = json.loads(
            operator.call(
                [
                    "aws",
                    "s3api",
                    "head-object",
                    "--bucket",
                    BUCKET,
                    "--key",
                    key,
                    "--output",
                    "json",
                ]
            )
        )
        require(
            type(head["ContentLength"]) is int and 0 <= head["ContentLength"] <= limit,
            "SAMPLE_OBJECT_TOO_LARGE",
        )
        downloaded += head["ContentLength"]
        require(downloaded <= 64 * 1024 * 1024, "SAMPLE_TOTAL_TOO_LARGE")
        target = directory / name
        exclusive(target, {})
        operator.call(
            [
                "aws",
                "s3api",
                "get-object",
                "--bucket",
                BUCKET,
                "--key",
                key,
                str(target),
                "--output",
                "json",
            ]
        )
        require(target.stat().st_size == head["ContentLength"], "SAMPLE_SIZE_CHANGED")
        return target.read_bytes()

    commits = listing(prefix + "/checkpoints/swaps_bronze/commits/")
    batches = sorted(
        int(x.rsplit("/", 1)[1]) for x in commits if x.rsplit("/", 1)[1].isdigit()
    )
    metadata = listing(prefix + "/bronze/swaps/_spark_metadata/")
    offset_keys = listing(prefix + "/checkpoints/swaps_bronze/offsets/")

    def offset_key(batch):
        return prefix + "/checkpoints/swaps_bronze/offsets/" + str(batch)

    regular = [
        batch
        for batch in batches
        if offset_key(batch) in offset_keys
        and (batch == 0 or offset_key(batch - 1) in offset_keys)
        and prefix + "/bronze/swaps/_spark_metadata/" + str(batch) in metadata
    ]
    compact_batches = [
        batch
        for batch in batches
        if offset_key(batch) in offset_keys
        and prefix + "/bronze/swaps/_spark_metadata/" + str(batch) + ".compact"
        in metadata
    ]
    selected, rows, compact = None, [], False
    for batch, compact in [(b, False) for b in regular] + [
        (b, True) for b in compact_batches
    ]:
        key = prefix + "/bronze/swaps/_spark_metadata/" + str(batch)
        if compact:
            key += ".compact"
        body = get(key, "meta-" + str(batch), 1024 * 1024).decode().splitlines()
        require(body and body[0] == "v1", "SPARK_METADATA_VERSION")
        rows = [json.loads(x) for x in body[1:] if x.strip()]
        require(all(x.get("action") == "add" for x in rows), "NON_APPEND_SINK_METADATA")
        if rows:
            selected = batch
            break
    require(
        selected is not None and 0 < len(rows) <= 32,
        "POSITIVE_COMMITTED_BATCH_REQUIRED",
    )

    def offsets(batch):
        data = (
            get(
                prefix + "/checkpoints/swaps_bronze/offsets/" + str(batch),
                "offset-" + str(batch),
                1024 * 1024,
            )
            .decode()
            .splitlines()
        )
        values = [json.loads(x) for x in data if x.startswith("{")]
        matches = [x[TOPIC] for x in values if TOPIC in x]
        require(
            len(matches) == 1
            and all(type(v) is int and v >= 0 for v in matches[0].values()),
            "OFFSET_LOG_REQUIRED",
        )
        return {str(k): v for k, v in matches[0].items()}

    end = offsets(selected)
    start = {"0": 0} if selected == 0 or compact else offsets(selected - 1)
    artifacts, total = [], 0
    for index, row in enumerate(rows):
        parsed = urlsplit(row["path"])
        require(
            parsed.scheme in {"s3a", "s3"}
            and parsed.netloc == BUCKET
            and not parsed.query
            and not parsed.fragment,
            "SAMPLE_URI_CHANGED",
        )
        key = unquote(parsed.path.lstrip("/"))
        require(
            key.startswith(prefix + "/bronze/swaps/protocol=uniswap_v3/chain=arbitrum/")
            and key.endswith(".parquet")
            and "/../" not in key,
            "SAMPLE_PARTITION_CHANGED",
        )
        value = get(key, "part-" + str(index) + ".parquet", 16 * 1024 * 1024)
        total += len(value)
        require(total <= 64 * 1024 * 1024, "SAMPLE_TOTAL_TOO_LARGE")
        artifacts.append(
            {
                "sha256": hashlib.sha256(value).hexdigest(),
                "base64": base64.b64encode(value).decode(),
            }
        )
    return {
        "committed_batch": selected,
        "checkpoint_commit_exists": True,
        "start_offsets": start,
        "end_offsets": end,
        "compact_metadata": compact,
        "artifact_count": len(artifacts),
        "artifact_bytes": total,
        "_artifact_payloads": artifacts,
    }
