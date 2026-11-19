"""Bounded metadata reads and single-use recovery with temporary fixtures only."""

from copy import deepcopy
import io
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from runtime.contracts import BUCKET, TOPIC
from runtime import recovery_metadata as m, recovery_operator as op
from runtime.recovery import RecoveryBlocked


def write_batch(env, batch, end):
    files = {
        f"checkpoint/commits/{batch}": b'v1\n{"nextBatchWatermarkMs":0}\n',
        f"checkpoint/offsets/{batch}": (
            "v1\n{}\n" + json.dumps({env.broker["topic"]: {"0": end}})
        ).encode(),
        f"bronze/_spark_metadata/{batch}": (
            "v1\n"
            + json.dumps(
                {
                    "action": "add",
                    "path": (env.root / "bronze" / f"part-{batch}.parquet").as_uri(),
                }
            )
        ).encode(),
    }
    for name, value in files.items():
        target = env.root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(value)


@pytest.fixture
def env(tmp_path):
    env = SimpleNamespace(root=tmp_path.resolve(), active=False, now=100.0, calls=[])
    env.broker = {
        "cluster_id": "MkU3OEVBNTcwNTJENDM2Qk",
        "topic_id": "NH7b0fLSTnKFKjVemmxT/A",
        "topic": "synthetic-pilot-events",
        "begin_offsets": {"0": 0},
        "end_offsets": {"0": 2},
    }
    write_batch(env, 13, 2)
    (env.root / "checkpoint/metadata").write_text(
        '{"id":"12345678-1234-1234-1234-123456789abc"}'
    )
    env.source = m.LocalMetadataSource(env.root)
    env.collector = m.MetadataCollector(
        env.source,
        lambda: deepcopy(env.broker),
        lambda: "synthetic-retained-volume",
        lambda: env.active,
        clock=lambda: env.now,
    )
    return env


def backend(env, failure=None):
    def action(name):
        def call(*args):
            env.calls.append(name)
            if name == "start_reader":
                env.active = True
            if name == failure:
                raise RuntimeError("private-provider-diagnostic-must-not-escape")
            if name == "start_source":
                env.broker["end_offsets"]["0"] = 3
            if name == "drain_reader":
                write_batch(env, 14, 3)
            if name == "stop_reader":
                env.active = False
            return True

        return call

    return SimpleNamespace(
        profile="synthetic-offline",
        **{
            name: action(name)
            for name in [
                "start_reader",
                "start_source",
                "stop_source",
                "drain_reader",
                "stop_reader",
                "verify_output",
            ]
        },
    )


def run(env, base, driver, run_id=None, **kwargs):
    return op.run_offline(
        base,
        m.digest(base),
        env.collector,
        driver,
        run_id or str(uuid4()),
        clock=lambda: env.now,
        **kwargs,
    )


def test_collection_is_stable_bounded_and_contains_no_file_payload(env):
    record = env.collector.collect()
    assert record["snapshot"]["checkpoint_batch"] == 13
    assert record["snapshot"]["checkpoint_end_offsets"] == {"0": 2}
    assert record["snapshot_sha256"] == m.digest(record["snapshot"])
    assert len(record["metadata_sha256"]) == 4
    assert (
        record["baseline_approved"] is False
        and record["cloud_start_authorized"] is False
    )
    assert "part-13" not in json.dumps(record)
    assert (
        op.preview(record, record, m.digest(record), now=100)["cloud_start_authorized"]
        is False
    )


def test_empty_and_compacted_append_logs(env):
    log = env.root / "bronze/_spark_metadata/13"
    log.write_bytes(b"v1\n")
    assert env.collector.collect()["snapshot"]["sink_batch"] == 13
    log.rename(log.with_suffix(".compact"))
    assert (
        "bronze/_spark_metadata/13.compact"
        in env.collector.collect()["metadata_sha256"]
    )


@pytest.mark.parametrize(
    "name,value,code",
    [
        (
            "checkpoint/metadata",
            b'{"id":"first","id":"second"}',
            "METADATA_DUPLICATE_JSON_KEY",
        ),
        ("checkpoint/metadata", b'{"id":NaN}', "METADATA_JSON_INVALID"),
        ("checkpoint/metadata", b'{"id":1e9999}', "METADATA_JSON_INVALID"),
        ("checkpoint/commits/13", b"v2\n{}", "METADATA_LOG_VERSION_INVALID"),
        (
            "checkpoint/offsets/13",
            b'v1\n{}\n{"other":{"0":2}}',
            "CHECKPOINT_OFFSETS_INVALID",
        ),
        (
            "bronze/_spark_metadata/13",
            b'v1\n{"action":"delete","path":"x"}',
            "NON_APPEND_SINK_METADATA",
        ),
        (
            "bronze/_spark_metadata/13",
            b'v1\n{"action":"add","path":"file:///outside/part.parquet"}',
            "SINK_METADATA_PATH_CHANGED",
        ),
    ],
)
def test_malformed_or_changed_metadata_is_refused(env, name, value, code):
    (env.root / name).write_bytes(value)
    with pytest.raises(RecoveryBlocked, match=code):
        env.collector.collect()


def test_uncommitted_offset_batch_and_ambiguous_sink_are_refused(env):
    (env.root / "checkpoint/offsets/14").write_bytes(b"v1\n{}\n{}")
    with pytest.raises(RecoveryBlocked, match="RECOVERY_PARTIAL_BATCH_REQUIRES_REVIEW"):
        env.collector.collect()
    (env.root / "checkpoint/offsets/14").unlink()
    (env.root / "bronze/_spark_metadata/13.compact").write_bytes(b"v1\n")
    with pytest.raises(RecoveryBlocked, match="METADATA_BATCH_AMBIGUOUS"):
        env.collector.collect()


@pytest.mark.parametrize(
    "key",
    [
        "../outside",
        "checkpoint/../secret",
        "bronze/part.parquet",
        "checkpoint/metadata/child",
    ],
)
def test_only_exact_metadata_keys_can_be_read(env, key):
    with pytest.raises(RecoveryBlocked, match="METADATA_KEY_FORBIDDEN"):
        env.source.read(key)


def test_symlinked_metadata_is_not_followed(env):
    target = env.root / "checkpoint/metadata"
    target.unlink()
    target.symlink_to(env.root / "checkpoint/commits/13")
    with pytest.raises(RecoveryBlocked, match="METADATA_FILE_UNSAFE"):
        env.collector.collect()


def test_active_writer_blocks_before_storage_reads(env, monkeypatch):
    env.active = True
    monkeypatch.setattr(
        env.source, "read", lambda key: pytest.fail("read with active writer")
    )
    with pytest.raises(RecoveryBlocked, match="EXISTING_WRITER_RECOVERY_BLOCKED"):
        env.collector.collect()


def test_changed_object_between_reads_is_refused(env, monkeypatch):
    original, counts = env.source.read, {}

    def read(key):
        counts[key] = counts.get(key, 0) + 1
        value = original(key)
        return value + b" " if counts[key] > 1 else value

    monkeypatch.setattr(env.source, "read", read)
    with pytest.raises(RecoveryBlocked, match="METADATA_OBJECT_CHANGED"):
        env.collector.collect()


def test_changing_broker_storage_or_clock_is_refused(env):
    calls = iter(
        [deepcopy(env.broker), {**env.broker, "topic_id": "AAAAAAAAAAEAAAAAAAAAAg"}]
    )
    env.collector.broker = lambda: next(calls)
    with pytest.raises(RecoveryBlocked, match="RECOVERY_IDENTITY_OR_OFFSETS_CHANGED"):
        env.collector.collect()
    env.collector.broker = lambda: deepcopy(env.broker)
    clock = iter([100, 131])
    env.collector.clock = lambda: next(clock)
    with pytest.raises(RecoveryBlocked, match="METADATA_OBSERVATION_STALE"):
        env.collector.collect()


def test_read_budgets_fail_closed(env, monkeypatch):
    monkeypatch.setattr(m, "MAX_TOTAL", 10)
    with pytest.raises(RecoveryBlocked, match="METADATA_TOTAL_TOO_LARGE"):
        env.collector.collect()


def test_provider_exception_text_never_escapes(env, capsys):
    def broken():
        raise RecoveryBlocked("private-provider-diagnostic-must-not-escape")

    env.collector.broker = broken
    with pytest.raises(RecoveryBlocked, match="^METADATA_COLLECTION_FAILED$"):
        env.collector.collect()
    assert not capsys.readouterr().out


class FakeS3:
    def __init__(self, env):
        self.calls, self.bodies, self.truncated, self.oversized = [], [], False, False
        source = m.S3MetadataSource(self)
        self.data = {}
        for path in env.root.rglob("*"):
            if path.is_file():
                key = path.relative_to(env.root).as_posix()
                self.data[source._key(key)] = (
                    path.read_bytes()
                    .replace(
                        (env.root / "bronze").as_uri().encode(), source.output.encode()
                    )
                    .replace(b"synthetic-pilot-events", TOPIC.encode())
                )

    def list_objects_v2(self, **kw):
        self.calls.append(("list", kw))
        assert kw["Bucket"] == BUCKET and kw["MaxKeys"] == m.MAX_KEYS
        return {
            "IsTruncated": self.truncated,
            "Contents": [
                {"Key": key}
                for key in sorted(self.data)
                if key.startswith(kw["Prefix"])
            ],
        }

    def head_object(self, **kw):
        self.calls.append(("head", kw))
        return {
            "ContentLength": m.MAX_OBJECT + 1
            if self.oversized
            else len(self.data[kw["Key"]]),
            "ETag": '"fixture-etag"',
        }

    def get_object(self, **kw):
        self.calls.append(("get", kw))
        value = self.data[kw["Key"]]
        assert (
            kw["IfMatch"] == '"fixture-etag"'
            and kw["Range"] == f"bytes=0-{len(value) - 1}"
        )
        body = io.BytesIO(value)
        self.bodies.append(body)
        return {"Body": body, "ETag": '"fixture-etag"'}


def s3_collector(env):
    client = FakeS3(env)
    state = {**deepcopy(env.broker), "topic": TOPIC}
    return client, m.MetadataCollector(
        m.S3MetadataSource(client),
        lambda: state,
        lambda: "reviewed-volume-and-filesystem",
        lambda: False,
        clock=lambda: 100,
    )


def test_s3_adapter_is_injected_read_only_scoped_and_closes_bodies(env):
    client, collector = s3_collector(env)
    record = collector.collect()
    assert record["snapshot"]["scope"]["output"] == collector.source.output
    assert {name for name, _ in client.calls} == {"list", "head", "get"}
    assert all(body.closed for body in client.bodies)
    with pytest.raises(RecoveryBlocked, match="METADATA_KEY_FORBIDDEN"):
        collector.source.read("bronze/part.parquet")


@pytest.mark.parametrize(
    "case,code",
    [
        ("truncated", "METADATA_LIST_TRUNCATED"),
        ("oversized", "METADATA_OBJECT_TOO_LARGE_OR_EMPTY"),
    ],
)
def test_s3_limits_stop_before_unbounded_download(env, case, code):
    client, collector = s3_collector(env)
    setattr(client, case, True)
    with pytest.raises(RecoveryBlocked, match=code):
        collector.collect()
    assert not any(name == "get" for name, _ in client.calls)


def test_operator_runs_once_preserves_baseline_and_checks_output(env):
    base = env.collector.collect()
    original = deepcopy(base)
    driver, run_id = backend(env), str(uuid4())
    result = run(env, base, driver, run_id)
    assert result["status"] == "OFFLINE_RECOVERY_COMPLETED"
    assert result["processed_offset_range"] == {"start": 2, "end": 3}
    assert base == original and env.active is False
    assert env.calls == [
        "start_reader",
        "start_source",
        "stop_source",
        "drain_reader",
        "stop_reader",
        "verify_output",
    ]
    previous = list(env.calls)
    with pytest.raises(RecoveryBlocked, match="RECOVERY_ATTEMPT_ALREADY_CONSUMED"):
        run(env, base, driver, run_id)
    assert env.calls == previous


@pytest.mark.parametrize(
    "phase",
    [
        "start_reader",
        "start_source",
        "stop_source",
        "drain_reader",
        "stop_reader",
        "verify_output",
    ],
)
def test_partial_failure_cleans_up_and_never_writes_success(env, phase):
    base, driver = env.collector.collect(), backend(env, phase)
    with pytest.raises(RecoveryBlocked) as caught:
        run(env, base, driver)
    assert "private-provider" not in str(caught.value)
    assert "stop_reader" in env.calls
    assert not list((env.root / ".recovery-operator").glob("*.result.json"))
    if phase in {"start_source", "stop_source"}:
        assert "stop_source" in env.calls


def test_expired_budget_still_stops_both_workers(env):
    base, driver = env.collector.collect(), backend(env)

    def delayed_source():
        env.now += 2
        return True

    driver.start_source = delayed_source
    with pytest.raises(RecoveryBlocked, match="RECOVERY_EXECUTION_EXPIRED"):
        run(env, base, driver, max_seconds=1)
    assert "stop_source" in env.calls and "stop_reader" in env.calls


def test_cloud_profiles_and_s3_execution_are_denied_before_callbacks(env):
    base, driver = env.collector.collect(), backend(env)
    driver.profile = "production"
    with pytest.raises(RecoveryBlocked, match="OFFLINE_EXECUTION_ONLY"):
        run(env, base, driver)
    driver.profile = "synthetic-offline"
    client, collector = s3_collector(env)
    with pytest.raises(RecoveryBlocked, match="OFFLINE_EXECUTION_ONLY"):
        op.run_offline(base, m.digest(base), collector, driver, str(uuid4()))
    assert not client.calls and not env.calls


def test_baseline_hash_staleness_and_concurrent_writer_block_before_start(env):
    base, driver = env.collector.collect(), backend(env)
    with pytest.raises(RecoveryBlocked, match="RECOVERY_BASELINE_HASH_CHANGED"):
        op.run_offline(base, "0" * 64, env.collector, driver, str(uuid4()))
    with pytest.raises(RecoveryBlocked, match="RECOVERY_OBSERVATION_STALE"):
        op.preview(base, base, m.digest(base), now=131)
    directory = env.root / ".recovery-operator"
    directory.mkdir(mode=0o700)
    with op._lock(directory):
        with pytest.raises(RecoveryBlocked, match="CHECKPOINT_ALREADY_OWNED"):
            run(env, base, driver)
    assert not env.calls


def test_semantically_equal_but_rewritten_metadata_requires_review(env):
    baseline = env.collector.collect()
    path = env.root / "checkpoint/metadata"
    path.write_bytes(path.read_bytes() + b" ")
    observed = env.collector.collect()
    assert observed["snapshot_sha256"] == baseline["snapshot_sha256"]
    with pytest.raises(RecoveryBlocked, match="RECOVERY_METADATA_HASH_CHANGED"):
        op.preview(baseline, observed, m.digest(baseline), now=100)


def test_backend_claimed_drain_without_commit_never_succeeds(env):
    baseline, driver = env.collector.collect(), backend(env)
    driver.drain_reader = lambda: True
    with pytest.raises(RecoveryBlocked, match="RECOVERY_DRAIN_NOT_COMMITTED"):
        run(env, baseline, driver)
    assert not env.active
    assert not list((env.root / ".recovery-operator").glob("*.result.json"))


def test_actual_pilot_topic_cannot_use_synthetic_executor(env):
    baseline, driver = env.collector.collect(), backend(env)
    baseline["snapshot"]["scope"]["topic"] = TOPIC
    baseline["snapshot_sha256"] = m.digest(baseline["snapshot"])
    with pytest.raises(RecoveryBlocked, match="OFFLINE_EXECUTION_ONLY"):
        run(env, baseline, driver)
    assert not env.calls


def test_s3_changed_etag_closes_body_and_refuses_snapshot(env, monkeypatch):
    client, collector = s3_collector(env)
    original = client.get_object

    def changed(**kwargs):
        response = original(**kwargs)
        response["ETag"] = '"changed-etag"'
        return response

    monkeypatch.setattr(client, "get_object", changed)
    with pytest.raises(RecoveryBlocked, match="METADATA_OBJECT_CHANGED"):
        collector.collect()
    assert client.bodies and all(body.closed for body in client.bodies)


@pytest.mark.parametrize(
    "partitions,internal", [([0], False), ([0, 1], False), ([1], False), ([0], True)]
)
def test_kafka_probe_reads_existing_topic_without_auto_creation(
    env, monkeypatch, partitions, internal
):
    import confluent_kafka
    import confluent_kafka.admin

    configs, closed = [], []

    class Future:
        def __init__(self, value):
            self.value = value

        def result(self, timeout):
            assert timeout == 10
            return self.value

    class Admin:
        def __init__(self, config):
            configs.append(config)

        def describe_cluster(self, **kwargs):
            assert kwargs == {"request_timeout": 8}
            return Future(SimpleNamespace(cluster_id=env.broker["cluster_id"]))

        def describe_topics(self, topics, **kwargs):
            assert kwargs == {"request_timeout": 8}
            return {
                "synthetic-pilot-events": Future(
                    SimpleNamespace(
                        name="synthetic-pilot-events",
                        is_internal=internal,
                        partitions=[SimpleNamespace(id=p) for p in partitions],
                        topic_id=confluent_kafka.Uuid(1, 2),
                    )
                )
            }

    class Consumer:
        def __init__(self, config):
            configs.append(config)

        def get_watermark_offsets(self, partition, **kwargs):
            assert (
                partition.topic == "synthetic-pilot-events" and partition.partition == 0
            )
            assert kwargs == {"timeout": 8, "cached": False}
            return 0, 2

        def close(self):
            closed.append(True)

    monkeypatch.setattr(confluent_kafka.admin, "AdminClient", Admin)
    monkeypatch.setattr(confluent_kafka, "Consumer", Consumer)
    probe = m.KafkaMetadataProbe("synthetic-pilot-events")
    if partitions == [0] and not internal:
        assert probe()["end_offsets"] == {"0": 2}
    else:
        with pytest.raises(RecoveryBlocked, match="BROKER_METADATA_INVALID"):
            probe()
    assert configs[1]["allow.auto.create.topics"] is False
    assert configs[1]["enable.auto.commit"] is False and closed == [True]


def test_invalid_run_id_and_duration_never_start(env):
    baseline, driver = env.collector.collect(), backend(env)
    with pytest.raises(RecoveryBlocked, match="RECOVERY_RUN_ID_INVALID"):
        run(env, baseline, driver, run_id="not-a-run")
    with pytest.raises(RecoveryBlocked, match="RECOVERY_DURATION_INVALID"):
        run(env, baseline, driver, max_seconds=True)
    assert not env.calls
