"""Offline safety, lifecycle, and deployment contracts for Milestone 11.6C."""

from __future__ import annotations

import json
import logging
import subprocess
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from runtime import host, producer, stream
from runtime.contracts import (
    MOUNT,
    PILOT_PREFIX,
    REGISTRY,
    TOPIC,
    validate_environment,
    validate_release,
    writer_lock,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def release():
    return {
        "instance_id": "i-" + "1" * 17,
        "kafka_volume_id": "vol-" + "2" * 17,
        "kafka_filesystem_uuid": "12345678-1234-1234-1234-123456789abc",
        "cluster_id": "MkU3OEVBNTcwNTJENDM2Qk",
        "producer_image": f"{REGISTRY}/blockchain-runtime-producer@sha256:" + "a" * 64,
        "spark_image": f"{REGISTRY}/blockchain-runtime-spark@sha256:" + "b" * 64,
        "kafka_image": f"{REGISTRY}/blockchain-runtime-producer@sha256:" + "c" * 64,
        "secret_arn": "arn:aws:secretsmanager:us-west-2:209479274979:secret:pilot/alchemy-ABC123",
        "pilot_prefix": PILOT_PREFIX,
        "max_runtime_seconds": 3600,
    }


def test_example_blocks_start_before_external_access():
    example = json.loads((ROOT / "deploy/runtime/release.example.json").read_text())
    with pytest.raises(ValueError):
        validate_release(example)


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_runtime_seconds", True),
        ("max_runtime_seconds", 0),
        ("max_runtime_seconds", 7201),
        ("pilot_prefix", "blockchain-analytics/production"),
        ("kafka_image", "apache/kafka:3.9.1"),
        ("spark_image", "other@sha256:" + "a" * 64),
        ("secret_arn", "*"),
        ("kafka_volume_id", "vol-unknown"),
        ("cluster_id", "different"),
        ("kafka_filesystem_uuid", "unknown"),
    ],
)
def test_release_rejects_changed_scope(release, field, value):
    validate_release(release)
    release[field] = value
    with pytest.raises(ValueError):
        validate_release(release)


def test_cloud_environment_and_no_inherited_credentials():
    compose = yaml.safe_load((ROOT / "deploy/runtime/compose.yaml").read_text())
    env = compose["services"]["spark"]["environment"]
    validate_environment(env)
    with pytest.raises(ValueError, match="STATIC_CREDENTIALS"):
        validate_environment(dict(env, AWS_ACCESS_KEY_ID="test"))
    with pytest.raises(ValueError, match="PREFIX"):
        validate_environment(dict(env, DATA_LAKE_PREFIX="production"))


def test_single_writer_lock_never_unlinks_inode(tmp_path):
    lock = tmp_path / "writer.lock"
    with writer_lock(lock):
        with pytest.raises(RuntimeError, match="ALREADY_OWNED"):
            with writer_lock(lock):
                pytest.fail("Second checkpoint owner admitted")
    assert lock.exists()
    with writer_lock(lock):
        pass


def test_mount_identity_checks_actual_device_and_filesystem(release):
    mount = {
        "target": str(MOUNT),
        "fstype": "xfs",
        "uuid": release["kafka_filesystem_uuid"],
    }

    def verify(value, serial):
        host.verify_mount(release, value, serial)

    verify(mount, release["kafka_volume_id"].replace("-", ""))
    for key, bad in [("target", "/"), ("fstype", "ext4"), ("uuid", "wrong")]:
        with pytest.raises(RuntimeError, match="IDENTITY"):
            verify(dict(mount, **{key: bad}), release["kafka_volume_id"])
    with pytest.raises(RuntimeError, match="IDENTITY"):
        verify(mount, "vol-root")


def test_secret_payload_is_not_a_docker_environment_variable(release, monkeypatch):
    monkeypatch.setenv("ALCHEMY_WEBSOCKET_URL", "never-inherit")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "never-inherit")
    monkeypatch.setenv("COMPOSE_FILE", "never-inherit")
    env = host.compose_environment(release)
    assert not any(
        key in env
        for key in ("ALCHEMY_WEBSOCKET_URL", "AWS_ACCESS_KEY_ID", "COMPOSE_FILE")
    )
    assert env["AWS_SHARED_CREDENTIALS_FILE"] == "/dev/null"
    assert env["COMPOSE_DISABLE_ENV_FILE"] == "1"


def test_cli_errors_never_expose_captured_payload(monkeypatch):
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1, stdout="secret-payload", stderr="secret-response"
        ),
    )
    with pytest.raises(RuntimeError) as error:
        host.command(["aws", "test"])
    assert str(error.value) == "AWS_COMMAND_FAILED"


def test_startup_commands_cannot_outlive_runtime_budget(release, monkeypatch):
    pilot = host.Pilot(release)
    timeouts = []
    monkeypatch.setattr(host.time, "monotonic", lambda: 100)
    monkeypatch.setattr(
        host, "command", lambda _args, **kwargs: timeouts.append(kwargs["timeout"])
    )
    pilot.deadline = 110
    pilot.execute(["docker", "test"], timeout=600)
    assert timeouts == [10]
    pilot.deadline = 99
    with pytest.raises(RuntimeError, match="DURATION_EXPIRED"):
        pilot.execute(["docker", "test"])
    pilot.stopping = True
    pilot.execute(["docker", "stop"], timeout=90)
    assert timeouts == [10, 90]


def test_producer_delivery_acknowledgement_and_failure(monkeypatch):
    class Publisher:
        failure = None
        remaining = 0

        def __init__(self, config):
            self.config = config

        def produce(self, _topic, **kwargs):
            self.callback = kwargs["on_delivery"]

        def flush(self, _timeout):
            self.callback(self.failure, None)
            return self.remaining

    monkeypatch.setattr(producer, "Producer", Publisher)
    publisher = producer.ConfirmedKafkaProducer("offline", "test")
    assert publisher.producer.config["enable.idempotence"] is True
    assert publisher.producer.config["acks"] == "all"
    publisher.send({"transaction_hash": "synthetic"})
    for failure, remaining in [("never-log-this-provider-error", 0), (None, 1)]:
        publisher.producer.failure, publisher.producer.remaining = failure, remaining
        with pytest.raises(RuntimeError, match="NOT_CONFIRMED"):
            publisher.send({})


def test_reconnect_resubscribes_and_backoff_is_bounded(caplog):
    calls, sleeps = [], []

    def fail_then_recover(**kwargs):
        calls.append(kwargs)
        if len(calls) < 10:
            raise OSError("credential-containing exception must not be logged")

    with caplog.at_level(logging.WARNING):
        producer.reconnect(fail_then_recover, sleep=sleeps.append, jitter=lambda *_: 0)
    assert len(calls) == 10
    assert sleeps == [1, 2, 4, 8, 16, 32, 60, 60, 60]
    assert all(
        call["kafka_factory"] is producer.ConfirmedKafkaProducer for call in calls
    )
    assert "RECOVERY_REQUIRED" in caplog.text
    assert "credential-containing" not in caplog.text


def test_cloud_kafka_factory_receives_canonical_normalized_event(monkeypatch):
    from eth_abi import encode

    from producer import run_producer
    from producer.event_handlers import SwapEventHandler

    messages = iter(
        [
            {
                "method": "eth_subscription",
                "params": {
                    "result": {
                        "address": "0x" + "11" * 20,
                        "topics": [
                            SwapEventHandler().topic,
                            "0x" + encode(["address"], ["0x" + "22" * 20]).hex(),
                            "0x" + encode(["address"], ["0x" + "33" * 20]).hex(),
                        ],
                        "data": "0x"
                        + encode(
                            ["int256", "int256", "uint160", "uint128", "int24"],
                            [-10, 20, 2**96, 1000, -120],
                        ).hex(),
                        "blockNumber": "0x64",
                        "transactionHash": "0x" + "ab" * 32,
                        "transactionIndex": "0x0",
                        "blockHash": "0x" + "cd" * 32,
                        "logIndex": "0x0",
                        "removed": False,
                    }
                },
            }
        ]
    )
    sent = []

    class Client:
        def __init__(self, _url):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def subscribe_logs(self, *_args):
            return 1

        def receive(self):
            try:
                return next(messages)
            except StopIteration:
                raise KeyboardInterrupt from None

        def get_block_timestamp(self, _number):
            return "2026-01-01T00:00:00+00:00"

    class Publisher(Client):
        def __init__(self, *_args):
            pass

        def send(self, event):
            sent.append(event)

    monkeypatch.setattr(run_producer, "AlchemyClient", Client)
    monkeypatch.setattr(
        run_producer, "get_alchemy_websocket_url", lambda: "wss://example.invalid"
    )
    monkeypatch.setattr(run_producer, "ENABLED_PROTOCOLS", ("uniswap_v3",))
    run_producer.run_producer(kafka_factory=Publisher)
    assert len(sent) == 1
    assert sent[0]["protocol"] == "uniswap_v3"
    assert sent[0]["block_number"] == 100
    assert sent[0]["producer_version"] and sent[0]["schema_version"]


def test_secret_reading_and_redaction(tmp_path):
    path = tmp_path / "url"
    key = "synthetic" + "-key"
    value = "wss://arb-mainnet.g.alchemy.com/v2/" + key
    path.write_text(value)
    assert producer.secret_url(path) == value
    record = logging.LogRecord(
        "test", logging.ERROR, "", 0, "failure %s key=%s", (value, key), None
    )
    producer.RedactEndpoint(value).filter(record)
    assert key not in record.getMessage()
    path.write_text("ws://insecure.invalid/key")
    with pytest.raises(ValueError):
        producer.secret_url(path)


def test_checkpoint_disappearance_blocks_automatic_fresh_start():
    stream.ensure_checkpoint_pair(False, False, False)
    stream.ensure_checkpoint_pair(True, True, True)
    with pytest.raises(RuntimeError, match="MISSING"):
        stream.ensure_checkpoint_pair(True, False, False)
    with pytest.raises(RuntimeError, match="INCOMPLETE"):
        stream.ensure_checkpoint_pair(True, True, False)
    with pytest.raises(RuntimeError, match="BRONZE_MISSING"):
        stream.ensure_checkpoint_pair(False, True, True)
    assert stream.stream_options()["startingOffsets"] == "earliest"
    assert stream.stream_options()["failOnDataLoss"] == "true"


def test_drain_requires_all_partition_offsets():
    progress = {"sources": [{"endOffset": {TOPIC: {"0": 10, "1": 20}}}]}
    stream.verify_drained(progress, {"0": 10, "1": 20})
    # Spark 4 serializes Kafka endOffset as a JSON string in SourceProgress.
    stream.verify_drained(
        {"sources": [{"endOffset": json.dumps({TOPIC: {"0": 10, "1": 20}})}]},
        {"0": 10, "1": 20},
    )
    for ends in [{"0": 11, "1": 20}, {"0": 10}, {"0": 10, "1": 20, "2": 1}]:
        with pytest.raises(RuntimeError, match="OFFSETS"):
            stream.verify_drained(progress, ends)


def test_health_requires_progress_recent_heartbeat_and_active_writer(tmp_path):
    path = tmp_path / "status.json"
    assert not stream.healthy(path)
    stream.write_status({}, path=path)
    assert not stream.healthy(path)


def test_spark_progress_canonical_json_handles_uuid_objects(tmp_path):
    from uuid import UUID

    class Progress(dict):
        @property
        def json(self):
            return json.dumps({"id": str(self["id"]), "batchId": 1})

    progress = Progress(id=UUID("12345678-1234-1234-1234-123456789abc"))
    path = tmp_path / "status.json"
    stream.write_status(progress, path=path)
    assert json.loads(path.read_text())["progress"]["id"] == str(progress["id"])
    stream.write_status({"batchId": 1}, path=path)
    assert stream.healthy(path)
    status = json.loads(path.read_text())
    assert not stream.healthy(path, now=status["heartbeat"] + 61)
    stream.write_status({"batchId": 1}, path=path, drained=True)
    assert not stream.healthy(path)


def test_drain_verifies_committed_batch_even_when_last_trigger_is_idle():
    calls = []
    progress = {
        "batchId": 2,
        "numInputRows": 0,
        "sources": [{"endOffset": json.dumps({TOPIC: {"0": 7}})}],
    }
    query = SimpleNamespace(
        lastProgress=progress,
        processAllAvailable=lambda: calls.append("drain"),
        stop=lambda: calls.append("stop"),
    )
    result = stream.drain_query(
        query, lambda batch: batch == 1, get_offsets=lambda: {"0": 7}
    )
    assert result["verifiedCheckpointBatchId"] == 1
    assert calls == ["drain", "stop"]
    with pytest.raises(RuntimeError, match="COMMIT_NOT_FOUND"):
        stream.drain_query(query, lambda _batch: False, get_offsets=lambda: {"0": 7})


def test_shutdown_order_and_failed_drain_preserves_broker(
    release, monkeypatch, tmp_path
):
    monkeypatch.setattr(host, "RUN", tmp_path)
    (tmp_path / "pilot").mkdir()
    status = tmp_path / "pilot/status.json"
    status.write_text(json.dumps({"drained": True}))
    pilot = host.Pilot(release)
    calls = []
    monkeypatch.setattr(pilot, "compose", lambda *args, **_kwargs: calls.append(args))
    monkeypatch.setattr(
        pilot, "state", lambda _service: {"ExitCode": 0, "Running": True}
    )
    pilot.stop()
    assert [args[-1] for args in calls] == ["producer", "spark", "kafka"]
    monkeypatch.setattr(
        pilot,
        "state",
        lambda service: {"ExitCode": 143 if service == "kafka" else 0, "Running": True},
    )
    pilot.stop()
    calls.clear()
    status.write_text(json.dumps({"drained": False}))
    with pytest.raises(RuntimeError, match="DRAIN_NOT_VERIFIED"):
        pilot.stop()
    assert not any(args[-1] == "kafka" for args in calls)


def test_compose_contract_has_no_host_ports_or_credentials():
    content = yaml.safe_load((ROOT / "deploy/runtime/compose.yaml").read_text())
    services = content["services"]
    for service in services.values():
        assert "ports" not in service
        assert "build" not in service
        assert service["restart"] == "no"
        assert service["ulimits"]["core"] == {"soft": 0, "hard": 0}
        assert service["logging"]["driver"] == "awslogs"
        assert service["logging"]["options"]["awslogs-create-group"] == "false"
    assert "EXTERNAL" not in str(services["kafka"]["environment"])
    assert services["kafka"]["volumes"][0]["bind"]["create_host_path"] is False
    assert services["producer"]["secrets"] == ["alchemy_websocket_url"]
    assert "secrets" not in services["spark"]
    assert "secrets" not in services["kafka"]
    assert services["spark"]["read_only"] is True


def test_images_pin_base_python_dependencies_and_connector_hashes():
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    pins = {package["name"]: package["version"] for package in lock["package"]}
    for component in ("producer", "spark"):
        dockerfile = (ROOT / f"deploy/runtime/Dockerfile.{component}").read_text()
        assert "@sha256:" in dockerfile
        assert "--require-hashes" in dockerfile
        assert "USER 10001:10001" in dockerfile
        requirements = (
            ROOT / f"deploy/runtime/{component}-requirements.txt"
        ).read_text()
        for line in requirements.splitlines():
            if line and not line.startswith((" ", "#")):
                name, value = line.split("==", 1)
                version = value.split()[0].rstrip("\\")
                assert pins[name] == version
    jars = json.loads((ROOT / "deploy/runtime/jars.lock.json").read_text())
    assert len(jars) == 8
    assert all(len(artifact["sha512"]) == 128 for artifact in jars)
    spark_image = (ROOT / "deploy/runtime/Dockerfile.spark").read_text()
    assert "openjdk-17-jre-headless" in spark_image
    assert "fetch_jars.py" in spark_image
    assert "spark.jars.packages" not in (ROOT / "runtime/stream.py").read_text()


def test_deadline_and_service_never_enable_automatic_boot():
    service = (ROOT / "deploy/runtime/blockchain-pilot.service").read_text()
    timer = (ROOT / "deploy/runtime/blockchain-pilot-deadline.timer").read_text()
    assert "Restart=no" in service and "LimitCORE=0" in service
    assert "ExecStopPost=/usr/bin/systemctl --no-block poweroff" in service
    assert "OnActiveSec=150min" in timer
    assert "[Install]" not in service + timer
