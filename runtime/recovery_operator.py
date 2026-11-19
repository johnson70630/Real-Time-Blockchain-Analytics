"""Recovery preview and single-use synthetic execution. No production executor.

No AWS/SSM/Docker client or subprocess is constructed here. The only execution
entrypoint requires a local metadata source and the synthetic topic/profile.
"""

from contextlib import contextmanager
import fcntl
import math
import os
import re
import stat
import time
from uuid import UUID

from runtime.recovery import RecoveryBlocked, _require, _validate, assess_recovery
from runtime.recovery_metadata import (
    LocalMetadataSource,
    canonical,
    digest,
    parse_json,
    _safe_directory,
)

ENVELOPE_FIELDS = {
    "schema_version",
    "snapshot",
    "snapshot_sha256",
    "observed_at_monotonic",
    "metadata_sha256",
    "stable_read_verified",
    "baseline_approved",
    "cloud_start_authorized",
}


def _checked(envelope):
    _require(
        type(envelope) is dict and set(envelope) == ENVELOPE_FIELDS,
        "RECOVERY_ENVELOPE_INVALID",
    )
    _require(
        type(envelope["schema_version"]) is int
        and envelope["schema_version"] == 1
        and envelope["stable_read_verified"] is True
        and envelope["baseline_approved"] is False
        and envelope["cloud_start_authorized"] is False,
        "RECOVERY_ENVELOPE_INVALID",
    )
    _validate(envelope["snapshot"])
    _require(
        envelope["snapshot_sha256"] == digest(envelope["snapshot"]),
        "RECOVERY_SNAPSHOT_CHANGED",
    )
    hashes = envelope["metadata_sha256"]
    batch = envelope["snapshot"]["checkpoint_batch"]
    expected = {
        "checkpoint/metadata",
        f"checkpoint/commits/{batch}",
        f"checkpoint/offsets/{batch}",
    }
    _require(
        type(hashes) is dict
        and len(hashes) == 4
        and all(
            type(v) is str and re.fullmatch("[a-f0-9]{64}", v) for v in hashes.values()
        ),
        "RECOVERY_ENVELOPE_INVALID",
    )
    _require(
        set(hashes)
        in (
            expected | {f"bronze/_spark_metadata/{batch}"},
            expected | {f"bronze/_spark_metadata/{batch}.compact"},
        ),
        "RECOVERY_ENVELOPE_INVALID",
    )
    stamp = envelope["observed_at_monotonic"]
    _require(
        type(stamp) in (int, float) and math.isfinite(stamp) and stamp >= 0,
        "RECOVERY_ENVELOPE_INVALID",
    )
    return envelope["snapshot"]


def preview(baseline, observed, expected_baseline_sha256, now=None):
    """Hash binding is provenance, not approval. Both records remain unapproved."""
    before, current = _checked(baseline), _checked(observed)
    _require(
        digest(baseline) == expected_baseline_sha256, "RECOVERY_BASELINE_HASH_CHANGED"
    )
    now = time.monotonic() if now is None else now
    _require(
        type(now) in (int, float)
        and math.isfinite(now)
        and 0 <= now - observed["observed_at_monotonic"] <= 30,
        "RECOVERY_OBSERVATION_STALE",
    )
    assess_recovery(before, current)
    _require(
        baseline["metadata_sha256"] == observed["metadata_sha256"],
        "RECOVERY_METADATA_HASH_CHANGED",
    )
    return {
        "status": "RECOVERY_PREVIEW_ONLY",
        "checkpoint_batch": before["checkpoint_batch"],
        "baseline_sha256": expected_baseline_sha256,
        "actions": [
            "start_reader",
            "start_source",
            "stop_source",
            "drain_reader",
            "stop_reader",
            "verify_output",
        ],
        "live_checks_required": True,
        "cloud_start_authorized": False,
    }


def _exclusive(path, record):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(canonical(record) + b"\n")


@contextmanager
def _lock(directory):
    fd = os.open(
        directory / "writer.lock",
        os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
        0o600,
    )
    with os.fdopen(fd, "a") as handle:
        info = os.fstat(handle.fileno())
        _require(
            stat.S_ISREG(info.st_mode)
            and info.st_uid == os.geteuid()
            and info.st_nlink == 1
            and not info.st_mode & 0o077,
            "RECOVERY_LOCK_UNSAFE",
        )
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RecoveryBlocked("CHECKPOINT_ALREADY_OWNED") from None
        yield


def _advanced(before, after):
    _require(_validate(before) == _validate(after), "RECOVERY_IDENTITY_CHANGED")
    _require(
        after["checkpoint_batch"] >= before["checkpoint_batch"]
        and after["checkpoint_end_offsets"]["0"]
        >= before["checkpoint_end_offsets"]["0"]
        and after["kafka_end_offsets"]["0"] >= before["kafka_end_offsets"]["0"]
        and before["kafka_begin_offsets"]["0"]
        <= after["kafka_begin_offsets"]["0"]
        <= before["checkpoint_end_offsets"]["0"],
        "RECOVERY_FINAL_PROGRESS_INVALID",
    )
    _require(
        after["checkpoint_end_offsets"] == after["kafka_end_offsets"],
        "RECOVERY_DRAIN_NOT_COMMITTED",
    )


def run_offline(
    baseline,
    expected_baseline_sha256,
    collector,
    backend,
    run_id,
    max_seconds=300,
    clock=time.monotonic,
):
    """Callbacks run only in the synthetic test profile. No live mode exists.

    Clock guards apply between callbacks; each backend/outer supervisor must
    bound blocking operations. Cleanup runs even after the time budget expires.
    """
    _require(
        getattr(backend, "profile", None) == "synthetic-offline"
        and type(collector.source) is LocalMetadataSource,
        "OFFLINE_EXECUTION_ONLY",
    )
    baseline = parse_json(canonical(baseline))
    initial = _checked(baseline)
    _require(
        initial["scope"]["topic"] == "synthetic-pilot-events"
        and initial["scope"]["storage_identity"].startswith("synthetic-"),
        "OFFLINE_EXECUTION_ONLY",
    )
    _require(
        digest(baseline) == expected_baseline_sha256, "RECOVERY_BASELINE_HASH_CHANGED"
    )
    try:
        valid_id = str(UUID(run_id)) == run_id and UUID(run_id).int != 0
    except (ValueError, TypeError, AttributeError):
        valid_id = False
    _require(valid_id, "RECOVERY_RUN_ID_INVALID")
    _require(
        type(max_seconds) is int and 1 <= max_seconds <= 300,
        "RECOVERY_DURATION_INVALID",
    )
    started = clock()

    def guard():
        now = clock()
        _require(
            type(started) in (int, float)
            and type(now) in (int, float)
            and math.isfinite(started)
            and math.isfinite(now)
            and 0 <= now - started < max_seconds,
            "RECOVERY_EXECUTION_EXPIRED",
        )

    guard()
    directory = collector.source.root / ".recovery-operator"
    if not os.path.lexists(directory):
        directory.mkdir(mode=0o700)
    _safe_directory(directory)
    _require(
        directory.stat().st_uid == os.geteuid()
        and not directory.stat().st_mode & 0o077,
        "RECOVERY_DIRECTORY_UNSAFE",
    )
    with _lock(directory):
        try:
            _exclusive(
                directory / (run_id + ".intent.json"),
                {
                    "baseline_sha256": expected_baseline_sha256,
                    "cloud_start_authorized": False,
                },
            )
        except FileExistsError:
            raise RecoveryBlocked("RECOVERY_ATTEMPT_ALREADY_CONSUMED") from None
        observed = collector.collect()
        preview(baseline, observed, expected_baseline_sha256, now=clock())
        reader_attempted = source_attempted = False
        failure = None
        stage = "start_reader"
        try:
            guard()
            reader_attempted = True
            _require(backend.start_reader() is True, "RECOVERY_BACKEND_FAILED")
            guard()
            stage = "start_source"
            source_attempted = True
            _require(backend.start_source() is True, "RECOVERY_BACKEND_FAILED")
            guard()
            stage = "stop_source"
            _require(backend.stop_source() is True, "RECOVERY_BACKEND_FAILED")
            source_attempted = False
            stage = "drain_reader"
            _require(backend.drain_reader() is True, "RECOVERY_DRAIN_NOT_COMMITTED")
            guard()
        except RecoveryBlocked as error:
            failure = (
                str(error)
                if str(error)
                in {
                    "RECOVERY_EXECUTION_EXPIRED",
                    "RECOVERY_BACKEND_FAILED",
                    "RECOVERY_DRAIN_NOT_COMMITTED",
                }
                else "RECOVERY_BACKEND_FAILED"
            )
        except Exception:
            failure = "RECOVERY_BACKEND_FAILED"
        finally:
            for attempted, stop, cleanup_stage in (
                (source_attempted, backend.stop_source, "cleanup_source"),
                (reader_attempted, backend.stop_reader, "cleanup_reader"),
            ):
                if attempted:
                    try:
                        if stop() is not True:
                            failure = "RECOVERY_CLEANUP_FAILED"
                            stage = cleanup_stage
                    except Exception:
                        failure = "RECOVERY_CLEANUP_FAILED"
                        stage = cleanup_stage
        if failure:
            _exclusive(
                directory / (run_id + ".failure.json"),
                {
                    "status": "OFFLINE_RECOVERY_FAILED",
                    "stage": stage,
                    "code": failure,
                    "cloud_start_authorized": False,
                },
            )
            raise RecoveryBlocked(failure)
        guard()
        final = collector.collect()
        after = _checked(final)
        _advanced(observed["snapshot"], after)
        try:
            verified = backend.verify_output(observed["snapshot"], after)
        except Exception:
            verified = False
        _require(verified is True, "RECOVERY_OUTPUT_NOT_VERIFIED")
        guard()
        result = {
            "status": "OFFLINE_RECOVERY_COMPLETED",
            "run_id": run_id,
            "baseline_sha256": expected_baseline_sha256,
            "final_snapshot_sha256": final["snapshot_sha256"],
            "checkpoint_batch": after["checkpoint_batch"],
            "processed_offset_range": {
                "start": initial["checkpoint_end_offsets"]["0"],
                "end": after["checkpoint_end_offsets"]["0"],
            },
            "output_verified": True,
            "cloud_start_authorized": False,
            "live_checks_required": True,
        }
        _exclusive(directory / (run_id + ".result.json"), result)
        return result
