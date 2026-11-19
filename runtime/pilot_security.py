"""Offline consumption gate for the historical, single-demonstration exception."""

import hashlib
import json
import os
from pathlib import Path
import stat

ACCEPTANCE = "docs/evidence/aws_runtime_security_acceptance_2026-11-17.json"
CONSUMPTION = "docs/evidence/aws_runtime_security_consumption_2026-11-19.json"
MAX_BYTES = 65536


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("DUPLICATE_KEY")
        result[key] = value
    return result


def _read(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError("REGULAR_FILE_REQUIRED")
        data = handle.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("RECORD_TOO_LARGE")
    return data


def require_unused_acceptance(repo):
    """Fail closed before packaging or AWS access; missing history is not approval.

    New artifacts or a new risk decision require an explicitly reviewed successor
    ledger and gate. Never erase the consumed record to obtain another run.
    """
    try:
        root = Path(repo)
        accepted_bytes = _read(root / ACCEPTANCE)
        accepted = json.loads(accepted_bytes, object_pairs_hook=_unique_object)
        ledger = json.loads(_read(root / CONSUMPTION), object_pairs_hook=_unique_object)
        count = ledger["demonstration_windows_consumed"]
        valid = (
            type(ledger["schema_version"]) is int
            and ledger["schema_version"] == 1
            and ledger["acceptance_record_path"] == ACCEPTANCE
            and ledger["acceptance_record_sha256"]
            == hashlib.sha256(accepted_bytes).hexdigest()
            and ledger["reviewed_scope_sha256"] == accepted["reviewed_scope_sha256"]
            and type(ledger["maximum_demonstration_windows"]) is int
            and ledger["maximum_demonstration_windows"] == 1
            and type(count) is int
            and count in (0, 1)
            and type(ledger["streaming_runs_performed"]) is int
            and ledger["streaming_runs_performed"] == count
            and type(ledger["controller_attempts"]) is int
            and ledger["controller_attempts"] == count
            and ledger["validation_container_executed"] is bool(count)
            and ledger["new_start_authorized"] is False
            and ledger["status"]
            == (
                "SINGLE_DEMONSTRATION_EXCEPTION_CONSUMED"
                if count
                else "SINGLE_DEMONSTRATION_EXCEPTION_UNUSED"
            )
        )
        if not valid:
            raise ValueError("INVALID_RECORD")
    except (OSError, ValueError, TypeError, KeyError, RecursionError):
        raise RuntimeError("SECURITY_CONSUMPTION_RECORD_INVALID") from None
    if count:
        raise RuntimeError("SECURITY_EXCEPTION_ALREADY_CONSUMED")
