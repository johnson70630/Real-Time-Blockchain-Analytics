"""Offline timing policy for a separately approved 45-minute streaming demo.

No cloud clients, subprocesses, credential access or startup action lives here.
The operator must bind this policy to the exact approved instance/current boot.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

WINDOW_SECONDS = 45 * 60
PREFLIGHT_SECONDS = 15 * 60
STOP_DUE_SECONDS = 30 * 60
GUEST_SHUTDOWN_SECONDS = 35 * 60
TARGET_STOPPED_SECONDS = 40 * 60
MINIMUM_SESSION_SECONDS = 90 * 60
TRANSIENT_UNIT = "blockchain-pilot-window-deadline"


def _epoch(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError("INVALID_EPOCH")
    return value


def deadlines(launch_epoch):
    launch = _epoch(launch_epoch)
    return {
        "preflight_due_epoch": launch + PREFLIGHT_SECONDS,
        "normal_stop_due_epoch": launch + STOP_DUE_SECONDS,
        "guest_shutdown_epoch": launch + GUEST_SHUTDOWN_SECONDS,
        "target_stopped_epoch": launch + TARGET_STOPPED_SECONDS,
        "window_end_epoch": launch + WINDOW_SECONDS,
    }


def stop_due(launch_epoch, now_epoch, application_confirmed):
    """A missing app start cannot consume the rest of the paid window."""
    if type(application_confirmed) is not bool:
        raise ValueError("APPLICATION_STATE_MUST_BE_BOOLEAN")
    launch, now = _epoch(launch_epoch), _epoch(now_epoch)
    if now < launch:
        raise ValueError("LAUNCH_TIME_IN_FUTURE")
    elapsed = now - launch
    return elapsed >= STOP_DUE_SECONDS or (
        elapsed >= PREFLIGHT_SECONDS and not application_confirmed
    )


def require_session(expiry_epoch, now_epoch):
    if _epoch(expiry_epoch) - _epoch(now_epoch) < MINIMUM_SESSION_SECONDS:
        raise ValueError("PRIVATE_MFA_REFRESH_REQUIRED")


def guest_deadline_command(launch_epoch, now_epoch):
    """Return a fixed command vector; never execute it or enable anything on boot."""
    launch, now = _epoch(launch_epoch), _epoch(now_epoch)
    if now < launch or now - launch >= PREFLIGHT_SECONDS:
        raise ValueError("PREFLIGHT_WINDOW_CLOSED")
    deadline = deadlines(launch)["guest_shutdown_epoch"]
    calendar = datetime.fromtimestamp(deadline, timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    return [
        "/usr/bin/systemd-run",
        "--unit=" + TRANSIENT_UNIT,
        "--on-calendar=" + calendar,
        "--timer-property=AccuracySec=1s",
        "--timer-property=Persistent=false",
        "/usr/bin/systemctl",
        "--no-block",
        "poweroff",
    ]
