import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "window_policy",
    Path(__file__).resolve().parents[1] / "scripts/runtime_demo_window_policy.py",
)
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)
LAUNCH = 1794957878


def test_all_deadlines_remain_tied_to_original_launch():
    assert policy.deadlines(LAUNCH) == {
        "preflight_due_epoch": LAUNCH + 900,
        "normal_stop_due_epoch": LAUNCH + 1800,
        "guest_shutdown_epoch": LAUNCH + 2100,
        "target_stopped_epoch": LAUNCH + 2400,
        "window_end_epoch": LAUNCH + 2700,
    }


def test_missing_application_aborts_at_fifteen_minutes():
    assert not policy.stop_due(LAUNCH, LAUNCH + 899, False)
    assert policy.stop_due(LAUNCH, LAUNCH + 900, False)


def test_active_application_has_normal_stop_deadline():
    assert not policy.stop_due(LAUNCH, LAUNCH + 1799, True)
    assert policy.stop_due(LAUNCH, LAUNCH + 1800, True)
    assert policy.stop_due(LAUNCH, LAUNCH + 2700, True)


def test_rechecking_guest_timer_does_not_extend_deadline():
    first = policy.guest_deadline_command(LAUNCH, LAUNCH + 60)
    later = policy.guest_deadline_command(LAUNCH, LAUNCH + 800)
    assert first == later
    assert "--on-calendar=2026-11-17 23:59:38 UTC" in first
    assert "--timer-property=Persistent=false" in first
    assert first[-3:] == ["/usr/bin/systemctl", "--no-block", "poweroff"]


def test_late_guest_timer_install_is_refused():
    with pytest.raises(ValueError, match="PREFLIGHT_WINDOW_CLOSED"):
        policy.guest_deadline_command(LAUNCH, LAUNCH + 900)


def test_session_margin_includes_supervision_and_shutdown():
    policy.require_session(LAUNCH + 5400, LAUNCH)
    with pytest.raises(ValueError, match="PRIVATE_MFA_REFRESH_REQUIRED"):
        policy.require_session(LAUNCH + 5399, LAUNCH)


@pytest.mark.parametrize(
    "value", [True, None, "1794957878", 0, -1, float("nan"), float("inf")]
)
def test_invalid_timestamps_cannot_create_deadlines(value):
    with pytest.raises(ValueError, match="INVALID_EPOCH"):
        policy.deadlines(value)


def test_future_launch_cannot_reset_elapsed_time():
    with pytest.raises(ValueError, match="LAUNCH_TIME_IN_FUTURE"):
        policy.stop_due(LAUNCH, LAUNCH - 1, False)


def test_truthy_application_string_is_not_confirmation():
    with pytest.raises(ValueError, match="APPLICATION_STATE_MUST_BE_BOOLEAN"):
        policy.stop_due(LAUNCH, LAUNCH + 900, "true")
