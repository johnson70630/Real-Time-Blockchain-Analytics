"""Readiness/drain acceptance, stale scope refusal and no terminal leakage."""

import importlib.util
import json
import os
import pty
import re
import termios
import threading
import time
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "pilot_relay",
    Path(__file__).resolve().parents[1] / "scripts/runtime_pilot_relay.py",
)
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def status():
    return {
        "unit": {"ActiveState": "active"},
        "stream": {
            "heartbeat_age_seconds": 1,
            "drained": False,
            "verifiedCheckpointBatchId": 0,
        },
        "containers": [
            {
                "service": name,
                "running": True,
                "healthy": name != "producer",
                "oom_killed": False,
                "exit_code": 0,
            }
            for name in ("kafka", "spark", "producer")
        ],
    }


def test_requested_start_is_not_confirmed_until_journal_and_health_match():
    value = status()
    assert not r.controller_confirmed(value, {})
    assert r.controller_confirmed(
        value, {"pilot_started_marker": True, "blocked_marker": False}
    )
    value["stream"]["heartbeat_age_seconds"] = 31
    assert not r.controller_confirmed(
        value, {"pilot_started_marker": True, "blocked_marker": False}
    )


def test_failed_or_missing_container_never_confirms_start():
    value = status()
    value["containers"][0]["oom_killed"] = True
    assert not r.controller_confirmed(
        value, {"pilot_started_marker": True, "blocked_marker": False}
    )
    value["containers"].pop()
    assert not r.controller_confirmed(
        value, {"pilot_started_marker": True, "blocked_marker": False}
    )


def test_drain_needs_checkpoint_marker_exit_codes_and_no_oom():
    value = status()
    journal = {"graceful_stop_verified_marker": True}
    assert not r.drain_confirmed(value, journal)
    value["stream"]["drained"] = True
    for item in value["containers"]:
        item["running"] = False
        item["exit_code"] = 143 if item["service"] == "kafka" else 0
    assert r.drain_confirmed(value, journal)
    value["containers"][1]["exit_code"] = 137
    assert not r.drain_confirmed(value, journal)


def test_format_only_or_expired_approval_never_contacts_sdk(tmp_path, monkeypatch):
    (tmp_path / "authorization.json").write_text(
        json.dumps(
            {
                "demo_approved": True,
                "max_controller_invocations": 0,
                "active_authorization": False,
            }
        )
    )
    monkeypatch.setattr(
        r.w,
        "clients",
        lambda *a: pytest.fail("SDK contacted without streaming approval"),
    )
    with pytest.raises(RuntimeError, match="STREAMING_SCOPE_APPROVAL_REQUIRED"):
        r.verify(tmp_path, tmp_path)


def test_empty_or_stale_reports_cannot_confirm_drain():
    assert not r.drain_confirmed({}, {})
    value = status()
    value["stream"]["verifiedCheckpointBatchId"] = None
    assert not r.drain_confirmed(value, {"graceful_stop_verified_marker": True})


@pytest.mark.parametrize("ending", [b"\n", b"\r\n", b"\r\r\n"])
@pytest.mark.parametrize("ok", [True, False])
def test_real_pty_translation_preserves_success_and_failure_replies(ending, ok):
    master, slave = pty.openpty()
    try:
        attrs = termios.tcgetattr(slave)
        attrs[1] |= termios.OPOST | termios.ONLCR
        termios.tcsetattr(slave, termios.TCSANOW, attrs)
        body = json.dumps({"ok": ok, "controls_verified": ok}).encode()
        os.write(slave, body + ending)
        assert r.read_json_reply(master, 2, 4096) == {"ok": ok, "controls_verified": ok}
    finally:
        os.close(master)
        os.close(slave)


def test_original_regex_rejects_real_nested_pty_crcrlf_but_new_parser_accepts():
    assert not re.search(rb'(\{"[^\r\n]+\})\r?\n', b'{"ok":true}\r\r\n')
    assert re.search(r.JSON_REPLY_PATTERN, b'{"ok":true}\r\r\n')


def test_reply_split_before_final_line_ending_is_received_without_leaking_noise(capsys):
    master, slave = pty.openpty()

    def writer():
        os.write(slave, b'private-dummy-terminal-noise\n{"ok":')
        time.sleep(0.02)
        os.write(slave, b"true}\r\n")

    thread = threading.Thread(target=writer)
    try:
        thread.start()
        assert r.read_json_reply(master, 2, 4096) == {"ok": True}
        assert capsys.readouterr().out == ""
    finally:
        thread.join()
        os.close(master)
        os.close(slave)


@pytest.mark.parametrize(
    "body",
    [
        b'{"ok":"true"}\r\n',
        b'{"ok":1}\r\n',
        b'{"missing":false}\r\n',
        b'{"ok":true,"bad":}\r\n',
    ],
)
def test_invalid_json_or_boolean_contract_fails_closed(body, capsys):
    master, slave = pty.openpty()
    try:
        os.write(slave, body)
        with pytest.raises(r.TerminalFailure) as error:
            r.read_json_reply(master, 2, 4096)
        assert error.value.code == "INVALID_JSON_REPLY"
        assert capsys.readouterr().out == ""
    finally:
        os.close(master)
        os.close(slave)


def test_terminal_output_limit_never_echoes_bytes(capsys):
    master, slave = pty.openpty()
    try:
        os.write(slave, b"private-dummy-data-without-reply")
        with pytest.raises(r.TerminalFailure) as error:
            r.read_json_reply(master, 2, 8)
        assert error.value.code == "TERMINAL_OUTPUT_LIMIT"
        assert capsys.readouterr().out == ""
    finally:
        os.close(master)
        os.close(slave)
