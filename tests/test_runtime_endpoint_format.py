import contextlib
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

spec = importlib.util.spec_from_file_location(
    "owner_format",
    Path(__file__).resolve().parents[1] / "scripts/check_runtime_endpoint_format.py",
)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
FAKE = "wss://arb-mainnet.g.alchemy.com/v2/fake"


@pytest.mark.parametrize(
    ("value", "code"),
    [
        ("", "EMPTY_INPUT"),
        ("x" * 4097, "INPUT_TOO_LONG"),
        (json.dumps({"endpoint": FAKE}), "JSON_OR_QUOTES"),
        (json.dumps(FAKE), "JSON_OR_QUOTES"),
        (FAKE + "\n", "WHITESPACE_PRESENT"),
        (" " + FAKE, "WHITESPACE_PRESENT"),
        (FAKE.replace("wss:", "https:"), "SCHEME_NOT_WSS"),
        (FAKE.replace("arb-mainnet", "eth-mainnet"), "HOSTNAME_MISMATCH"),
        ("wss:", "HOSTNAME_MISMATCH"),
        (FAKE.replace("wss://", "wss://user@"), "USERINFO_PRESENT"),
        (FAKE.replace(".com/", ".com:80/"), "PORT_NOT_443"),
        (FAKE + "?private-placeholder", "QUERY_OR_FRAGMENT_PRESENT"),
        (FAKE + "/extra", "PATH_MISMATCH"),
        (FAKE.replace("://", "://["), "MALFORMED_URL"),
    ],
)
def test_fixed_codes_without_input_disclosure(value, code):
    report = checker.assess(value)
    assert report["status"] == "FORMAT_FAIL"
    assert code in report["codes"]
    assert "private-placeholder" not in json.dumps(report)
    assert "alchemy.com" not in json.dumps(report)


def test_valid_endpoint_matches_cloud_validator():
    assert checker.assess(FAKE) == {"status": "FORMAT_PASS", "codes": []}


def test_clipboard_reader_runs_only_after_owner_confirmation_and_never_echoes_value():
    calls = []
    clipboard = Mock(
        side_effect=lambda *_args, **_kwargs: (
            calls.append("read") or SimpleNamespace(returncode=0, stdout=FAKE)
        )
    )
    output = io.StringIO()
    with (
        patch.object(checker.sys, "argv", ["check", "--clipboard"]),
        patch.object(checker.sys, "platform", "darwin"),
        patch.object(checker.sys.stdin, "isatty", return_value=True),
        patch("builtins.input", side_effect=lambda *_: calls.append("confirm")),
        patch.object(checker.subprocess, "run", clipboard),
        contextlib.redirect_stdout(output),
    ):
        assert checker.main() == 0
    assert calls == ["confirm", "read"]
    assert json.loads(output.getvalue()) == {"status": "FORMAT_PASS", "codes": []}
    assert FAKE not in output.getvalue()


def test_clipboard_failure_suppresses_exception_details():
    output = io.StringIO()
    with (
        patch.object(checker.sys, "argv", ["check", "--clipboard"]),
        patch.object(checker.sys, "platform", "darwin"),
        patch.object(checker.sys.stdin, "isatty", return_value=True),
        patch("builtins.input", return_value=""),
        patch.object(
            checker.subprocess, "run", side_effect=RuntimeError("private-placeholder")
        ),
        contextlib.redirect_stdout(output),
    ):
        assert checker.main() == 2
    assert output.getvalue() == "FORMAT_CHECK_UNAVAILABLE\n"


def test_noninteractive_process_cannot_read_clipboard():
    with (
        patch.object(checker.sys, "argv", ["check", "--clipboard"]),
        patch.object(checker.sys, "platform", "darwin"),
        patch.object(checker.sys.stdin, "isatty", return_value=False),
        patch.object(checker.subprocess, "run") as clipboard,
        contextlib.redirect_stdout(io.StringIO()),
    ):
        assert checker.main() == 2
    clipboard.assert_not_called()
