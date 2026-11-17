import contextlib
import importlib.util
import io
import json
import ssl
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "preflight",
    str(Path(__file__).resolve().parents[1] / "scripts/check_runtime_chain.py"),
)
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
client = {"__name__": "offline_test"}
exec(compile(p.CLIENT, "chain-client", "exec"), client)


class Checks(unittest.TestCase):
    def test_client_roundtrip_keeps_endpoint_out_of_output(self):
        endpoint = "wss://arb-mainnet.g.alchemy.com/v2/fake"
        connection = Mock()
        connection.recv.return_value = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "result": "0xa4b1"}
        )
        websocket = SimpleNamespace(
            enableTrace=Mock(), create_connection=Mock(return_value=connection)
        )
        output = io.StringIO()
        with (
            patch.dict("sys.modules", {"websocket": websocket}),
            patch("sys.stdin", io.StringIO(endpoint)),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(client["main"](), 0)
        websocket.create_connection.assert_called_once_with(endpoint, timeout=10)
        self.assertEqual(
            json.loads(connection.send.call_args.args[0]),
            {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
        )
        connection.close.assert_called_once()
        self.assertEqual(output.getvalue(), "ARBITRUM_CHAIN_ID_VERIFIED\n")

    def test_client_timeout_never_prints_exception_or_endpoint(self):
        endpoint = "wss://arb-mainnet.g.alchemy.com/v2/fake"
        websocket = SimpleNamespace(
            enableTrace=Mock(),
            create_connection=Mock(side_effect=TimeoutError(endpoint)),
        )
        output = io.StringIO()
        with (
            patch.dict("sys.modules", {"websocket": websocket}),
            patch("sys.stdin", io.StringIO(endpoint)),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(client["main"](), 1)
        self.assertEqual(
            output.getvalue(), "CHAIN_PREFLIGHT_FAILED stage=RPC_CONNECT code=TIMEOUT\n"
        )

    def test_host_secret_uses_stdin_and_only_reviewed_image(self):
        endpoint = "wss://arb-mainnet.g.alchemy.com/v2/fake"
        calls = []

        def fake_run(args, **kwargs):
            calls.append((args, kwargs))
            return ["amd64\n", json.dumps(endpoint), "ARBITRUM_CHAIN_ID_VERIFIED\n"][
                len(calls) - 1
            ]

        output = io.StringIO()
        with (
            patch.object(p.os, "geteuid", return_value=0),
            patch.object(p, "instance_id", return_value=p.INSTANCE),
            patch.object(p.resource, "setrlimit"),
            patch.object(
                p.Path, "read_text", return_value="Filename Type Size Used Priority\n"
            ),
            patch.object(
                p.shutil, "which", side_effect=lambda name, **_: "/usr/bin/" + name
            ),
            patch.object(p, "run", side_effect=fake_run),
            contextlib.redirect_stdout(output),
        ):
            p.execute("docs/evidence/aws_runtime_release_candidate_2026-11-17.json")
        self.assertEqual(len(calls), 3)
        self.assertIn(p.SECRET, calls[1][0])
        args, kwargs = calls[2]
        self.assertIn(p.IMAGE, args)
        for flag in [
            "--pull=never",
            "--read-only",
            "--log-driver=none",
            "--user=10001:10001",
            "--cap-drop=ALL",
        ]:
            self.assertIn(flag, args)
        self.assertNotIn(endpoint, args)
        self.assertEqual(kwargs["data"], endpoint)
        self.assertEqual(kwargs["timeout"], 45)
        self.assertEqual(output.getvalue(), "ARBITRUM_CHAIN_ID_VERIFIED\n")

    def test_wrong_image_architecture_prevents_secret_read(self):
        with (
            patch.object(p.os, "geteuid", return_value=0),
            patch.object(p, "instance_id", return_value=p.INSTANCE),
            patch.object(p.resource, "setrlimit"),
            patch.object(
                p.Path, "read_text", return_value="Filename Type Size Used Priority\n"
            ),
            patch.object(p.shutil, "which", return_value="/usr/bin/tool"),
            patch.object(p, "run", return_value="arm64") as run,
        ):
            with self.assertRaisesRegex(RuntimeError, "REVIEWED_AMD64_IMAGE_REQUIRED"):
                p.execute("docs/evidence/aws_runtime_release_candidate_2026-11-17.json")
        self.assertEqual(run.call_count, 1)

    def test_correct_endpoint(self):
        for value in [
            "wss://arb-mainnet.g.alchemy.com/v2/fake",
            "wss://arb-mainnet.g.alchemy.com:443/v2/fake",
        ]:
            self.assertEqual(client["validate_endpoint"](value), value)

    def test_wrong_endpoints(self):
        for value in [
            "wss://eth-mainnet.g.alchemy.com/v2/fake",
            "wss://arb-sepolia.g.alchemy.com/v2/fake",
            "ws://arb-mainnet.g.alchemy.com/v2/fake",
            "wss://arb-mainnet.g.alchemy.com.evil/v2/fake",
            "wss://user@arb-mainnet.g.alchemy.com/v2/fake",
            "wss://arb-mainnet.g.alchemy.com/v2/",
            "wss://arb-mainnet.g.alchemy.com/v2/fake?x=y",
            "wss://arb-mainnet.g.alchemy.com/v2/fake#fragment",
            "wss://arb-mainnet.g.alchemy.com:80/v2/fake",
            "wss://arb-mainnet.g.alchemy.com/v2/fake\n",
            "wss://arb-mainnet.g.alchemy.com/v2/fake/extra",
            None,
            "x" * 4097,
        ]:
            with self.subTest(case=value):
                with self.assertRaises(ValueError):
                    client["validate_endpoint"](value)

    def test_correct_chain(self):
        client["verify_reply"](
            json.dumps({"jsonrpc": "2.0", "id": 1, "result": "0xa4b1"})
        )

    def test_wrong_replies(self):
        for value in [
            {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
            {"jsonrpc": "2.0", "id": 1, "result": "0x66eee"},
            {"jsonrpc": "2.0", "id": 2, "result": "0xa4b1"},
            {"jsonrpc": "2.0", "id": True, "result": "0xa4b1"},
            {"jsonrpc": "2.0", "id": 1, "result": 42161},
            {"jsonrpc": "2.0", "id": 1, "result": "0xa4b1", "error": {}},
            {"jsonrpc": "1.0", "id": 1, "result": "0xa4b1"},
            [],
            {},
        ]:
            with self.subTest(case=value):
                with self.assertRaises(ValueError):
                    client["verify_reply"](json.dumps(value))
        with self.assertRaises(ValueError):
            client["verify_reply"]("not json")

    def test_no_credentials_in_clean_environment(self):
        with patch.dict(
            p.os.environ,
            {
                "AWS_ACCESS_KEY_ID": "fake",
                "AWS_SECRET_ACCESS_KEY": "fake",
                "AWS_PROFILE": "fake",
                "HTTP_PROXY": "fake",
            },
        ):
            env = p.clean_env()
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_PROFILE",
            "HTTP_PROXY",
        ]:
            self.assertNotIn(name, env)

    def test_changed_release_before_any_cloud_call(self):
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile() as f:
            f.write(b"{}")
            f.flush()
            with patch.object(
                p, "instance_id", side_effect=AssertionError("forbidden")
            ):
                with self.assertRaisesRegex(RuntimeError, "UNREVIEWED_RELEASE"):
                    p.execute(f.name)

    def test_wrong_host_before_aws(self):
        with (
            patch.object(p.os, "geteuid", return_value=0),
            patch.object(p, "instance_id", return_value="i-wrong"),
            patch.object(p, "run", side_effect=AssertionError("forbidden")),
        ):
            with self.assertRaisesRegex(RuntimeError, "APPROVED_HOST_REQUIRED"):
                p.execute("docs/evidence/aws_runtime_release_candidate_2026-11-17.json")

    def test_failure_output_is_fixed(self):
        with (
            patch.object(
                p.argparse.ArgumentParser,
                "parse_args",
                return_value=type("Args", (), {"release": "unused"})(),
            ),
            patch.object(
                p, "execute", side_effect=RuntimeError("sensitive-placeholder")
            ),
        ):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(p.main(), 1)
            self.assertEqual(
                output.getvalue(),
                "CHAIN_PREFLIGHT_FAILED stage=HOST_PRECHECK code=HOST_ERROR\n",
            )

    def test_aws_access_denial_emits_only_allowlisted_code(self):
        result = SimpleNamespace(
            returncode=254,
            stdout="private-placeholder",
            stderr="An error occurred (AccessDeniedException) when calling GetSecretValue: private-placeholder",
        )
        with patch.object(p.subprocess, "run", return_value=result):
            with self.assertRaises(p.PreflightFailure) as raised:
                p.run(["aws"], stage="SECRET_RETRIEVAL")
        self.assertEqual(
            str(raised.exception), "SECRET_RETRIEVAL AccessDeniedException"
        )

    def test_unknown_aws_code_and_provider_text_are_hidden(self):
        result = SimpleNamespace(
            returncode=254,
            stdout="private-placeholder",
            stderr="An error occurred (privatePlaceholder) private-placeholder",
        )
        with patch.object(p.subprocess, "run", return_value=result):
            with self.assertRaises(p.PreflightFailure) as raised:
                p.run(["aws"], stage="SECRET_RETRIEVAL")
        self.assertEqual(str(raised.exception), "SECRET_RETRIEVAL AWS_CLI_FAILED")

    def test_known_client_diagnostic_survives_outer_command_failure(self):
        result = SimpleNamespace(
            returncode=1,
            stdout="CHAIN_PREFLIGHT_FAILED stage=RPC_CONNECT code=HTTP_401\n",
            stderr="private-placeholder",
        )
        with patch.object(p.subprocess, "run", return_value=result):
            with self.assertRaises(p.PreflightFailure) as raised:
                p.run(["docker"], stage="CONTAINER")
        self.assertEqual(str(raised.exception), "RPC_CONNECT HTTP_401")

    def test_outer_operator_can_parse_safe_host_diagnostic(self):
        line = (
            "CHAIN_PREFLIGHT_FAILED stage=SECRET_RETRIEVAL code=AccessDeniedException\n"
        )
        parsed = p.parse_failure(line)
        self.assertEqual(str(parsed), "SECRET_RETRIEVAL AccessDeniedException")
        self.assertIsNone(p.client_failure(line))

    def test_forged_or_extra_diagnostic_output_never_passes_through(self):
        for output in [
            "CHAIN_PREFLIGHT_FAILED stage=privatePlaceholder code=HTTP_401\n",
            "CHAIN_PREFLIGHT_FAILED stage=RPC_CONNECT code=privatePlaceholder\n",
            "CHAIN_PREFLIGHT_FAILED stage=RPC_CONNECT code=HTTP_401\nprivate-placeholder",
            "private-placeholder",
        ]:
            with self.subTest(output=output):
                result = SimpleNamespace(
                    returncode=1, stdout=output, stderr="private-placeholder"
                )
                with patch.object(p.subprocess, "run", return_value=result):
                    with self.assertRaises(p.PreflightFailure) as raised:
                        p.run(["docker"], stage="CONTAINER")
                self.assertEqual(str(raised.exception), "CONTAINER CONTAINER_EXIT_1")

    def test_cli_timeout_hides_partial_output_and_exception_details(self):
        error = subprocess.TimeoutExpired(
            ["docker"], 45, output="private-placeholder", stderr="private-placeholder"
        )
        with patch.object(p.subprocess, "run", side_effect=error):
            with self.assertRaises(p.PreflightFailure) as raised:
                p.run(["docker"], stage="CONTAINER")
        self.assertEqual(str(raised.exception), "CONTAINER TIMEOUT")
        self.assertTrue(raised.exception.__suppress_context__)

    def test_docker_cli_failure_emits_only_exit_status(self):
        result = SimpleNamespace(
            returncode=125, stdout="private-placeholder", stderr="private-placeholder"
        )
        with patch.object(p.subprocess, "run", return_value=result):
            with self.assertRaises(p.PreflightFailure) as raised:
                p.run(["docker"], stage="CONTAINER")
        self.assertEqual(str(raised.exception), "CONTAINER CONTAINER_EXIT_125")

    def test_client_http_denial_hides_endpoint_and_provider_body(self):
        endpoint = "wss://arb-mainnet.g.alchemy.com/v2/fake"
        error_type = type(
            "WebSocketBadStatusException", (Exception,), {"status_code": 401}
        )
        websocket = SimpleNamespace(
            enableTrace=Mock(), create_connection=Mock(side_effect=error_type(endpoint))
        )
        output = io.StringIO()
        with (
            patch.dict("sys.modules", {"websocket": websocket}),
            patch("sys.stdin", io.StringIO(endpoint)),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(client["main"](), 1)
        self.assertEqual(
            output.getvalue(),
            "CHAIN_PREFLIGHT_FAILED stage=RPC_CONNECT code=HTTP_401\n",
        )

    def test_client_format_failure_prevents_any_connection(self):
        websocket = SimpleNamespace(enableTrace=Mock(), create_connection=Mock())
        output = io.StringIO()
        with (
            patch.dict("sys.modules", {"websocket": websocket}),
            patch(
                "sys.stdin", io.StringIO("wss://arb-mainnet.g.alchemy.com/v2/fake\n")
            ),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(client["main"](), 1)
        websocket.create_connection.assert_not_called()
        self.assertEqual(
            output.getvalue(),
            "CHAIN_PREFLIGHT_FAILED stage=SECRET_FORMAT code=INVALID_ENDPOINT\n",
        )

    def test_tls_error_keeps_certificate_details_private(self):
        websocket = SimpleNamespace(
            enableTrace=Mock(),
            create_connection=Mock(
                side_effect=ssl.SSLCertVerificationError("private-placeholder")
            ),
        )
        output = io.StringIO()
        with (
            patch.dict("sys.modules", {"websocket": websocket}),
            patch("sys.stdin", io.StringIO("wss://arb-mainnet.g.alchemy.com/v2/fake")),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(client["main"](), 1)
        self.assertEqual(
            output.getvalue(),
            "CHAIN_PREFLIGHT_FAILED stage=RPC_CONNECT code=TLS_ERROR\n",
        )

    def test_rpc_error_response_does_not_expose_provider_message(self):
        connection = Mock()
        connection.recv.return_value = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": -32000, "message": "private-placeholder"},
            }
        )
        websocket = SimpleNamespace(
            enableTrace=Mock(), create_connection=Mock(return_value=connection)
        )
        output = io.StringIO()
        with (
            patch.dict("sys.modules", {"websocket": websocket}),
            patch("sys.stdin", io.StringIO("wss://arb-mainnet.g.alchemy.com/v2/fake")),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(client["main"](), 1)
        self.assertEqual(
            output.getvalue(),
            "CHAIN_PREFLIGHT_FAILED stage=RPC_REPLY code=RPC_ERROR_RESPONSE\n",
        )

    def test_wrong_chain_is_distinct_from_connection_failure(self):
        connection = Mock()
        connection.recv.return_value = json.dumps(
            {"jsonrpc": "2.0", "id": 1, "result": "0x1"}
        )
        websocket = SimpleNamespace(
            enableTrace=Mock(), create_connection=Mock(return_value=connection)
        )
        output = io.StringIO()
        with (
            patch.dict("sys.modules", {"websocket": websocket}),
            patch("sys.stdin", io.StringIO("wss://arb-mainnet.g.alchemy.com/v2/fake")),
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(client["main"](), 1)
        self.assertEqual(
            output.getvalue(),
            "CHAIN_PREFLIGHT_FAILED stage=RPC_REPLY code=WRONG_CHAIN\n",
        )

    def test_diagnostic_constructor_rejects_unknown_or_nonstring_values(self):
        for stage, code in [("private-placeholder", "private-placeholder"), ({}, [])]:
            with self.subTest(stage=stage):
                self.assertEqual(
                    str(p.PreflightFailure(stage, code)), "HOST_PRECHECK HOST_ERROR"
                )


if __name__ == "__main__":
    unittest.main()
