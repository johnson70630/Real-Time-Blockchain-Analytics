"""Inert on import; version-pinned, metadata-only secret contract diagnostic.

Execution requires a separately approved live scope and the exact existing host.
No Docker, RPC, application start, secret edit, value hash or response file exists.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import resource
import shutil
import subprocess
import sys
from pathlib import Path

EXPECTED_VERSION = "12d38103-a68c-4226-b20a-3095cbb3de78"
EXPECTED_SECRET = "arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-d44flr"
EXPECTED_NAME = "blockchain-runtime/11-6c/alchemy-websocket-url"
RELEASE_PATH = Path("/opt/blockchain-runtime/release.json")
DEPENDENCIES = {
    "check_runtime_chain.py": "6aa8611bc872a5c9d0e5b204715390b5d45a16f06ff79b9690496acb5b4507d0",
    "check_runtime_endpoint_format.py": "2789f10d599c1597fa06d002163591c0ff5d3dcb6dd3858cc01f5ed246c397a8",
}
FORMAT_CODES = {
    "EMPTY_INPUT",
    "INPUT_TOO_LONG",
    "JSON_OR_QUOTES",
    "WHITESPACE_PRESENT",
    "SCHEME_NOT_WSS",
    "HOSTNAME_MISMATCH",
    "USERINFO_PRESENT",
    "PORT_NOT_443",
    "QUERY_OR_FRAGMENT_PRESENT",
    "PATH_MISMATCH",
    "MALFORMED_URL",
    "CLOUD_VALIDATOR_REJECTED",
}
FAILURE_CODES = {
    "DEPENDENCY_CHANGED",
    "APPROVED_HOST_REQUIRED",
    "UNREVIEWED_RELEASE",
    "SWAP_MUST_BE_DISABLED",
    "AWS_CLI_MISSING",
    "AWS_CLI_FAILED",
    "AWS_CLI_VERSION_UNAVAILABLE",
    "TIMEOUT",
    "HOST_ERROR",
}


class DiagnosticFailure(RuntimeError):
    def __init__(self, code):
        self.code = (
            code if isinstance(code, str) and code in FAILURE_CODES else "HOST_ERROR"
        )
        super().__init__(self.code)


def dependencies():
    """Reuse the owner's exact validator without invoking its clipboard action."""
    paths = {name: Path(__file__).with_name(name) for name in DEPENDENCIES}
    for name, path in paths.items():
        if hashlib.sha256(path.read_bytes()).hexdigest() != DEPENDENCIES[name]:
            raise DiagnosticFailure("DEPENDENCY_CHANGED")
    modules = []
    sys.dont_write_bytecode = True
    for name, path in paths.items():
        spec = importlib.util.spec_from_file_location("contract_" + path.stem, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        modules.append(module)
    modules[0].secret_contract()  # Verify the shared reader before any live read.
    return tuple(modules)


def assess_response(payload):
    """Return only fixed fields/codes; never echo untrusted response metadata."""
    result = {
        "status": "CONTRACT_FAIL",
        "expected_version_id": EXPECTED_VERSION,
        "identity_codes": [],
        "format_status": "NOT_CHECKED",
        "format_codes": [],
        "raw_format_status": "NOT_CHECKED",
        "raw_format_codes": [],
        "secret_representation": "NOT_CHECKED",
    }
    if not isinstance(payload, str) or len(payload) > 524288:
        result["identity_codes"] = ["INVALID_RESPONSE_SIZE_OR_TYPE"]
        return result
    try:
        response = json.loads(payload)
    except (ValueError, TypeError):
        result["identity_codes"] = ["RESPONSE_NOT_JSON"]
        return result
    if not isinstance(response, dict):
        result["identity_codes"] = ["RESPONSE_NOT_OBJECT"]
        return result
    checks = {
        "SECRET_ARN_MISMATCH": response.get("ARN") == EXPECTED_SECRET,
        "SECRET_NAME_MISMATCH": response.get("Name") == EXPECTED_NAME,
        "VERSION_ID_MISMATCH": response.get("VersionId") == EXPECTED_VERSION,
        "CURRENT_STAGE_MISMATCH": isinstance(response.get("VersionStages"), list)
        and all(isinstance(x, str) for x in response["VersionStages"])
        and "AWSCURRENT" in response["VersionStages"],
        "SECRET_BINARY_PRESENT": "SecretBinary" not in response,
        "SECRET_STRING_MISSING": "SecretString" in response,
        "SECRET_STRING_NOT_STRING": isinstance(response.get("SecretString"), str),
    }
    result["identity_codes"] = [code for code, passed in checks.items() if not passed]
    result["response_identity_matches"] = all(checks.values())
    if not result["response_identity_matches"]:
        return result
    chain, owner_checker = dependencies()
    raw_report = owner_checker.assess(response["SecretString"])
    codes = raw_report["codes"]
    if not isinstance(codes, list) or any(
        not isinstance(x, str) or x not in FORMAT_CODES for x in codes
    ):
        raise DiagnosticFailure("HOST_ERROR")
    result["raw_format_codes"] = codes
    result["raw_format_status"] = "FORMAT_FAIL" if codes else "FORMAT_PASS"
    try:
        resolved = chain.resolve_secret_value(response["SecretString"])
    except chain.PreflightFailure as error:
        if error.stage != "SECRET_FORMAT":
            raise DiagnosticFailure("DEPENDENCY_CHANGED") from None
        result["format_status"] = "FORMAT_FAIL"
        result["format_codes"] = [error.code]
        return result
    result["secret_representation"] = resolved.representation
    result["format_codes"] = owner_checker.assess(resolved.url)["codes"]
    result["format_status"] = "FORMAT_FAIL" if result["format_codes"] else "FORMAT_PASS"
    result["status"] = "CONTRACT_FAIL" if result["format_codes"] else "CONTRACT_PASS"
    return result


def verify_host(release_path, chain):
    if Path(release_path) != RELEASE_PATH or os.geteuid() != 0:
        raise DiagnosticFailure("APPROVED_HOST_REQUIRED")
    body = RELEASE_PATH.read_bytes()
    if hashlib.sha256(body).hexdigest() != chain.RELEASE_SHA:
        raise DiagnosticFailure("UNREVIEWED_RELEASE")
    release = json.loads(body)
    if (
        release.get("secret_arn") != EXPECTED_SECRET
        or release.get("instance_id") != chain.INSTANCE
    ):
        raise DiagnosticFailure("UNREVIEWED_RELEASE")
    if len(Path("/proc/swaps").read_text().splitlines()) != 1:
        raise DiagnosticFailure("SWAP_MUST_BE_DISABLED")
    if chain.instance_id() != chain.INSTANCE:
        raise DiagnosticFailure("APPROVED_HOST_REQUIRED")


def cli_command(aws):
    return [
        aws,
        "secretsmanager",
        "get-secret-value",
        "--region",
        "us-west-2",
        "--secret-id",
        EXPECTED_SECRET,
        "--version-id",
        EXPECTED_VERSION,
        "--version-stage",
        "AWSCURRENT",
        "--output",
        "json",
        "--cli-connect-timeout",
        "5",
        "--cli-read-timeout",
        "10",
    ]


def execute(release_path):
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    chain, _ = dependencies()
    verify_host(release_path, chain)
    env = chain.clean_env()
    env.update(
        AWS_MAX_ATTEMPTS="1", AWS_RETRY_MODE="standard", AWS_CLI_AUTO_PROMPT="off"
    )
    aws = shutil.which("aws", path=env["PATH"])
    if not aws:
        raise DiagnosticFailure("AWS_CLI_MISSING")
    version = subprocess.run(
        [aws, "--version"],
        capture_output=True,
        text=True,
        env=env,
        timeout=5,
        check=False,
    )
    match = re.match(r"^aws-cli/(\d+\.\d+\.\d+)\b", version.stdout)
    if version.returncode or not match:
        raise DiagnosticFailure("AWS_CLI_VERSION_UNAVAILABLE")
    # Only this call retrieves the value. No query discards identity/version fields.
    response = subprocess.run(
        cli_command(aws),
        capture_output=True,
        text=True,
        env=env,
        timeout=25,
        check=False,
    )
    if response.returncode:
        # Provider text is intentionally discarded, including any embedded value.
        error = re.search(r"An error occurred \(([A-Za-z]+)\)", response.stderr)
        if error and error[1] in chain.AWS_CODES:
            return {
                "status": "DIAGNOSTIC_FAILED",
                "code": "AWS_CLI_FAILED",
                "aws_error_code": error[1],
            }
        raise DiagnosticFailure("AWS_CLI_FAILED")
    report = assess_response(response.stdout)
    report["aws_cli_version"] = match[1]
    report["python_version"] = list(sys.version_info[:3])
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute-on-approved-host", action="store_true", required=True
    )
    parser.add_argument("release")
    args = parser.parse_args()
    try:
        report = execute(args.release)
    except DiagnosticFailure as error:
        report = {"status": "DIAGNOSTIC_FAILED", "code": error.code}
    except subprocess.TimeoutExpired:
        report = {"status": "DIAGNOSTIC_FAILED", "code": "TIMEOUT"}
    except Exception:
        report = {"status": "DIAGNOSTIC_FAILED", "code": "HOST_ERROR"}
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "CONTRACT_PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
