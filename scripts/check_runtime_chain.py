"""Reviewed operator preflight; offline import is inert. Execute only after approval."""

import argparse
import hashlib
import json
import os
import re
import resource
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

RELEASE_SHA = "7f948e94cec7f7aebaf2aa4dab4ddd8e92a60f083f17bb9cefcff4db89cd200f"
INSTANCE = "i-0ca1671a59751acc5"
IMAGE = "209479274979.dkr.ecr.us-west-2.amazonaws.com/blockchain-runtime-producer@sha256:63d9d0f24a1cb7eb30b080e58f8a40ddd49292b89020328409e6f45319597afd"
SECRET = "arn:aws:secretsmanager:us-west-2:209479274979:secret:blockchain-runtime/11-6c/alchemy-websocket-url-d44flr"
CLIENT = r"""
import json
import sys
from urllib.parse import urlsplit

def validate_endpoint(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 4096:
        raise ValueError('INVALID_ENDPOINT')
    parsed = urlsplit(value)
    if (any(c.isspace() for c in value) or parsed.scheme != 'wss'
        or parsed.hostname != 'arb-mainnet.g.alchemy.com'
        or parsed.username is not None or parsed.password is not None
        or parsed.port not in (None, 443) or parsed.query or parsed.fragment
        or not parsed.path.startswith('/v2/') or len(parsed.path) <= 4
        or '/' in parsed.path[4:]):
        raise ValueError('INVALID_ENDPOINT')
    return value

def verify_reply(raw):
    if not isinstance(raw, str) or len(raw) > 8192:
        raise ValueError('INVALID_REPLY')
    reply = json.loads(raw)
    if (not isinstance(reply, dict) or reply.get('jsonrpc') != '2.0'
        or type(reply.get('id')) is not int or reply['id'] != 1):
        raise ValueError('INVALID_REPLY')
    if 'error' in reply:
        raise ValueError('RPC_ERROR_RESPONSE')
    if not isinstance(reply.get('result'), str):
        raise ValueError('INVALID_REPLY')
    if reply['result'].lower() != '0xa4b1':
        raise ValueError('WRONG_CHAIN')

def failure_code(error):
    name = type(error).__name__
    if name == 'WebSocketBadStatusException':
        status = getattr(error, 'status_code', None)
        if type(status) is int and status in (400,401,403,404,429,500,502,503,504):
            return 'HTTP_' + str(status)
        return 'HTTP_ERROR'
    if name in ('TimeoutError', 'WebSocketTimeoutException'):
        return 'TIMEOUT'
    if name in ('SSLCertVerificationError', 'SSLError'):
        return 'TLS_ERROR'
    if name in ('ModuleNotFoundError', 'ImportError'):
        return 'IMPORT_ERROR'
    if name in ('gaierror', 'ConnectionRefusedError', 'ConnectionResetError',
                'WebSocketConnectionClosedException', 'OSError'):
        return 'CONNECTION_ERROR'
    if isinstance(error, ValueError):
        code = error.args[0] if error.args else None
        if isinstance(code, str) and code in ('INVALID_ENDPOINT', 'INVALID_REPLY',
                                              'RPC_ERROR_RESPONSE', 'WRONG_CHAIN'):
            return code
        return 'INVALID_INPUT_OR_REPLY'
    return 'CLIENT_ERROR'

def main():
    stage = 'CLIENT_IMPORT'
    try:
        import websocket
        websocket.enableTrace(False)
        stage = 'SECRET_FORMAT'
        value = validate_endpoint(sys.stdin.read(4097))
        stage = 'RPC_CONNECT'
        connection = websocket.create_connection(value, timeout=10)
        try:
            stage = 'RPC_REQUEST'
            connection.send(json.dumps({'jsonrpc':'2.0','id':1,'method':'eth_chainId','params':[]}))
            stage = 'RPC_REPLY'
            verify_reply(connection.recv())
        finally:
            try:
                connection.close()
            except Exception:
                pass  # Process exit closes the socket; preserve any original failure.
        print('ARBITRUM_CHAIN_ID_VERIFIED')
        return 0
    except Exception as error:
        print('CHAIN_PREFLIGHT_FAILED stage=' + stage + ' code=' + failure_code(error))
        return 1

if __name__ == '__main__':
    raise SystemExit(main())
"""

CLIENT_STAGES = {
    "CLIENT_IMPORT",
    "SECRET_FORMAT",
    "RPC_CONNECT",
    "RPC_REQUEST",
    "RPC_REPLY",
}
CLIENT_CODES = {
    "INVALID_ENDPOINT",
    "INVALID_REPLY",
    "RPC_ERROR_RESPONSE",
    "WRONG_CHAIN",
    "HTTP_400",
    "HTTP_401",
    "HTTP_403",
    "HTTP_404",
    "HTTP_429",
    "HTTP_500",
    "HTTP_502",
    "HTTP_503",
    "HTTP_504",
    "HTTP_ERROR",
    "TIMEOUT",
    "TLS_ERROR",
    "IMPORT_ERROR",
    "CONNECTION_ERROR",
    "INVALID_INPUT_OR_REPLY",
    "CLIENT_ERROR",
}
AWS_CODES = {
    "AccessDenied",
    "AccessDeniedException",
    "DecryptionFailure",
    "ResourceNotFoundException",
    "InvalidRequestException",
    "InvalidParameterException",
    "UnrecognizedClientException",
    "ExpiredToken",
    "ExpiredTokenException",
    "ThrottlingException",
    "InternalServiceError",
}
HOST_GUARDS = {
    "UNREVIEWED_RELEASE",
    "RELEASE_SCOPE_MISMATCH",
    "APPROVED_HOST_REQUIRED",
    "SWAP_MUST_BE_DISABLED",
    "HOST_TOOLS_MISSING",
    "REVIEWED_AMD64_IMAGE_REQUIRED",
    "SECRET_FORMAT_INVALID",
    "CHAIN_PREFLIGHT_FAILED",
}
HOST_CODES = {"HOST_ERROR", "HOST_COMMAND_FAILED", "AWS_CLI_FAILED", "CONTAINER_FAILED"}
HOST_CODES |= {
    "CONTAINER_EXIT_" + str(value) for value in (1, 2, 125, 126, 127, 137, 143)
}


class PreflightFailure(RuntimeError):
    """Only allowlisted constants may reach stdout; never use provider messages."""

    def __init__(self, stage, code):
        self.stage = (
            stage
            if isinstance(stage, str)
            and stage
            in CLIENT_STAGES
            | {
                "HOST_PRECHECK",
                "IMAGE_IDENTITY",
                "SECRET_RETRIEVAL",
                "INPUT_TRANSPORT",
                "CONTAINER",
            }
            else "HOST_PRECHECK"
        )
        self.code = (
            code
            if isinstance(code, str)
            and code in CLIENT_CODES | AWS_CODES | HOST_CODES | HOST_GUARDS
            else "HOST_ERROR"
        )
        super().__init__(self.stage + " " + self.code)


def parse_failure(output):
    """Validate a complete diagnostic line before an outer operator forwards it."""
    match = re.fullmatch(
        r"CHAIN_PREFLIGHT_FAILED stage=([A-Z_]+) code=([A-Za-z_0-9]+)\n?", output
    )
    if match:
        safe = PreflightFailure(match[1], match[2])
        if safe.stage == match[1] and safe.code == match[2]:
            return safe
    return None


def client_failure(output):
    safe = parse_failure(output)
    if safe and safe.stage in CLIENT_STAGES and safe.code in CLIENT_CODES:
        return safe
    return None


def clean_env():
    return {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME": "/root",
        "AWS_REGION": "us-west-2",
        "AWS_DEFAULT_REGION": "us-west-2",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_PAGER": "",
        "AWS_EC2_METADATA_DISABLED": "false",
        "NO_PROXY": "169.254.169.254,localhost,127.0.0.1",
        "DOCKER_CONFIG": "/run/blockchain-runtime/docker",
    }


def validate_secret_payload(payload):
    """Validate the retrieved value before Docker, using the exact client rules.

    Do not strip, unwrap nested JSON, repair or include the value in errors.
    Compiling CLIENT under a different module name cannot invoke its main.
    """
    try:
        value = json.loads(payload)
        namespace = {"__name__": "host_endpoint_validation"}
        exec(compile(CLIENT, "reviewed_endpoint_client", "exec"), namespace)
        namespace["validate_endpoint"](value)
    except (ValueError, TypeError):
        raise PreflightFailure("SECRET_FORMAT", "INVALID_ENDPOINT") from None
    return value


def run(args, *, data=None, timeout=30, stage="HOST_PRECHECK"):
    try:
        result = subprocess.run(
            args,
            input=data,
            text=True,
            capture_output=True,
            env=clean_env(),
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise PreflightFailure(stage, "TIMEOUT") from None
    except Exception:
        raise PreflightFailure(stage, "HOST_COMMAND_FAILED") from None
    if result.returncode:
        if stage == "SECRET_RETRIEVAL":
            match = re.search(r"An error occurred \(([A-Za-z]+)\)", result.stderr)
            code = match[1] if match and match[1] in AWS_CODES else "AWS_CLI_FAILED"
            raise PreflightFailure(stage, code)
        if stage == "CONTAINER":
            safe = client_failure(result.stdout)
            if safe:
                raise safe
            code = "CONTAINER_EXIT_" + str(result.returncode)
            raise PreflightFailure(
                stage, code if code in HOST_CODES else "CONTAINER_FAILED"
            )
        raise PreflightFailure(stage, "HOST_COMMAND_FAILED")
    return result.stdout


def instance_id():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    base = "http://169.254.169.254/latest/"
    request = urllib.request.Request(
        base + "api/token",
        method="PUT",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
    )
    with opener.open(request, timeout=5) as response:
        token = response.read(2048).decode()
    request = urllib.request.Request(
        base + "meta-data/instance-id", headers={"X-aws-ec2-metadata-token": token}
    )
    with opener.open(request, timeout=5) as response:
        return response.read(128).decode()


def execute(release_path):
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    body = Path(release_path).read_bytes()
    if hashlib.sha256(body).hexdigest() != RELEASE_SHA:
        raise RuntimeError("UNREVIEWED_RELEASE")
    release = json.loads(body)
    if (
        release.get("instance_id") != INSTANCE
        or release.get("producer_image") != IMAGE
        or release.get("secret_arn") != SECRET
    ):
        raise RuntimeError("RELEASE_SCOPE_MISMATCH")
    if os.geteuid() != 0 or instance_id() != INSTANCE:
        raise RuntimeError("APPROVED_HOST_REQUIRED")
    if len(Path("/proc/swaps").read_text().splitlines()) != 1:
        raise RuntimeError("SWAP_MUST_BE_DISABLED")
    tools = {
        name: shutil.which(name, path=clean_env()["PATH"]) for name in ("aws", "docker")
    }
    if not all(tools.values()):
        raise RuntimeError("HOST_TOOLS_MISSING")
    architecture = run(
        [tools["docker"], "image", "inspect", "--format", "{{.Architecture}}", IMAGE],
        stage="IMAGE_IDENTITY",
    )
    if architecture.strip() != "amd64":
        raise RuntimeError("REVIEWED_AMD64_IMAGE_REQUIRED")
    payload = run(
        [
            tools["aws"],
            "secretsmanager",
            "get-secret-value",
            "--region",
            "us-west-2",
            "--secret-id",
            SECRET,
            "--query",
            "SecretString",
            "--output",
            "json",
        ],
        stage="SECRET_RETRIEVAL",
    )
    value = validate_secret_payload(payload)
    try:
        output = run(
            [
                tools["docker"],
                "run",
                "--rm",
                "--pull=never",
                "--network=bridge",
                "--user=10001:10001",
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges:true",
                "--ulimit",
                "core=0",
                "--pids-limit=64",
                "--memory=256m",
                "--cpus=0.25",
                "--log-driver=none",
                "--tmpfs",
                "/tmp:rw,nosuid,nodev,noexec,size=32m",
                "--interactive",
                "--entrypoint=python",
                IMAGE,
                "-c",
                CLIENT,
            ],
            data=value,
            timeout=45,
            stage="CONTAINER",
        )
    except PreflightFailure as error:
        if error.stage == "SECRET_FORMAT":
            # The exact value passed host validation; rejection now happened
            # after stdin handoff. This does not prove the stored value is bad.
            raise PreflightFailure("INPUT_TRANSPORT", error.code) from None
        raise
    if output.strip() != "ARBITRUM_CHAIN_ID_VERIFIED":
        raise RuntimeError("CHAIN_PREFLIGHT_FAILED")
    print("ARBITRUM_CHAIN_ID_VERIFIED")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute-on-approved-host", action="store_true", required=True
    )
    parser.add_argument("release")
    args = parser.parse_args()
    try:
        execute(args.release)
        return 0
    except Exception as error:
        if isinstance(error, PreflightFailure):
            safe = error
        else:
            guard = (
                error.args[0] if type(error) is RuntimeError and error.args else None
            )
            safe = PreflightFailure(
                "HOST_PRECHECK",
                guard
                if isinstance(guard, str) and guard in HOST_GUARDS
                else "HOST_ERROR",
            )
        print("CHAIN_PREFLIGHT_FAILED stage=" + safe.stage + " code=" + safe.code)
        return 1


if __name__ == "__main__":
    sys.exit(main())
