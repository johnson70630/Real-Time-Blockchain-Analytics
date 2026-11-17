"""Reviewed operator preflight; offline import is inert. Execute only after approval."""

import argparse
import hashlib
import json
import os
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
        or type(reply.get('id')) is not int or reply['id'] != 1
        or 'error' in reply or not isinstance(reply.get('result'), str)
        or reply['result'].lower() != '0xa4b1'):
        raise ValueError('WRONG_CHAIN_OR_INVALID_REPLY')

def main():
    try:
        import websocket
        websocket.enableTrace(False)
        value = validate_endpoint(sys.stdin.read(4097))
        connection = websocket.create_connection(value, timeout=10)
        try:
            connection.send(json.dumps({'jsonrpc':'2.0','id':1,'method':'eth_chainId','params':[]}))
            verify_reply(connection.recv())
        finally:
            connection.close()
        print('ARBITRUM_CHAIN_ID_VERIFIED')
        return 0
    except Exception:
        print('CHAIN_PREFLIGHT_FAILED')
        return 1

if __name__ == '__main__':
    raise SystemExit(main())
"""


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


def run(args, *, data=None, timeout=30):
    result = subprocess.run(
        args,
        input=data,
        text=True,
        capture_output=True,
        env=clean_env(),
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError("PREFLIGHT_COMMAND_FAILED")
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
        [tools["docker"], "image", "inspect", "--format", "{{.Architecture}}", IMAGE]
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
        ]
    )
    value = json.loads(payload)
    if not isinstance(value, str) or not 1 <= len(value) <= 4096:
        raise RuntimeError("SECRET_FORMAT_INVALID")
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
    )
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
    except Exception:
        print("CHAIN_PREFLIGHT_FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
