"""Owner-run Mac clipboard check; reports format codes, never the endpoint.

Run with --clipboard, then copy the secret's Plaintext value privately and press
Enter. No AWS call, network request, file write or clipboard modification occurs.
The assistant must not execute the clipboard-reading action.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import resource
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit


def assess(value: str) -> dict:
    """Return only constant codes; preserve whitespace for accurate diagnosis."""
    if not isinstance(value, str) or not value:
        return {"status": "FORMAT_FAIL", "codes": ["EMPTY_INPUT"]}
    if len(value) > 4096:
        return {"status": "FORMAT_FAIL", "codes": ["INPUT_TOO_LONG"]}
    codes = []
    if value.lstrip().startswith(("{", "[", '"', "'")):
        codes.append("JSON_OR_QUOTES")
    if any(character.isspace() for character in value):
        codes.append("WHITESPACE_PRESENT")
    try:
        parsed = urlsplit(value)
        if parsed.scheme != "wss":
            codes.append("SCHEME_NOT_WSS")
        if parsed.hostname != "arb-mainnet.g.alchemy.com":
            codes.append("HOSTNAME_MISMATCH")
        if parsed.username is not None or parsed.password is not None:
            codes.append("USERINFO_PRESENT")
        if parsed.port not in (None, 443):
            codes.append("PORT_NOT_443")
        if parsed.query or parsed.fragment:
            codes.append("QUERY_OR_FRAGMENT_PRESENT")
        if (
            not parsed.path.startswith("/v2/")
            or len(parsed.path) <= 4
            or "/" in parsed.path[4:]
        ):
            codes.append("PATH_MISMATCH")
    except ValueError:
        codes.append("MALFORMED_URL")

    # Use the actual cloud validator too, so a local PASS cannot bypass its rules.
    source = Path(__file__).with_name("check_runtime_chain.py")
    spec = importlib.util.spec_from_file_location("reviewed_chain_check", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    namespace = {"__name__": "owner_format_check"}
    exec(compile(module.CLIENT, "reviewed_client", "exec"), namespace)
    try:
        namespace["validate_endpoint"](value)
    except ValueError:
        if not codes:
            codes.append("CLOUD_VALIDATOR_REJECTED")
    return {"status": "FORMAT_FAIL" if codes else "FORMAT_PASS", "codes": codes}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clipboard", action="store_true", required=True)
    parser.parse_args()
    if sys.platform != "darwin" or not sys.stdin.isatty():
        print("FORMAT_CHECK_UNAVAILABLE")
        return 2
    try:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        input(
            "Now privately copy the secret's Plaintext value in AWS Console. "
            "Return here and press Enter; do not paste it into the terminal: "
        )
        result = subprocess.run(
            ["/usr/bin/pbpaste"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode:
            print("FORMAT_CHECK_UNAVAILABLE")
            return 2
        report = assess(result.stdout)
        result = None
        print(json.dumps(report, sort_keys=True))
        return 0 if report["status"] == "FORMAT_PASS" else 1
    except (Exception, KeyboardInterrupt):
        # Never display clipboard contents, provider text or exception messages.
        print("FORMAT_CHECK_UNAVAILABLE")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
