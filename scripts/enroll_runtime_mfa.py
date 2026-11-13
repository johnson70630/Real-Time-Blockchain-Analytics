"""Interactive first-device enrollment; never run by CI or by an agent.

Run manually in the user's Terminal only after the exact temporary IAM grant
is approved and installed. No device deletion, deactivation, resync, or GUI
enrollment is performed. QR seeds and codes must never be sent to chat/logs.
"""

from __future__ import annotations

import getpass
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path

ACCOUNT = "209479274979"
USER = "blockchain-de-local"
PROFILE = "blockchain-de"
DEVICE = "blockchain-de-local-demo"
USER_ARN = f"arn:aws:iam::{ACCOUNT}:user/{USER}"
DEVICE_ARN = f"arn:aws:iam::{ACCOUNT}:mfa/{DEVICE}"
AWS = "/opt/homebrew/bin/aws"
OPERATIONS = {
    ("sts", "get-caller-identity"),
    ("iam", "list-mfa-devices"),
    ("iam", "list-virtual-mfa-devices"),
    ("iam", "create-virtual-mfa-device"),
    ("iam", "enable-mfa-device"),
}
SAFE_ERROR_CODES = {
    "AccessDenied", "AccessDeniedException", "EntityAlreadyExists",
    "InvalidInput", "LimitExceeded", "ConcurrentModification",
    "InvalidAuthenticationCode", "ServiceFailure", "NoSuchEntity",
}


class EnrollmentError(Exception):
    """Contains only controlled, nonsensitive messages."""


def clean_environment() -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if not key.startswith("AWS_")}
    env.update(AWS_EC2_METADATA_DISABLED="true", AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off")
    return env


def require_history_disabled() -> None:
    result = subprocess.run(
        [AWS, "configure", "get", "cli_history", "--profile", PROFILE],
        text=True, capture_output=True, check=False, env=clean_environment(), timeout=15,
    )
    if result.returncode not in (0, 1) or result.stdout.strip() not in ("", "disabled"):
        raise EnrollmentError("AWS CLI command history is enabled or unverified; stop before entering codes.")


def aws_json(service: str, operation: str, *args: str, payload=None):
    if (service, operation) not in OPERATIONS:
        raise EnrollmentError("Operation is outside the enrollment allowlist.")
    command = [
        AWS, service, operation, "--profile", PROFILE, "--region", "us-west-2",
        "--output", "json", "--no-cli-pager", "--no-cli-auto-prompt", *args,
    ]
    if payload is not None:
        command.extend(["--cli-input-json", "file:///dev/stdin"])
    result = subprocess.run(
        command, input=None if payload is None else json.dumps(payload),
        text=True, capture_output=True, check=False, env=clean_environment(), timeout=60,
    )
    if result.returncode:
        match = re.search(r"An error occurred \(([A-Za-z0-9_]+)\)", result.stderr)
        code = match.group(1) if match and match.group(1) in SAFE_ERROR_CODES else "AWS_CLI_FAILED"
        raise EnrollmentError(f"{operation} failed: {code}. No automatic cleanup or retry.")
    try:
        return json.loads(result.stdout) if result.stdout.strip() else None
    except (ValueError, TypeError):
        raise EnrollmentError("AWS returned an unexpected response; output suppressed.") from None


def assigned_devices() -> set[str]:
    result = aws_json("iam", "list-mfa-devices", "--user-name", USER, "--query", "MFADevices[].SerialNumber")
    return checked_serials(result)


def checked_serials(result) -> set[str]:
    if not isinstance(result, list) or not all(isinstance(x, str) for x in result):
        raise EnrollmentError("Unexpected MFA inventory; stop without changes.")
    return set(result)


def virtual_devices(status: str) -> set[str]:
    return checked_serials(aws_json(
        "iam", "list-virtual-mfa-devices", "--assignment-status", status,
        "--query", "VirtualMFADevices[].SerialNumber",
    ))


def read_code(prompt: str) -> str:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            code = getpass.getpass(prompt)
    except getpass.GetPassWarning:
        raise EnrollmentError("Hidden Terminal input is unavailable; stop without entering codes.") from None
    if not re.fullmatch(r"[0-9]{6}", code):
        raise EnrollmentError("The code must contain six digits; its value was not logged.")
    return code


def enroll() -> Path:
    if not all(stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr)):
        raise EnrollmentError("Run manually in your own interactive Terminal, without pipes, tee, or recording.")
    require_history_disabled()
    identity = aws_json("sts", "get-caller-identity")
    if not isinstance(identity, dict) or identity.get("Account") != ACCOUNT or identity.get("Arn") != USER_ARN:
        raise EnrollmentError("The source profile is not the exact approved IAM user; stop.")
    if assigned_devices():
        raise EnrollmentError("The user already has an MFA device; this first-device procedure cannot continue.")
    before_all = virtual_devices("Any")
    before_unassigned = virtual_devices("Unassigned")
    if DEVICE_ARN in before_all:
        raise EnrollmentError("The target device already exists; preserve it and review recovery. Do not rerun creation.")
    print(f"Target user: {USER_ARN}\nNew device: {DEVICE_ARN}")
    print(f"Preserving {len(before_unassigned)} existing unassigned device(s).")
    if input("Type ENROLL to create only this device, then enroll it locally: ") != "ENROLL":
        raise EnrollmentError("Canceled before device creation.")

    directory = Path(tempfile.mkdtemp(prefix="blockchain-mfa-enroll-"))
    directory.chmod(0o700)
    qr = directory / "bootstrap.png"
    fd = os.open(qr, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    print(f"Private enrollment directory: {directory}")
    print("If interrupted, preserve this directory for recovery; never send its contents to chat.")
    serial = aws_json(
        "iam", "create-virtual-mfa-device", "--virtual-mfa-device-name", DEVICE,
        "--path", "/", "--bootstrap-method", "QRCodePNG", "--outfile", str(qr),
        "--query", "VirtualMFADevice.SerialNumber",
    )
    qr.chmod(0o600)
    if serial != DEVICE_ARN or stat.S_IMODE(qr.stat().st_mode) != 0o600 or qr.is_symlink():
        raise EnrollmentError("Device/QR verification failed; preserve the private directory for recovery.")
    with qr.open("rb") as handle:
        if handle.read(8) != b"\x89PNG\r\n\x1a\n":
            raise EnrollmentError("The private QR file is invalid; preserve it for review.")
    print(f"Open {qr} yourself in Preview and scan it into your authenticator app.")
    print("Do not use screen sharing/recording or send the image, seed, or codes to the assistant.")
    if input("After scanning, type SCANNED to continue: ") != "SCANNED":
        raise EnrollmentError("Canceled; new device and private directory preserved for recovery.")
    if assigned_devices():
        raise EnrollmentError("MFA assignment changed during enrollment; preserve all devices and stop.")
    first = read_code("Current six-digit code (hidden): ")
    second = read_code("Wait for the NEXT code, then enter it (hidden): ")
    if first == second:
        raise EnrollmentError("Two successive different codes are required; device and QR preserved.")
    aws_json("iam", "enable-mfa-device", payload={
        "UserName": USER, "SerialNumber": DEVICE_ARN,
        "AuthenticationCode1": first, "AuthenticationCode2": second,
    })
    del first, second
    if assigned_devices() != {DEVICE_ARN}:
        raise EnrollmentError("Assignment verification was inconclusive; no automatic retry or cleanup.")
    after_all = virtual_devices("Any")
    after_unassigned = virtual_devices("Unassigned")
    if not before_all.issubset(after_all) or not before_unassigned.issubset(after_unassigned):
        raise EnrollmentError("Existing MFA inventory changed; stop for review without reverting anything.")

    receipt = directory / "receipt.json"
    fd = os.open(receipt, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump({
            "account": ACCOUNT, "user_arn": USER_ARN, "device_arn": DEVICE_ARN,
            "assignment_verified": True,
            "existing_unassigned_devices_preserved": sorted(before_unassigned),
        }, handle, indent=2)
        handle.write("\n")
    # Only the newly generated local secret file is removed; no cloud deletion.
    qr.unlink()
    print(f"MFA assignment verified. Private QR file removed. Nonsecret receipt: {receipt}")
    print("Close Preview without saving/exporting. No role assumption or deployment was performed.")
    return receipt


def main() -> int:
    try:
        enroll()
    except EnrollmentError as error:
        print(f"STOP: {error}", file=sys.stderr)
        return 1
    except (KeyboardInterrupt, EOFError):
        print("STOP: interrupted; no automatic cloud cleanup. Preserve any private enrollment directory.", file=sys.stderr)
        return 1
    except (OSError, subprocess.SubprocessError):
        print("STOP: local/CLI failure; details suppressed to protect secrets. Preserve any private enrollment directory.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
