"""Interactive first-device enrollment; never run by CI or by an agent.

Run manually in the user's Terminal only after the exact temporary IAM grant
is approved and installed. No device deletion, deactivation, resync, or GUI
enrollment is performed. QR seeds and codes must never be sent to chat/logs.
"""

from __future__ import annotations

import getpass
import argparse
import json
import logging
import os
import re
import stat
import subprocess
import sys
import tempfile
import warnings
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    boto3 = None

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
    "ValidationError", "ParamValidation",
}


class EnrollmentError(Exception):
    """Contains only controlled, nonsensitive messages."""


@dataclass
class EnrollmentProgress:
    phase: str = "preflight"
    attachment_started: bool = False
    attachment_returned: bool = False
    verified: bool = False


def clean_environment() -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if not key.startswith("AWS_")}
    env.update(
        AWS_EC2_METADATA_DISABLED="true", AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off",
        AWS_MAX_ATTEMPTS="1", AWS_RETRY_MODE="standard",
    )
    return env


def require_history_disabled() -> None:
    result = subprocess.run(
        [AWS, "configure", "get", "cli_history", "--profile", PROFILE],
        text=True, capture_output=True, check=False, env=clean_environment(), timeout=15,
    )
    if result.returncode not in (0, 1) or result.stdout.strip() not in ("", "disabled"):
        raise EnrollmentError("AWS CLI command history is enabled or unverified; stop before entering codes.")


@contextmanager
def private_sdk_environment():
    original = {key: value for key, value in os.environ.items() if key.startswith("AWS_")}
    loggers = [logging.getLogger(name) for name in ("boto3", "botocore", "urllib3")]
    levels = [logger.level for logger in loggers]
    try:
        for key in original:
            os.environ.pop(key, None)
        os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
        for logger in loggers:
            logger.setLevel(logging.WARNING)
        yield
    finally:
        for key in list(os.environ):
            if key.startswith("AWS_"):
                os.environ.pop(key, None)
        os.environ.update(original)
        for logger, level in zip(loggers, levels):
            logger.setLevel(level)


def enable_with_sdk(payload) -> None:
    if boto3 is None:
        raise EnrollmentError("Use this project's .venv/bin/python; the enrollment SDK is required.")
    if (
        set(payload) != {"UserName", "SerialNumber", "AuthenticationCode1", "AuthenticationCode2"}
        or payload["UserName"] != USER
        or payload["SerialNumber"] != DEVICE_ARN
        or not all(re.fullmatch(r"[0-9]{6}", str(payload[key])) for key in ("AuthenticationCode1", "AuthenticationCode2"))
        or payload["AuthenticationCode1"] == payload["AuthenticationCode2"]
    ):
        raise EnrollmentError("Enrollment request does not match the exact approved target/code format.")
    with private_sdk_environment():
        try:
            session = boto3.Session(profile_name=PROFILE, region_name="us-west-2")
            with closing(session.client("iam", config=Config(
                retries={"total_max_attempts": 1, "mode": "standard"},
                connect_timeout=10, read_timeout=30,
            ))) as client:
                client.enable_mfa_device(**payload)
        except ClientError as error:
            candidate = error.response.get("Error", {}).get("Code")
            code = candidate if candidate in SAFE_ERROR_CODES else "AWS_REQUEST_FAILED"
            raise EnrollmentError(f"enable-mfa-device failed: {code}. No automatic retry.") from None
        except BotoCoreError:
            raise EnrollmentError("Enrollment SDK failed; sensitive details suppressed. No automatic retry.") from None


def aws_json(service: str, operation: str, *args: str, payload=None):
    if (service, operation) not in OPERATIONS:
        raise EnrollmentError("Operation is outside the enrollment allowlist.")
    if payload is not None:
        if (service, operation) != ("iam", "enable-mfa-device") or args:
            raise EnrollmentError("Only the exact MFA attachment operation accepts a secret payload.")
        # Keep codes in SDK memory. The CLI file:///dev/stdin path fails with
        # the installed CLI parser and must not be used for secret transport.
        enable_with_sdk(payload)
        return None
    if (service, operation) == ("iam", "enable-mfa-device"):
        raise EnrollmentError("MFA codes must use the private SDK payload, never CLI arguments.")
    command = [
        AWS, service, operation, "--profile", PROFILE, "--region", "us-west-2",
        "--output", "json", "--no-cli-pager", "--no-cli-auto-prompt", *args,
    ]
    result = subprocess.run(
        command,
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


def enroll(resume: bool = False, progress: EnrollmentProgress | None = None) -> Path:
    progress = progress or EnrollmentProgress()
    if not all(stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr)):
        raise EnrollmentError("Run manually in your own interactive Terminal, without pipes, tee, or recording.")
    require_history_disabled()
    if boto3 is None:
        raise EnrollmentError("Use this project's .venv/bin/python; the enrollment SDK is required.")
    identity = aws_json("sts", "get-caller-identity")
    if not isinstance(identity, dict) or identity.get("Account") != ACCOUNT or identity.get("Arn") != USER_ARN:
        raise EnrollmentError("The source profile is not the exact approved IAM user; stop.")
    if assigned_devices():
        raise EnrollmentError("The user already has an MFA device; this first-device procedure cannot continue.")
    before_all = virtual_devices("Any")
    before_unassigned = virtual_devices("Unassigned")
    if DEVICE_ARN in before_all and not resume:
        raise EnrollmentError("The target device already exists; preserve it and review recovery. Do not rerun creation.")
    if resume and DEVICE_ARN not in before_unassigned:
        raise EnrollmentError("The exact recovery target is not unassigned; stop without changes.")
    preserved_unassigned = before_unassigned - {DEVICE_ARN}
    print(f"Target user: {USER_ARN}\nNew device: {DEVICE_ARN}")
    print(f"Preserving {len(preserved_unassigned)} other existing unassigned device(s).")
    if resume:
        print("Resuming the existing device. Use its ORIGINAL scanned authenticator entry.")
        if input("Type RESUME to attach only this existing device: ") != "RESUME":
            raise EnrollmentError("Canceled before attachment; all devices preserved.")
    elif input("Type ENROLL to create only this device, then enroll it locally: ") != "ENROLL":
        raise EnrollmentError("Canceled before device creation.")

    progress.phase = "local-preparation"
    directory = Path(tempfile.mkdtemp(prefix="blockchain-mfa-enroll-"))
    directory.chmod(0o700)
    qr = None
    print(f"Private enrollment directory: {directory}")
    print("If interrupted, preserve this directory for recovery; never send its contents to chat.")
    if not resume:
        progress.phase = "device-creation"
        qr = directory / "bootstrap.png"
        fd = os.open(qr, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)
        serial = aws_json(
            "iam", "create-virtual-mfa-device", "--virtual-mfa-device-name", DEVICE,
            "--path", "/", "--bootstrap-method", "QRCodePNG", "--outfile", str(qr),
            "--query", "VirtualMFADevice.SerialNumber",
        )
        if qr.is_symlink():
            raise EnrollmentError("The QR path changed; stop without following it.")
        qr.chmod(0o600)
        if serial != DEVICE_ARN or stat.S_IMODE(qr.stat().st_mode) != 0o600:
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
    progress.phase = "code-entry"
    first = read_code("Current six-digit code (hidden): ")
    second = read_code("Wait for the NEXT code, then enter it (hidden): ")
    if first == second:
        raise EnrollmentError("Two successive different codes are required; device and QR preserved.")
    progress.phase = "attachment"
    progress.attachment_started = True
    aws_json("iam", "enable-mfa-device", payload={
        "UserName": USER, "SerialNumber": DEVICE_ARN,
        "AuthenticationCode1": first, "AuthenticationCode2": second,
    })
    progress.attachment_returned = True
    del first, second
    progress.phase = "confirmation"
    if assigned_devices() != {DEVICE_ARN}:
        raise EnrollmentError("Assignment verification was inconclusive; no automatic retry or cleanup.")
    after_all = virtual_devices("Any")
    after_unassigned = virtual_devices("Unassigned")
    if not before_all.issubset(after_all) or not preserved_unassigned.issubset(after_unassigned):
        raise EnrollmentError("Existing MFA inventory changed; stop for review without reverting anything.")

    progress.verified = True
    progress.phase = "receipt"
    receipt = directory / "receipt.json"
    fd = os.open(receipt, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump({
            "account": ACCOUNT, "user_arn": USER_ARN, "device_arn": DEVICE_ARN,
            "assignment_verified": True,
            "existing_unassigned_devices_preserved": sorted(preserved_unassigned),
            "resumed_existing_device": resume,
        }, handle, indent=2)
        handle.write("\n")
    # Only the newly generated local secret file is removed; no cloud deletion.
    if qr is not None:
        progress.phase = "local-qr-cleanup"
        qr.unlink()
    print(f"MFA assignment verified. Nonsecret receipt: {receipt}")
    print("New QR removed if generated this run; any earlier recovery QR remains untouched.")
    print("Close Preview without saving/exporting. No role assumption or deployment was performed.")
    progress.phase = "complete"
    return receipt


def report_interruption(progress: EnrollmentProgress, category: str, detail: str | None = None) -> None:
    if progress.verified:
        message = "MFA ENROLLMENT VERIFIED; local receipt/cleanup did not finish. Do not repeat enrollment."
    elif progress.attachment_returned:
        message = "AWS attachment request returned successfully; local confirmation is incomplete. Do not repeat enrollment."
    elif progress.attachment_started:
        message = "MFA attachment outcome is unconfirmed and it may already be enabled. Do not repeat enrollment."
    else:
        message = "Enrollment stopped before attachment. Preserve any created device and private directory."
    print(message, file=sys.stderr)
    print(f"Stage: {progress.phase}; category: {category}. Sensitive error details suppressed.", file=sys.stderr)
    if detail:
        print(detail, file=sys.stderr)
    print("Inspect IAM assignment before further action. No automatic retry or cloud cleanup.", file=sys.stderr)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resume", action="store_true", help="Attach only the existing named unassigned device; never create another.")
    options = parser.parse_args(argv)
    progress = EnrollmentProgress()
    try:
        enroll(resume=options.resume, progress=progress)
    except EnrollmentError as error:
        report_interruption(progress, "EnrollmentError", str(error))
        return 1
    except (KeyboardInterrupt, EOFError) as error:
        report_interruption(progress, type(error).__name__)
        return 1
    except (OSError, subprocess.SubprocessError) as error:
        report_interruption(progress, type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
