"""Prepare/verify a versioned candidate; live install needs separate approval.

Import, inspection and unit rendering do not run systemctl or contact AWS.
Never overwrites the legacy installation, enables a service, or starts a pilot.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import subprocess
import tarfile
from pathlib import Path

from runtime.bundle import FILES, MANIFEST, MAX_BUNDLE_BYTES, _archive, verify_bundle
from runtime.host import metadata_instance

SOURCE_COMMIT = "ae678174072b2484e2c095593f3363adf822904e"
BUNDLE_SHA = "568d0fc889ca7139aefa651d6d5dccea90ac2bc2106130d8d6451e736b3c9143"
RELEASE_SHA = "7f948e94cec7f7aebaf2aa4dab4ddd8e92a60f083f17bb9cefcff4db89cd200f"
INSTANCE = "i-0ca1671a59751acc5"
BASE = Path("/opt/blockchain-runtime-candidates")
LEGACY = Path("/opt/blockchain-runtime")
UNIT = "blockchain-pilot-reader-candidate.service"
UNIT_DIRECTORY = Path("/run/systemd/system")
RECEIPT = "candidate.receipt.json"


def _require(condition, code):
    if not condition:
        raise ValueError(code)


def _json(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _receipt():
    return {
        "source_commit": SOURCE_COMMIT,
        "bundle_sha256": BUNDLE_SHA,
        "release_sha256": RELEASE_SHA,
    }


def _directory(path):
    _require(not path.is_symlink() and path.is_dir(), "UNSAFE_CANDIDATE_DIRECTORY")
    info = path.stat()
    _require(
        info.st_uid == os.geteuid() and not info.st_mode & 0o022,
        "UNSAFE_CANDIDATE_DIRECTORY",
    )


def _write(path, body, mode):
    descriptor = os.open(
        path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode
    )
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(body)


class _Snapshot:
    def __init__(self, data):
        self.data = data

    def open(self, _mode):
        return io.BytesIO(self.data)


def _inputs(archive, release_path):
    # Use the same bounded bytes for verification and installation. The caller's
    # source paths may change after this read, but unverified bytes cannot enter.
    with Path(archive).open("rb") as handle:
        data = handle.read(MAX_BUNDLE_BYTES + 1)
    result = verify_bundle(_Snapshot(data), BUNDLE_SHA)
    _require(result["git_commit"] == SOURCE_COMMIT, "CANDIDATE_SOURCE_MISMATCH")
    with Path(release_path).open("rb") as handle:
        release = handle.read(32769)
    _require(len(release) <= 32768, "RELEASE_TOO_LARGE")
    _require(hashlib.sha256(release).hexdigest() == RELEASE_SHA, "RELEASE_MISMATCH")
    _require(json.loads(release)["instance_id"] == INSTANCE, "INSTANCE_MISMATCH")
    return result, data, release


def inspect_inputs(archive, release_path):
    return _inputs(archive, release_path)[0]


def verify_install(base=BASE):
    """Verify installed bytes against the external archive digest, not a mutable receipt."""
    base = Path(base)
    _directory(base)
    root = base / SOURCE_COMMIT
    _directory(root)
    expected = set(FILES) | {MANIFEST, "release.json", RECEIPT}
    directories = {
        parent.as_posix()
        for name in expected
        for parent in Path(name).parents
        if parent != Path(".")
    }
    found = set()
    for path in root.rglob("*"):
        _require(not path.is_symlink(), "CANDIDATE_SYMLINK_FORBIDDEN")
        if path.is_dir():
            _require(
                path.relative_to(root).as_posix() in directories,
                "CANDIDATE_EXTRA_DIRECTORY",
            )
            _directory(path)
            continue
        _require(path.is_file(), "CANDIDATE_NOT_REGULAR")
        relative = path.relative_to(root).as_posix()
        _require(relative in expected, "CANDIDATE_EXTRA_FILE")
        info = path.stat()
        _require(
            info.st_uid == os.geteuid() and not info.st_mode & 0o022,
            "CANDIDATE_FILE_PERMISSIONS",
        )
        found.add(relative)
    _require(found == expected, "CANDIDATE_INCOMPLETE")
    payload = {name: (root / name).read_bytes() for name in FILES}
    rebuilt = _archive(payload, (root / MANIFEST).read_bytes())
    _require(
        hashlib.sha256(rebuilt).hexdigest() == BUNDLE_SHA, "INSTALLED_CANDIDATE_CHANGED"
    )
    _require(
        hashlib.sha256((root / "release.json").read_bytes()).hexdigest() == RELEASE_SHA,
        "RELEASE_MISMATCH",
    )
    _require(
        (root / RECEIPT).read_bytes() == _json(_receipt()), "CANDIDATE_RECEIPT_CHANGED"
    )
    return {
        **_receipt(),
        "candidate_root": str(root),
        "installed_files_verified": len(FILES),
    }


def install_candidate(archive, release_path, base=BASE):
    """Exclusive writes only; incomplete or modified installs require operator review.

    The base override supports temporary offline fixtures. The live CLI exposes
    no destination override and checks root/instance before calling this function.
    """
    _result, data, release = _inputs(archive, release_path)
    base = Path(base)
    _directory(base.parent)
    if os.path.lexists(base):
        _directory(base)
    else:
        base.mkdir(mode=0o755)
    root = base / SOURCE_COMMIT
    if os.path.lexists(root):
        return {**verify_install(base), "existing_candidate_reused": True}
    root.mkdir(mode=0o755)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as handle:
        for name in [*FILES, MANIFEST]:
            target = root / name
            target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
            _directory(target.parent)
            _write(target, handle.extractfile(name).read(), 0o644)
    _write(root / "release.json", release, 0o600)
    _write(root / RECEIPT, _json(_receipt()), 0o600)  # Completion marker is last.
    return {**verify_install(base), "existing_candidate_reused": False}


def unit_text():
    root = BASE / SOURCE_COMMIT
    return f"""[Unit]
Description=Reviewed blockchain reader candidate (manual start only)
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target
RequiresMountsFor=/var/lib/blockchain-runtime/kafka

[Service]
Type=simple
WorkingDirectory={root}
Environment=PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 PYTHONPATH={root}
ExecStart=/usr/bin/python3 -m runtime.host run {root}/release.json
KillSignal=SIGTERM
KillMode=process
TimeoutStopSec=420
Restart=no
UMask=0077
LimitCORE=0
ExecStopPost=/usr/bin/systemctl --no-block poweroff
"""


def stage_unit(directory=UNIT_DIRECTORY):
    directory = Path(directory)
    _directory(directory)
    if directory == UNIT_DIRECTORY:
        for folder in (
            "/etc/systemd/system",
            "/run/systemd/system",
            "/usr/lib/systemd/system",
        ):
            _require(
                not os.path.lexists(Path(folder) / (UNIT + ".d")),
                "UNIT_OVERRIDE_FORBIDDEN",
            )
            if folder != "/run/systemd/system":
                _require(
                    not os.path.lexists(Path(folder) / UNIT), "UNIT_SHADOW_FORBIDDEN"
                )
    path = directory / UNIT
    body = unit_text().encode()
    if os.path.lexists(path):
        _require(
            not path.is_symlink()
            and path.is_file()
            and path.stat().st_uid == os.geteuid()
            and not path.stat().st_mode & 0o022
            and path.read_bytes() == body,
            "EXISTING_UNIT_CHANGED",
        )
    else:
        _write(path, body, 0o644)
    return hashlib.sha256(body).hexdigest()


def require_host():
    _require(os.geteuid() == 0, "APPROVED_ROOT_HOST_REQUIRED")
    _require(metadata_instance() == INSTANCE, "APPROVED_ROOT_HOST_REQUIRED")
    _require(len(Path("/proc/swaps").read_text().splitlines()) == 1, "SWAP_FORBIDDEN")
    _require(
        LEGACY.resolve() == LEGACY and LEGACY.is_dir() and not LEGACY.is_symlink(),
        "LEGACY_ROOT_REDIRECTED",
    )
    _directory(LEGACY)


def verify_unit():
    # Includes inherited type/prefix drop-ins that cannot be discovered by
    # looking only for this unit's own .d directory.
    for name, expected in (
        ("FragmentPath", str(UNIT_DIRECTORY / UNIT)),
        ("DropInPaths", ""),
    ):
        result = subprocess.run(
            ["systemctl", "show", UNIT, "--property=" + name, "--value"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        _require(
            result.returncode == 0 and result.stdout.strip() == expected,
            "LOADED_UNIT_MISMATCH",
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["plan", "verify", "install"])
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--release", type=Path)
    parser.add_argument("--execute-on-approved-host", action="store_true")
    args = parser.parse_args()
    try:
        if args.action == "plan":
            inspect_inputs(args.bundle, args.release)
            result = {
                **_receipt(),
                "candidate_root": str(BASE / SOURCE_COMMIT),
                "unit": UNIT,
                "unit_sha256": hashlib.sha256(unit_text().encode()).hexdigest(),
                "cloud_actions": False,
            }
        else:
            _require(args.execute_on_approved_host, "EXPLICIT_HOST_EXECUTION_REQUIRED")
            require_host()
            if args.action == "verify":
                result = verify_install()
            else:
                result = install_candidate(args.bundle, args.release)
                result["unit_sha256"] = stage_unit()
                response = subprocess.run(
                    ["systemctl", "daemon-reload"],
                    capture_output=True,
                    check=False,
                    timeout=30,
                )
                _require(response.returncode == 0, "DAEMON_RELOAD_FAILED")
                verify_unit()
            result["pilot_started"] = False
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        print(
            json.dumps(
                {"status": "CANDIDATE_BLOCKED", "category": type(error).__name__}
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
