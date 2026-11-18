"""Build a non-secret SSM upload/probe package offline; never execute its commands.

The first live gate is one pinned read and a metadata-only format result. This
package cannot start Docker, RPC, the application or the candidate service.
"""

from __future__ import annotations

import argparse
import ast
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime.bundle import _git  # noqa: E402

FILES = {
    "check_runtime_secret_contract.py": "scripts/check_runtime_secret_contract.py",
    "check_runtime_chain.py": "scripts/check_runtime_chain.py",
    "check_runtime_endpoint_format.py": "scripts/check_runtime_endpoint_format.py",
    "contracts.py": "runtime/contracts.py",
}
INSTANCE = "i-0ca1671a59751acc5"
VERSION = "12d38103-a68c-4226-b20a-3095cbb3de78"
RELEASE = "/opt/blockchain-runtime/release.json"


def require(condition, code):
    if not condition:
        raise ValueError(code)


def constant(source, name):
    for item in ast.parse(source).body:
        if isinstance(item, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in item.targets
        ):
            return ast.literal_eval(item.value)
    raise ValueError("HELPER_CONSTANT_MISSING")


def archive_bytes(payload, manifest):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted({**payload, "probe.manifest.json": manifest}.items()):
            member = tarfile.TarInfo(name)
            member.size, member.mode = len(data), 0o600
            archive.addfile(member, io.BytesIO(data))
    return stream.getvalue()


def bootstrap(archive, digest, destination):
    # Public code/metadata only. The secret is fetched later by the reviewed
    # inspector, stays in memory, and is never part of this upload command.
    encoded = base64.b64encode(archive).decode()
    chunks = "\n".join(
        repr(encoded[offset : offset + 1024]) for offset in range(0, len(encoded), 1024)
    )
    return f"""import base64,hashlib,io,os,subprocess,tarfile,urllib.request
from pathlib import Path
def require(ok):
    if not ok:raise RuntimeError('READER_UPLOAD_BLOCKED')
require(os.geteuid()==0)
require(len(Path('/proc/swaps').read_text().splitlines())==1)
require(subprocess.check_output(['findmnt','-n','-o','FSTYPE','-T','/run'],text=True).strip()=='tmpfs')
opener=urllib.request.build_opener(urllib.request.ProxyHandler({{}}))
metadata='http://169.254.169.254/latest/'
request=urllib.request.Request(metadata+'api/token',method='PUT',headers={{'X-aws-ec2-metadata-token-ttl-seconds':'60'}})
with opener.open(request,timeout=5) as response:token=response.read(2048).decode()
request=urllib.request.Request(metadata+'meta-data/instance-id',headers={{'X-aws-ec2-metadata-token':token}})
with opener.open(request,timeout=5) as response:require(response.read(128).decode()=={INSTANCE!r})
data=base64.b64decode((
{chunks}
),validate=True)
require(hashlib.sha256(data).hexdigest()=={digest!r})
root=Path({destination!r})
root.mkdir(mode=0o700)
with tarfile.open(fileobj=io.BytesIO(data),mode='r:') as archive:
    for member in archive:
        require(member.isfile() and member.name in {sorted([*FILES, "probe.manifest.json"])!r} and member.size<=262144)
        descriptor=os.open(root/member.name,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
        with os.fdopen(descriptor,'wb') as handle:handle.write(archive.extractfile(member).read())
print('READER_PROBE_FILES_STAGED')
"""


def build(repo, output):
    repo, output = Path(repo), Path(output)
    require(
        _git(repo, "branch", "--show-current").decode().strip() == "develop",
        "DEVELOP_REQUIRED",
    )
    require(
        not _git(repo, "status", "--porcelain", "--untracked-files=no").strip(),
        "TRACKED_WORKTREE_CHANGED",
    )
    commit = _git(repo, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
    payload = {
        name: _git(repo, "cat-file", "blob", f"{commit}:{path}")
        for name, path in FILES.items()
    }
    hashes = {name: hashlib.sha256(data).hexdigest() for name, data in payload.items()}
    require(
        constant(payload["check_runtime_chain.py"], "SECRET_CONTRACT_SHA")
        == hashes["contracts.py"],
        "SHARED_CONTRACT_HASH_MISMATCH",
    )
    require(
        constant(payload["check_runtime_secret_contract.py"], "DEPENDENCIES")
        == {
            name: hashes[name]
            for name in ("check_runtime_chain.py", "check_runtime_endpoint_format.py")
        },
        "HELPER_HASH_MISMATCH",
    )
    require(
        constant(payload["check_runtime_secret_contract.py"], "EXPECTED_VERSION")
        == VERSION,
        "SECRET_VERSION_CHANGED",
    )
    manifest = {
        "schema_version": 1,
        "source_commit": commit,
        "instance_id": INSTANCE,
        "secret_version_id": VERSION,
        "secret_arn": constant(
            payload["check_runtime_secret_contract.py"], "EXPECTED_SECRET"
        ),
        "release_sha256": constant(payload["check_runtime_chain.py"], "RELEASE_SHA"),
        "file_sha256": hashes,
        "maximum_secret_reads": 1,
        "secret_writes": 0,
        "container_starts": 0,
        "provider_rpc_calls": 0,
        "owner_approved": False,
        "live_validation_performed": False,
    }
    body = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    archive = archive_bytes(payload, body)
    digest = hashlib.sha256(archive).hexdigest()
    destination = "/run/blockchain-reader-probe-" + digest[:16]
    command = (
        "sudo /usr/bin/python3 - <<'BLOCKCHAIN_READER_PROBE_UPLOAD'\n"
        + bootstrap(archive, digest, destination)
        + "BLOCKCHAIN_READER_PROBE_UPLOAD\n"
    )
    probe = f"sudo /usr/bin/python3 {destination}/check_runtime_secret_contract.py --execute-on-approved-host {RELEASE}\n"
    require(
        output.parent.is_dir() and not output.parent.is_symlink(),
        "OUTPUT_PARENT_REQUIRED",
    )
    output.mkdir(mode=0o700)  # Never replace an artifact or an existing directory.
    for name, data in {
        "probe.tar": archive,
        "proposal.json": body,
        "upload-command.sh": command.encode(),
        "probe-command.sh": probe.encode(),
    }.items():
        descriptor = os.open(
            output / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
    return {
        "source_commit": commit,
        "probe_archive_sha256": digest,
        "guest_directory": destination,
        "files": len(payload),
        "owner_approved": False,
        "cloud_actions": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            build(Path(__file__).resolve().parents[1], args.output), sort_keys=True
        )
    )


if __name__ == "__main__":
    main()
