"""Build committed, non-secret operator assets and proposed SSM commands offline."""

from __future__ import annotations

import argparse
import ast
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime import bundle, deployment as d, pilot_operator as op  # noqa: E402


def constant(data, name):
    for node in ast.parse(data).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise ValueError("CONSTANT_NOT_FOUND")


def assemble(repo, candidate, legacy, release):
    require = op.require
    require(
        bundle._git(repo, "branch", "--show-current").decode().strip() == "develop",
        "DEVELOP_REQUIRED",
    )
    require(
        not bundle._git(repo, "status", "--porcelain", "--untracked-files=no").strip(),
        "TRACKED_SOURCE_CHANGED",
    )
    commit = bundle._git(repo, "rev-parse", "HEAD").decode().strip()
    _, candidate_bytes, release_bytes = d._inputs(candidate, release)
    with Path(legacy).open("rb") as handle:
        legacy_bytes = handle.read(bundle.MAX_BUNDLE_BYTES + 1)
    bundle.verify_bundle(d._Snapshot(legacy_bytes), op.LEGACY_BUNDLE_SHA)
    payload = {
        "host-candidate.tar": candidate_bytes,
        "legacy-host.tar": legacy_bytes,
        "release.json": release_bytes,
    }
    with tarfile.open(fileobj=io.BytesIO(candidate_bytes), mode="r:") as archive:
        for name in (*bundle.FILES, bundle.MANIFEST):
            payload[name] = archive.extractfile(name).read()
    sources = {
        "runtime/deployment.py": "runtime/deployment.py",
        "runtime/pilot_operator.py": "runtime/pilot_operator.py",
        "runtime/pilot_sample.py": "runtime/pilot_sample.py",
        "scripts/runtime_demo_window_policy.py": "scripts/runtime_demo_window_policy.py",
        "scripts/check_runtime_chain.py": "scripts/check_runtime_chain.py",
        "security-acceptance.json": "docs/evidence/aws_runtime_security_acceptance_2026-11-17.json",
    }
    for destination, source in sources.items():
        payload[destination] = bundle._git(
            repo, "cat-file", "blob", f"{commit}:{source}"
        )
        require(
            len(payload[destination]) <= bundle.MAX_FILE_BYTES,
            "OPERATOR_SOURCE_TOO_LARGE",
        )
    require(set(payload) == set(op.FILES), "OPERATOR_FILE_SET_CHANGED")
    require(
        constant(payload["scripts/check_runtime_chain.py"], "SECRET_CONTRACT_SHA")
        == hashlib.sha256(payload["runtime/contracts.py"]).hexdigest(),
        "SHARED_READER_CHANGED",
    )
    manifest = {
        "schema_version": 1,
        "source_commit": commit,
        "frozen_host_source_commit": d.SOURCE_COMMIT,
        "instance_id": d.INSTANCE,
        "owner_approved": False,
        "file_sha256": {n: hashlib.sha256(b).hexdigest() for n, b in payload.items()},
    }
    payload[op.MANIFEST] = bundle._json(manifest)
    return payload, manifest


def archive_bytes(payload):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted(payload.items()):
            member = tarfile.TarInfo(name)
            member.size, member.mode = len(data), 0o600
            archive.addfile(member, io.BytesIO(data))
    require = op.require
    require(len(stream.getvalue()) <= 8 * 1024 * 1024, "OPERATOR_PACKAGE_TOO_LARGE")
    return stream.getvalue()


def upload_command(data, digest, destination):
    encoded = __import__("base64").b64encode(data).decode()
    chunks = "\n".join(
        repr(encoded[i : i + 1024]) for i in range(0, len(encoded), 1024)
    )
    script = f"""import base64,hashlib,io,os,resource,subprocess,tarfile,urllib.request
from pathlib import Path
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
def require(ok):
 if not ok:raise RuntimeError('OPERATOR_UPLOAD_BLOCKED')
require(os.geteuid()==0 and len(Path('/proc/swaps').read_text().splitlines())==1)
require(subprocess.check_output(['findmnt','-n','-o','FSTYPE','-T','/run'],text=True).strip()=='tmpfs')
opener=urllib.request.build_opener(urllib.request.ProxyHandler({{}}))
base='http://169.254.169.254/latest/'
request=urllib.request.Request(base+'api/token',method='PUT',headers={{'X-aws-ec2-metadata-token-ttl-seconds':'60'}})
with opener.open(request,timeout=5) as response:token=response.read(2048).decode()
request=urllib.request.Request(base+'meta-data/instance-id',headers={{'X-aws-ec2-metadata-token':token}})
with opener.open(request,timeout=5) as response:require(response.read(128).decode()=={d.INSTANCE!r})
data=base64.b64decode((
{chunks}
),validate=True)
require(hashlib.sha256(data).hexdigest()=={digest!r})
root=Path({destination!r});root.mkdir(mode=0o700)
with tarfile.open(fileobj=io.BytesIO(data),mode='r:') as archive:
 members=archive.getmembers()
 require(len(members)=={len(op.FILES) + 1} and {{m.name for m in members}}==set({sorted([*op.FILES, op.MANIFEST])!r}))
 for member in members:
  require(member.isfile() and 0<=member.size<=2097152)
  target=root/member.name;target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
  descriptor=os.open(target,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
  with os.fdopen(descriptor,'wb') as handle:handle.write(archive.extractfile(member).read())
print('PILOT_OPERATOR_FILES_STAGED',flush=True)
"""
    return (
        "sudo -n /usr/bin/python3 -B - <<'BCLAB_PILOT_UPLOAD'\n"
        + script
        + "BCLAB_PILOT_UPLOAD\n"
    )


def build(repo, candidate, legacy, release, output):
    payload, manifest = assemble(repo, candidate, legacy, release)
    data = archive_bytes(payload)
    digest = hashlib.sha256(data).hexdigest()
    destination = "/run/blockchain-pilot-operator-" + digest[:16]
    output = Path(output)
    output.mkdir(mode=0o700)
    records = {
        "operator.tar": data,
        "proposal.json": bundle._json(
            {
                "source_commit": manifest["source_commit"],
                "archive_sha256": digest,
                "guest_directory": destination,
                "owner_approved": False,
                "instance_id": d.INSTANCE,
                "maximum_controller_invocations": 1,
                "maximum_manual_starts": 1,
                "window_minutes": 45,
                "keep_secret_unchanged": True,
                "operator_file_sha256": manifest["file_sha256"],
            }
        ),
        "upload-command.sh": upload_command(data, digest, destination).encode(),
    }
    for action in (
        "controls",
        "install",
        "prepare",
        "launch",
        "status",
        "sample",
        "journal",
        "stop",
    ):
        script = f"cd {destination} && exec /usr/bin/python3 -B -m runtime.pilot_operator {action} --launch-epoch __APPROVED_LAUNCH_EPOCH__ --execute-on-approved-host"
        command = (
            f"sudo -n /usr/bin/env -i PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin HOME=/root PYTHONPATH={destination} PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 /bin/sh -c "
            + shlex.quote(script)
            + "\n"
        )
        records[action + "-command.template"] = command.encode()
    for name, body in records.items():
        descriptor = os.open(
            output / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
    return {
        "source_commit": manifest["source_commit"],
        "archive_sha256": digest,
        "files": len(payload),
        "guest_directory": destination,
        "owner_approved": False,
        "cloud_actions": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate", "legacy", "release", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            build(
                Path(__file__).resolve().parents[1],
                args.candidate,
                args.legacy,
                args.release,
                args.output,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
