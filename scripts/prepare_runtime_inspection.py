"""Build a committed-source inspection-only package; never uploads or executes it."""

import argparse
import base64
import hashlib
import io
import os
from pathlib import Path
import shlex
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime import bundle, inspection as ins  # noqa: E402
from runtime.recovery_metadata import canonical  # noqa: E402
from runtime.recovery import _require  # noqa: E402


def build(repo, output):
    repo, output = Path(repo), Path(output)
    _require(
        bundle._git(repo, "branch", "--show-current").decode().strip() == "develop",
        "DEVELOP_REQUIRED",
    )
    _require(
        not bundle._git(repo, "status", "--porcelain", "--untracked-files=no").strip(),
        "TRACKED_SOURCE_CHANGED",
    )
    commit = bundle._git(repo, "rev-parse", "HEAD").decode().strip()
    payload = {}
    for name in ins.FILES:
        entry = bundle._git(repo, "ls-tree", "-z", commit, "--", name)
        _require(
            entry.startswith((b"100644 blob ", b"100755 blob ")),
            "INSPECTION_SOURCE_NOT_REGULAR",
        )
        value = bundle._git(repo, "cat-file", "blob", commit + ":" + name)
        _require(len(value) <= 262144, "INSPECTION_SOURCE_TOO_LARGE")
        payload[name] = value
    manifest = {
        "schema_version": 1,
        "source_commit": commit,
        "owner_approved": False,
        "instance_id": ins.INSTANCE,
        "purpose": "retained-metadata-inspection",
        "file_sha256": {
            name: hashlib.sha256(value).hexdigest() for name, value in payload.items()
        },
    }
    payload[ins.MANIFEST] = canonical(manifest)
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name, value in sorted(payload.items()):
            member = tarfile.TarInfo(name)
            member.size = len(value)
            member.mode = 0o600
            tar.addfile(member, io.BytesIO(value))
    archive = stream.getvalue()
    _require(len(archive) <= ins.MAX_ARCHIVE, "INSPECTION_PACKAGE_TOO_LARGE")
    sha = hashlib.sha256(archive).hexdigest()
    manifest_sha = hashlib.sha256(payload[ins.MANIFEST]).hexdigest()
    destination = "/run/blockchain-retained-inspection-" + sha[:16]
    encoded = base64.b64encode(archive).decode()
    chunks = "\n".join(
        repr(encoded[i : i + 1024]) for i in range(0, len(encoded), 1024)
    )
    bootstrap = f"""import base64,hashlib,io,json,os,resource,tarfile,time,urllib.request
from pathlib import Path
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
def require(ok):
 if not ok:raise RuntimeError('INSPECTION_UPLOAD_BLOCKED')
approval=base64.b64decode('__INSPECTION_APPROVAL_BASE64__',validate=True)
require(len(approval)<=16384)
a=json.loads(approval)
require(a.get('owner_approved') is True and a.get('active_authorization') is True and a.get('purpose')=='retained-metadata-inspection')
require(a.get('archive_sha256')=={sha!r} and a.get('manifest_sha256')=={manifest_sha!r} and a.get('source_commit')=={commit!r})
require(a.get('instance_id')=={ins.INSTANCE!r} and a.get('container_starts')==0 and a.get('secret_reads')==0 and a.get('production_data_writes')==0)
require(os.geteuid()==0 and len(Path('/proc/swaps').read_text().splitlines())==1)
require(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==a['boot_id'])
require(a['launch_epoch']<=time.time()<a['setup_due_epoch'])
import subprocess
require(subprocess.check_output(['/usr/bin/findmnt','-n','-o','FSTYPE','-T','/run'],text=True).strip()=='tmpfs')
opener=urllib.request.build_opener(urllib.request.ProxyHandler({{}}))
base='http://169.254.169.254/latest/'
request=urllib.request.Request(base+'api/token',method='PUT',headers={{'X-aws-ec2-metadata-token-ttl-seconds':'60'}})
with opener.open(request,timeout=3) as response:token=response.read(2048).decode()
request=urllib.request.Request(base+'meta-data/instance-id',headers={{'X-aws-ec2-metadata-token':token}})
with opener.open(request,timeout=3) as response:require(response.read(128).decode()=={ins.INSTANCE!r})
data=base64.b64decode((
{chunks}
),validate=True)
require(hashlib.sha256(data).hexdigest()=={sha!r})
root=Path({destination!r});root.mkdir(mode=0o700)
def write(path,value):
 fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
 with os.fdopen(fd,'wb') as handle:handle.write(value)
write(root/'inspection.tar',data)
with tarfile.open(fileobj=io.BytesIO(data),mode='r:') as tar:
 members=tar.getmembers()
 require(len(members)=={len(payload)} and {{m.name for m in members}}==set({sorted(payload)!r}))
 for member in members:
  require(member.isfile() and member.size<=262144)
  path=root/member.name;path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
  write(path,tar.extractfile(member).read())
write(root/'authorization.json',approval)
print('INSPECTION_FILES_STAGED',flush=True)
"""
    proposal = {
        "schema_version": 1,
        "source_commit": commit,
        "purpose": "retained-metadata-inspection",
        "instance_id": ins.INSTANCE,
        "archive_sha256": sha,
        "manifest_sha256": manifest_sha,
        "guest_directory": destination,
        "file_sha256": manifest["file_sha256"],
        "owner_approved": False,
        "active_authorization": False,
        "cloud_calls_made": False,
        "maximum_inspections": 1,
        "container_starts": 0,
        "secret_reads": 0,
        "production_data_writes": 0,
        "broker_watermarks_measured": False,
        "baseline_approved": False,
        "recovery_authorized": False,
        "external_stop_minutes": 10,
        "guest_poweroff_minutes": 12,
        "outer_supervision_minutes": 15,
    }
    records = {
        "inspection.tar": archive,
        "proposal.json": canonical(proposal) + b"\n",
        "upload-command.template": (
            "sudo -n /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin HOME=/root /usr/bin/python3 -I -B - <<'BLOCKCHAIN_INSPECTION_UPLOAD'\n"
            + bootstrap
            + "BLOCKCHAIN_INSPECTION_UPLOAD\n"
        ).encode(),
    }
    for action in ("controls", "inspect"):
        script = f"cd {destination} && exec /usr/bin/python3 -B -s -m runtime.inspection {action} --execute-on-approved-host"
        records[action + "-command.template"] = (
            "sudo -n /usr/bin/env -i PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin HOME=/root PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="
            + destination
            + " /bin/sh -c "
            + shlex.quote(script)
            + "\n"
        ).encode()
    output.mkdir(mode=0o700)
    for name, value in records.items():
        fd = os.open(
            output / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600
        )
        with os.fdopen(fd, "wb") as handle:
            handle.write(value)
    return {
        "source_commit": commit,
        "archive_sha256": sha,
        "manifest_sha256": manifest_sha,
        "files": len(payload),
        "owner_approved": False,
        "cloud_calls_made": False,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(canonical(build(Path(__file__).resolve().parents[1], args.output)).decode())
