"""Inspect a flattened Docker export without extracting files or running code."""

import json
from pathlib import PurePosixPath
import sys
import tarfile

MAX_MEMBERS = 1_000_000
MAX_BYTES = 20 * 1024**3


def inspect_export(stream):
    names, privileged, files, size = set(), [], 0, 0
    try:
        with tarfile.open(fileobj=stream, mode="r|") as archive:
            for member in archive:
                name = PurePosixPath(member.name)
                if (
                    name.is_absolute()
                    or ".." in name.parts
                    or str(name) in names
                    or len(names) >= MAX_MEMBERS
                    or member.size < 0
                ):
                    raise ValueError("CONTAINER_FILESYSTEM_ARCHIVE_INVALID")
                names.add(str(name))
                size += member.size
                if size > MAX_BYTES:
                    raise ValueError("CONTAINER_FILESYSTEM_ARCHIVE_TOO_LARGE")
                if member.isfile() or member.islnk():
                    files += 1
                    if member.mode & 0o6000:
                        privileged.append(str(name))
    except (tarfile.TarError, EOFError) as error:
        raise ValueError("CONTAINER_FILESYSTEM_ARCHIVE_INVALID") from error
    if not files:
        raise ValueError("CONTAINER_FILESYSTEM_ARCHIVE_EMPTY")
    if privileged:
        raise ValueError("CONTAINER_SETID_BITS_PRESENT")
    return {"regular_files_and_links": files, "setid_files": 0}


if __name__ == "__main__":
    try:
        result = inspect_export(sys.stdin.buffer)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from None
    print("CONTAINER_SETID_BITS_ABSENT " + json.dumps(result, sort_keys=True))
