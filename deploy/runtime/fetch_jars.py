"""Build-time-only, checksum-verified connector installation from Maven Central."""

import argparse
import hashlib
import http.client
import json
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

MAX_CACHE_BYTES = 1024**3
ATTEMPTS = 3


def verified(path, expected):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_CACHE_BYTES:
        raise ValueError("Invalid connector file")
    digest = hashlib.sha512()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise ValueError("Connector checksum mismatch")


def write_verified(source, destination, expected):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent, suffix=".download", delete=False
        ) as output:
            temporary = Path(output.name)
            size = 0
            while chunk := source.read(1024 * 1024):
                size += len(chunk)
                if size > MAX_CACHE_BYTES:
                    raise ValueError("Connector exceeds size limit")
                output.write(chunk)
        verified(temporary, expected)
        temporary.chmod(0o644)
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def download(path, destination, expected):
    """Retry briefly without changing hosts/pins or promoting partial content."""
    for attempt in range(ATTEMPTS):
        try:
            with urllib.request.urlopen(
                "https://repo.maven.apache.org/maven2/" + path, timeout=120
            ) as response:
                write_verified(response, destination, expected)
            return
        except (
            urllib.error.URLError, TimeoutError, ConnectionError,
            http.client.IncompleteRead,
        ) as error:
            delay = 2 ** (attempt + 1)
            if isinstance(error, urllib.error.HTTPError):
                if error.code not in {429, 500, 502, 503, 504}:
                    raise
                retry_after = error.headers.get("Retry-After") if error.headers else None
                if retry_after is not None:
                    # Long or nonnumeric cooldowns stop rather than retrying early.
                    if not retry_after.isdigit() or int(retry_after) > 30:
                        raise
                    delay = max(delay, int(retry_after))
                error.close()
            if attempt == ATTEMPTS - 1:
                raise
            time.sleep(delay)


def install(lock, *, target=None, cache=None, offline=False):
    if target is None:
        import pyspark

        target = Path(pyspark.__file__).parent / "jars"
    if target.is_symlink():
        raise ValueError("Invalid connector target")
    target.mkdir(parents=True, exist_ok=True)
    if cache is not None and (cache.is_symlink() or not cache.is_dir()):
        raise ValueError("Invalid connector cache")
    names = set()
    total = 0
    for artifact in lock:
        path = artifact["path"]
        if ".." in path or not path.endswith(".jar") or path.startswith("/"):
            raise ValueError("Invalid Maven artifact path")
        destination = target / path.rsplit("/", 1)[-1]
        if destination.name in names or destination.is_symlink():
            raise ValueError("Duplicate or symlink connector")
        names.add(destination.name)
        if destination.exists():
            verified(destination, artifact["sha512"])
        elif cache is not None and (cache / destination.name).exists():
            cached = cache / destination.name
            verified(cached, artifact["sha512"])
            # The cache is public dependency data, never executable build input
            # until its pinned checksum has been independently checked.
            with cached.open("rb") as source:
                write_verified(source, destination, artifact["sha512"])
        elif offline:
            raise ValueError("Verified connector missing from offline cache")
        else:
            download(path, destination, artifact["sha512"])
        total += destination.stat().st_size
        if total > MAX_CACHE_BYTES:
            raise ValueError("Connector cache exceeds size limit")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lock", type=Path)
    parser.add_argument("--cache-only", type=Path)
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    install(
        json.loads(args.lock.read_text()), target=args.cache_only,
        cache=args.cache, offline=args.offline,
    )
