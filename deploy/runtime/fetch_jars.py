"""Build-time-only, checksum-verified connector installation from Maven Central."""

import hashlib
import json
import sys
import urllib.request
from pathlib import Path

import pyspark


def install(lock):
    target = Path(pyspark.__file__).parent / "jars"
    for artifact in lock:
        path = artifact["path"]
        if ".." in path or not path.endswith(".jar") or path.startswith("/"):
            raise ValueError("Invalid Maven artifact path")
        destination = target / path.rsplit("/", 1)[-1]
        if destination.exists():
            if hashlib.sha512(destination.read_bytes()).hexdigest() != artifact["sha512"]:
                raise ValueError(f"Connector conflicts with bundled Spark JAR: {destination.name}")
            continue  # Spark already ships this exact verified dependency.
        temporary = destination.with_suffix(".download")
        digest = hashlib.sha512()
        with (
            urllib.request.urlopen(
                "https://repo.maven.apache.org/maven2/" + path, timeout=120
            ) as response,
            temporary.open("wb") as output,
        ):
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                digest.update(chunk)
        if digest.hexdigest() != artifact["sha512"]:
            temporary.unlink()
            raise ValueError("Connector checksum mismatch")
        temporary.replace(destination)


if __name__ == "__main__":
    install(json.loads(Path(sys.argv[1]).read_text()))
