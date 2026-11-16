"""Verify cloud image requirements against their separate, frozen uv lock.

Exporting is offline and installs nothing. Lock freshness is checked separately
by CI; --frozen alone cannot detect an out-of-date pyproject/lock pair.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import tempfile


PROJECT = Path(__file__).resolve().parents[1] / "deploy/runtime"


def check_exports(project: Path = PROJECT) -> bool:
    matched = True
    with tempfile.TemporaryDirectory(
        prefix="blockchain-requirements-check-"
    ) as directory:
        for role in ("producer", "spark"):
            output = Path(directory) / f"{role}.txt"
            subprocess.run(
                [
                    "uv",
                    "export",
                    "--project",
                    str(project),
                    "--no-config",
                    "--cache-dir",
                    str(Path(directory) / "cache"),
                    "--only-group",
                    role,
                    "--frozen",
                    "--offline",
                    "--no-python-downloads",
                    "--no-header",
                    "--no-annotate",
                    "--no-emit-project",
                    "--output-file",
                    str(output),
                ],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            if (
                output.read_bytes()
                != (project / f"{role}-requirements.txt").read_bytes()
            ):
                print(f"RUNTIME_REQUIREMENTS_STALE: {role}")
                matched = False
    return matched


if __name__ == "__main__":
    raise SystemExit(0 if check_exports() else 1)
