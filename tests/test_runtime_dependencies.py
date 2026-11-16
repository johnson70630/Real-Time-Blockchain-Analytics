"""Detect stale or manually edited container exports without installing packages."""

from pathlib import Path
import shutil

from scripts.check_runtime_dependencies import PROJECT, check_exports


def test_frozen_cloud_exports_match_and_reject_a_different_pin(tmp_path: Path):
    for name in (
        "pyproject.toml",
        "uv.lock",
        "producer-requirements.txt",
        "spark-requirements.txt",
    ):
        shutil.copyfile(PROJECT / name, tmp_path / name)
    assert check_exports(tmp_path)
    requirements = tmp_path / "producer-requirements.txt"
    requirements.write_text(
        requirements.read_text().replace("certifi==2026.10.23", "certifi==2025.1.31")
    )
    assert not check_exports(tmp_path)
