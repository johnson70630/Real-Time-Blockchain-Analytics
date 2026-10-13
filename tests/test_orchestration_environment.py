"""Tests for lightweight warehouse orchestration prerequisites."""

from pathlib import Path

import pytest
import yaml

from orchestration.validate_environment import validate_dbt_profile


def _write_profile(path: Path, output: dict[str, object]) -> None:
    path.mkdir()
    (path / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                "blockchain_analytics": {
                    "target": "dev",
                    "outputs": {"dev": output},
                }
            }
        ),
        encoding="utf-8",
    )


def test_dbt_profile_requires_dbt_role_and_existing_key(tmp_path: Path) -> None:
    key_path = tmp_path / "key.p8"
    key_path.write_text("unit-test-placeholder", encoding="utf-8")
    profiles_dir = tmp_path / "profiles"
    _write_profile(
        profiles_dir,
        {
            "role": "BLOCKCHAIN_DBT_ROLE",
            "private_key_path": str(key_path),
        },
    )

    assert validate_dbt_profile(profiles_dir, "blockchain_analytics") == "dev"


def test_dbt_profile_rejects_password_and_wrong_role(tmp_path: Path) -> None:
    key_path = tmp_path / "key.p8"
    key_path.write_text("unit-test-placeholder", encoding="utf-8")
    profiles_dir = tmp_path / "profiles"
    _write_profile(
        profiles_dir,
        {
            "role": "BLOCKCHAIN_ADMIN_ROLE",
            "private_key_path": str(key_path),
            "password": "not-allowed",
        },
    )

    with pytest.raises(ValueError, match="BLOCKCHAIN_DBT_ROLE"):
        validate_dbt_profile(profiles_dir, "blockchain_analytics")
