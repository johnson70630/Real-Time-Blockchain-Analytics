"""Validate lightweight runtime prerequisites for warehouse orchestration."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import yaml

from warehouse.load_raw import validate_key_pair_connection_config

LOGGER = logging.getLogger(__name__)
DBT_ROLE = "BLOCKCHAIN_DBT_ROLE"


def validate_dbt_profile(profiles_dir: Path, project_profile: str) -> str:
    """Validate the active dbt target uses key-pair auth and the dbt role."""
    profile_path = profiles_dir / "profiles.yml"
    if not profile_path.is_file():
        raise FileNotFoundError(f"dbt profile not found: {profile_path}")
    profiles = yaml.safe_load(profile_path.read_text(encoding="utf-8")) or {}
    profile = profiles.get(project_profile, {})
    target_name = profile.get("target")
    output = profile.get("outputs", {}).get(target_name, {})
    if str(output.get("role", "")).upper() != DBT_ROLE:
        raise ValueError(f"dbt target must use {DBT_ROLE}")
    if output.get("password"):
        raise ValueError("Password authentication is not allowed for dbt")
    private_key_path = output.get("private_key_path")
    if not private_key_path:
        raise ValueError("dbt target must configure a private_key_path")
    key_path = Path(str(private_key_path)).expanduser()
    if not key_path.is_file():
        raise FileNotFoundError("Configured dbt private key is unavailable")
    return str(target_name)


def main() -> None:
    """Fail early when project paths or credential configuration are absent."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    project_root = Path(os.getenv("PROJECT_ROOT", "/opt/project"))
    dbt_root = project_root / "dbt"
    for required_path in (
        project_root / "warehouse",
        project_root / "config",
        dbt_root / "dbt_project.yml",
    ):
        if not required_path.exists():
            raise FileNotFoundError(f"Required project path missing: {required_path}")

    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "blockchain-dev")
    validate_key_pair_connection_config(connection_name)
    target = validate_dbt_profile(
        Path(os.getenv("DBT_PROFILES_DIR", "~/.dbt")).expanduser(),
        "blockchain_analytics",
    )
    LOGGER.info(
        "Environment valid project=%s snowflake_connection=%s dbt_target=%s "
        "raw_role=BLOCKCHAIN_PIPELINE_ROLE dbt_role=%s",
        project_root,
        connection_name,
        target,
        DBT_ROLE,
    )


if __name__ == "__main__":
    main()
