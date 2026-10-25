"""Contracts for preserving least-privilege tutorial MART grants."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml

from app.app import available_pages
from app.config import AppConfig
from warehouse.check_mart_grants import (
    ANALYST_ROLE,
    EXPECTED_RELATIONS,
    TUTORIAL_MARTS,
    validate_select_contract,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = PROJECT_ROOT / "dbt"
CI_PROFILE = PROJECT_ROOT / "ci" / "dbt"


@pytest.fixture(scope="module")
def parsed_manifest(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict]:
    target_path = tmp_path_factory.mktemp("dbt-grant-manifest")
    environment = {
        **os.environ,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    result = subprocess.run(
        (
            "dbt",
            "parse",
            "--project-dir",
            str(DBT_ROOT),
            "--profiles-dir",
            str(CI_PROFILE),
            "--target",
            "ci",
            "--no-partial-parse",
            "--target-path",
            str(target_path),
        ),
        cwd=PROJECT_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    yield json.loads((target_path / "manifest.json").read_text())


def _mart_nodes(manifest: dict) -> dict[str, dict]:
    return {
        node["name"]: node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
        and node["original_file_path"].startswith("models/marts/")
    }


def test_effective_tutorial_mart_config_enables_copy_grants(
    parsed_manifest: dict,
) -> None:
    mart_nodes = _mart_nodes(parsed_manifest)

    assert set(mart_nodes) == {name.lower() for name in TUTORIAL_MARTS}
    assert all(node["config"]["copy_grants"] is True for node in mart_nodes.values())


def test_mart_config_preserves_existing_grants_without_assigning_roles(
    parsed_manifest: dict,
) -> None:
    mart_nodes = _mart_nodes(parsed_manifest)

    assert all(not node["config"].get("grants") for node in mart_nodes.values())
    project = yaml.safe_load((DBT_ROOT / "dbt_project.yml").read_text())
    assert project["models"]["blockchain_analytics"]["marts"]["+copy_grants"]

    dbt_sources = "\n".join(
        path.read_text()
        for path in DBT_ROOT.rglob("*")
        if path.is_file() and path.suffix.lower() in {".sql", ".yml", ".yaml"}
    ).lower()
    assert "grant select" not in dbt_sources
    assert "future grants" not in dbt_sources
    assert ANALYST_ROLE.lower() not in dbt_sources


def test_ci_dbt_compile_remains_credential_free() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    profile = yaml.safe_load((CI_PROFILE / "profiles.yml").read_text())

    assert "DBT_CI_OFFLINE_COMPILE=true" in makefile
    assert profile["blockchain_analytics"]["target"] == "ci"
    assert "password" not in profile["blockchain_analytics"]["outputs"]["ci"]


def test_hosted_application_remains_marts_only_without_operations() -> None:
    config = AppConfig(
        environment="hosted",
        connection_name=None,
        account="example-account",
        user="BLOCKCHAIN_APP_USER",
        role=ANALYST_ROLE,
        private_key_pem="placeholder",
        private_key_passphrase="placeholder",
    )

    assert config.schema == "MARTS"
    assert "Operations" not in available_pages(config)
    assert all(".MARTS." in relation for relation in EXPECTED_RELATIONS)


def _grant(relation: str) -> dict[str, str]:
    return {
        "privilege": "SELECT",
        "granted_on": "TABLE",
        "name": relation,
    }


def test_grant_baseline_accepts_exact_tutorial_select_contract() -> None:
    assert validate_select_contract(map(_grant, EXPECTED_RELATIONS), ()) == (0, 0)


def test_grant_baseline_rejects_missing_unexpected_or_future_selects() -> None:
    expected = tuple(map(_grant, EXPECTED_RELATIONS))

    with pytest.raises(RuntimeError, match="Missing analyst SELECT"):
        validate_select_contract(expected[:-1], ())
    with pytest.raises(RuntimeError, match="Unexpected analyst SELECT"):
        validate_select_contract(
            (*expected, _grant("BLOCKCHAIN_ANALYTICS.CORE.DIM_TOKEN")), ()
        )
    with pytest.raises(RuntimeError, match="future-table SELECT"):
        validate_select_contract(expected, (_grant("BLOCKCHAIN_ANALYTICS.MARTS.<TABLE>"),))
