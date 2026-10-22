"""Contracts for credential-free, non-destructive pull-request CI."""

from __future__ import annotations

from pathlib import Path

from scripts.check_secrets import format_findings, scan_paths

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = PROJECT_ROOT / ".github" / "workflows" / "ci.yaml"


def test_ci_has_least_privilege_and_expected_quality_gates() -> None:
    workflow = WORKFLOW.read_text()

    assert "permissions:\n  contents: read" in workflow
    for job in ("quality", "spark", "app-contracts", "dbt-compile", "airflow"):
        assert f"  {job}:" in workflow
    assert "persist-credentials: false" in workflow
    assert "uv lock --check" in workflow
    assert "uv sync --all-extras --dev --frozen" in workflow


def test_ci_commands_are_validation_only_and_credential_free() -> None:
    workflow = WORKFLOW.read_text()

    for forbidden in (
        "snowflake-load",
        "run_aave_forward",
        "run_chainlink_forward",
        "run_uniswap_recovery",
        "run_correction_replay",
        "streamlit deploy",
        "git push",
        "AWS_ACCESS_KEY_ID",
        "SNOWFLAKE_PRIVATE_KEY",
        "ALCHEMY_RPC_URL",
        "AAVE_BACKFILL_RPC_URL",
    ):
        assert forbidden not in workflow


def test_ci_dbt_profile_contains_placeholders_not_credentials() -> None:
    profile = (PROJECT_ROOT / "ci" / "dbt" / "profiles.yml").read_text()
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    macro = (PROJECT_ROOT / "dbt" / "macros" / "is_incremental.sql").read_text()

    assert "target: ci" in profile
    assert "authenticator: externalbrowser" in profile
    assert "password:" not in profile
    assert "private_key" not in profile
    assert "DBT_CI_OFFLINE_COMPILE=true" in makefile
    assert "DBT_CI_OFFLINE_COMPILE" in macro
    assert "adapter.get_relation" in macro
    assert "should_full_refresh()" in macro


def test_secret_scanner_detects_values_without_echoing_them(tmp_path: Path) -> None:
    secret = "AKIA" + "ABCDEFGHIJKLMNOP"
    candidate = tmp_path / "candidate.txt"
    candidate.write_text(f"credential={secret}\n")

    findings = scan_paths((Path("candidate.txt"),), root=tmp_path)
    rendered = format_findings(findings)

    assert len(findings) == 1
    assert findings[0].pattern_name == "AWS access key"
    assert "candidate.txt:1" in rendered
    assert secret not in rendered


def test_secret_scanner_allows_documented_placeholders(tmp_path: Path) -> None:
    candidate = tmp_path / "example.env"
    candidate.write_text(
        "ALCHEMY_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/<YOUR_API_KEY>\n"
        "SNOWFLAKE_PASSWORD=<PASSWORD>\n"
        "private_key_passphrase = \"<PRIVATE_KEY_PASSPHRASE>\"\n"
    )

    assert scan_paths((Path("example.env"),), root=tmp_path) == []


def test_secret_scanner_detects_provider_urls_and_streamlit_secrets(
    tmp_path: Path,
) -> None:
    provider_key = "provider" + "credential123"
    endpoint = tmp_path / "endpoint.env"
    endpoint.write_text(
        "ALCHEMY_WEBSOCKET_URL="
        f"wss://arb-mainnet.g.alchemy.com/v2/{provider_key}\n"
    )
    streamlit_secret = tmp_path / ".streamlit" / "secrets.toml"
    streamlit_secret.parent.mkdir()
    streamlit_secret.write_text("placeholder = true\n")

    findings = scan_paths(
        (Path("endpoint.env"), Path(".streamlit/secrets.toml")),
        root=tmp_path,
    )
    rendered = format_findings(findings)

    assert {finding.pattern_name for finding in findings} == {
        "Alchemy credential-bearing endpoint",
        "tracked Streamlit secrets",
    }
    assert provider_key not in rendered
