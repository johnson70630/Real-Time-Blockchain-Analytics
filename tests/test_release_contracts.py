"""Contracts for main-only, non-mutating Streamlit release verification."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.app import available_pages
from app.config import AppConfig
from scripts.check_streamlit_release import (
    EXPECTED_PUBLIC_LABELS,
    BrowserSmokeError,
    BrowserSnapshot,
    FailureCategory,
    SmokePolicy,
    validate_browser_snapshot,
    validate_public_url,
    wait_until_healthy,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = PROJECT_ROOT / ".github" / "workflows" / "release.yaml"
POLICY = PROJECT_ROOT / "config" / "release.json"
SMOKE_SCRIPT = PROJECT_ROOT / "scripts" / "check_streamlit_release.py"
APP_URL = "https://defi-data-lab.streamlit.app"


def rendered_snapshot(*, labels: frozenset[str] | None = None) -> BrowserSnapshot:
    return BrowserSnapshot(
        final_url=f"{APP_URL}/~/+/",
        visible_labels=labels or frozenset(EXPECTED_PUBLIC_LABELS),
        body_text="DeFi Data Lab",
    )


def test_release_is_main_only_after_successful_ci() -> None:
    workflow = WORKFLOW.read_text()

    assert "workflow_run:" in workflow
    assert "      - CI" in workflow
    assert "      - main" in workflow
    assert "workflow_run.conclusion == 'success'" in workflow
    assert "workflow_run.event == 'push'" in workflow
    assert "workflow_run.head_branch == 'main'" in workflow
    assert "ref: ${{ env.RELEASE_SHA }}" in workflow


def test_release_is_least_privilege_and_credential_free() -> None:
    workflow = WORKFLOW.read_text()

    assert "permissions:\n  contents: read" in workflow
    assert "persist-credentials: false" in workflow
    assert "vars." not in workflow
    assert "secrets." not in workflow
    for forbidden in (
        "snowflake-load",
        "dbt run",
        "airflow-up",
        "run_aave_forward",
        "run_chainlink_forward",
        "run_uniswap_recovery",
        "AWS_ACCESS_KEY_ID",
        "SNOWFLAKE_PRIVATE_KEY",
        "ALCHEMY_RPC_URL",
        "AAVE_BACKFILL_RPC_URL",
    ):
        assert forbidden not in workflow


def test_release_installs_isolated_playwright_chromium_runtime() -> None:
    workflow = WORKFLOW.read_text()

    assert (
        "python -m pip install --disable-pip-version-check playwright==1.64.1"
        in workflow
    )
    assert "python -m playwright install --with-deps chromium" in workflow
    assert "python scripts/check_streamlit_release.py" in workflow


def test_release_policy_identifies_streamlit_and_bounded_retries() -> None:
    values = json.loads(POLICY.read_text())
    policy = SmokePolicy.from_file(POLICY)

    assert values["component"] == "defi-data-lab-streamlit"
    assert values["environment"] == "production"
    assert values["release_branch"] == "main"
    assert values["entrypoint"] == "streamlit_app.py"
    assert values["deployment_mechanism"] == "streamlit_community_cloud_main"
    assert policy.app_url == (
        "https://real-time-blockchain-analytics-"
        "pyys8l6fsnypocxtz6exjp.streamlit.app"
    )
    assert policy.retry_interval_seconds == 15
    assert policy.max_attempts == 20
    assert policy.timeout_seconds == 300
    assert "health_path" not in values


def test_streamlit_entrypoint_and_hosted_navigation_remain_safe() -> None:
    entrypoint = (PROJECT_ROOT / "streamlit_app.py").read_text()
    hosted = AppConfig(
        environment="hosted",
        connection_name=None,
        account="example-account",
        user="BLOCKCHAIN_APP_USER",
        role="BLOCKCHAIN_ANALYST_ROLE",
        private_key_pem="placeholder",
        private_key_passphrase="placeholder",
    )

    assert "from app.app import main" in entrypoint
    assert "Operations" not in available_pages(hosted)


def test_browser_snapshot_accepts_public_lesson_shell() -> None:
    validate_browser_snapshot(rendered_snapshot(), APP_URL)


@pytest.mark.parametrize(
    ("snapshot", "category"),
    (
        (
            BrowserSnapshot(
                "https://share.streamlit.io/-/auth/app",
                frozenset(),
                "",
            ),
            FailureCategory.AUTHENTICATION_BLOCKED,
        ),
        (
            BrowserSnapshot(APP_URL, frozenset(), "Sign in to continue"),
            FailureCategory.AUTHENTICATION_BLOCKED,
        ),
        (
            BrowserSnapshot(APP_URL, frozenset(), "This app is waking up"),
            FailureCategory.DEPLOYMENT_STARTING,
        ),
        (
            BrowserSnapshot(APP_URL, frozenset({"DeFi Data Lab"}), ""),
            FailureCategory.SHELL_NOT_RENDERED,
        ),
        (
            rendered_snapshot(
                labels=frozenset((*EXPECTED_PUBLIC_LABELS, "Operations"))
            ),
            FailureCategory.SHELL_NOT_RENDERED,
        ),
    ),
)
def test_browser_snapshot_rejects_invalid_hosted_states(
    snapshot: BrowserSnapshot, category: FailureCategory
) -> None:
    with pytest.raises(BrowserSmokeError) as error:
        validate_browser_snapshot(snapshot, APP_URL)

    assert error.value.category == category


def test_failure_categories_are_stable_and_complete() -> None:
    assert {category.value for category in FailureCategory} == {
        "BROWSER_UNREACHABLE",
        "DEPLOYMENT_STARTING",
        "AUTHENTICATION_BLOCKED",
        "SHELL_NOT_RENDERED",
        "BROWSER_RUNTIME_ERROR",
    }


def test_raw_http_status_is_not_part_of_browser_release_contract() -> None:
    smoke_script = SMOKE_SCRIPT.read_text()

    assert "urllib.request" not in smoke_script
    assert "/_stcore/health" not in smoke_script
    assert "HTTP 303" not in smoke_script


def test_smoke_check_retries_then_succeeds_without_credentials() -> None:
    policy = SmokePolicy(APP_URL, 15, 3, 300)
    calls: list[str] = []
    sleeps: list[float] = []

    def probe(url: str, timeout: float) -> BrowserSnapshot:
        calls.append(url)
        if len(calls) == 1:
            raise BrowserSmokeError(FailureCategory.DEPLOYMENT_STARTING)
        return rendered_snapshot()

    attempts = wait_until_healthy(
        APP_URL,
        policy,
        probe=probe,
        sleep=sleeps.append,
        monotonic=lambda: 0.0,
    )

    assert attempts == 2
    assert sleeps == [15]
    assert calls == [APP_URL, APP_URL]


def test_smoke_check_stops_at_attempt_limit() -> None:
    policy = SmokePolicy(APP_URL, 15, 2, 300)

    def unavailable(url: str, timeout: float) -> BrowserSnapshot:
        raise BrowserSmokeError(FailureCategory.BROWSER_UNREACHABLE)

    with pytest.raises(RuntimeError, match="BROWSER_UNREACHABLE"):
        wait_until_healthy(
            APP_URL,
            policy,
            probe=unavailable,
            sleep=lambda seconds: None,
            monotonic=lambda: 0.0,
        )


def test_smoke_check_stops_at_overall_deadline() -> None:
    policy = SmokePolicy(APP_URL, 15, 20, 300)
    ticks = iter((0.0, 301.0))
    probes: list[str] = []

    def unavailable(url: str, timeout: float) -> BrowserSnapshot:
        probes.append(url)
        raise BrowserSmokeError(FailureCategory.BROWSER_UNREACHABLE)

    with pytest.raises(RuntimeError, match="bounded retry window"):
        wait_until_healthy(
            APP_URL,
            policy,
            probe=unavailable,
            sleep=lambda seconds: None,
            monotonic=lambda: next(ticks),
        )

    assert probes == []


@pytest.mark.parametrize(
    "url",
    (
        "http://defi-data-lab.streamlit.app",
        "https://example.com",
        "https://defi-data-lab.streamlit.app/private",
    ),
)
def test_smoke_check_rejects_unsafe_or_non_root_urls(url: str) -> None:
    with pytest.raises(ValueError):
        validate_public_url(url)


def test_rollback_contract_forbids_history_rewrites() -> None:
    values = json.loads(POLICY.read_text())
    documentation = (PROJECT_ROOT / "docs" / "release_and_cd.md").read_text()

    assert values["rollback_strategy"] == "revert_or_forward_fix"
    assert values["force_push_rollback_allowed"] is False
    assert "normal revert commit" in documentation
    assert "Never force-push" in documentation
    assert "rewrite `main`" in documentation
