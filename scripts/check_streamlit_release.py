"""Bounded browser smoke verification for the hosted Streamlit application."""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY_PATH = PROJECT_ROOT / "config" / "release.json"

EXPECTED_PUBLIC_LABELS = (
    "DeFi Data Lab",
    "Home",
    "Uniswap Swap",
    "Aave Lending",
    "Chainlink Oracle",
    "Point-in-Time Pricing",
)
FORBIDDEN_HOSTED_LABELS = ("Operations",)
_AUTHENTICATION_MARKERS = (
    "sign in",
    "log in",
    "continue with google",
    "continue with github",
)
_STARTING_MARKERS = (
    "app is in the oven",
    "app is sleeping",
    "waking up",
    "please wait",
)


class FailureCategory(StrEnum):
    """Stable, non-sensitive browser-smoke failure categories."""

    BROWSER_UNREACHABLE = "BROWSER_UNREACHABLE"
    DEPLOYMENT_STARTING = "DEPLOYMENT_STARTING"
    AUTHENTICATION_BLOCKED = "AUTHENTICATION_BLOCKED"
    SHELL_NOT_RENDERED = "SHELL_NOT_RENDERED"
    BROWSER_RUNTIME_ERROR = "BROWSER_RUNTIME_ERROR"


class BrowserSmokeError(RuntimeError):
    """A categorized failure safe to expose in release logs."""

    def __init__(self, category: FailureCategory) -> None:
        self.category = category
        super().__init__(category.value)


@dataclass(frozen=True)
class SmokePolicy:
    """Public browser-smoke settings with a finite retry budget."""

    app_url: str
    retry_interval_seconds: int
    max_attempts: int
    timeout_seconds: int

    @classmethod
    def from_file(cls, path: Path = DEFAULT_POLICY_PATH) -> SmokePolicy:
        """Load and validate the version-controlled release policy."""
        values = json.loads(path.read_text())
        policy = cls(
            app_url=validate_public_url(str(values["app_url"])),
            retry_interval_seconds=int(values["retry_interval_seconds"]),
            max_attempts=int(values["max_attempts"]),
            timeout_seconds=int(values["timeout_seconds"]),
        )
        if min(
            policy.retry_interval_seconds,
            policy.max_attempts,
            policy.timeout_seconds,
        ) <= 0:
            raise ValueError("release retry settings must be positive")
        return policy


@dataclass(frozen=True)
class BrowserSnapshot:
    """Non-sensitive browser state used to validate the rendered shell."""

    final_url: str
    visible_labels: frozenset[str]
    body_text: str


def validate_public_url(value: str) -> str:
    """Require a public HTTPS Streamlit Community Cloud application URL."""
    url = value.strip().rstrip("/")
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("STREAMLIT_APP_URL must be an HTTPS URL")
    if not parsed.hostname.endswith(".streamlit.app"):
        raise ValueError("STREAMLIT_APP_URL must use a streamlit.app host")
    if parsed.path not in {"", "/"} or parsed.params or parsed.query:
        raise ValueError("STREAMLIT_APP_URL must be the application root URL")
    return url


def validate_browser_snapshot(snapshot: BrowserSnapshot, app_url: str) -> None:
    """Validate that an unauthenticated browser rendered the public app shell."""
    expected_host = urllib.parse.urlparse(app_url).hostname
    final_host = urllib.parse.urlparse(snapshot.final_url).hostname
    body_text = snapshot.body_text.casefold()

    if final_host == "share.streamlit.io" or any(
        marker in body_text for marker in _AUTHENTICATION_MARKERS
    ):
        raise BrowserSmokeError(FailureCategory.AUTHENTICATION_BLOCKED)

    if any(marker in body_text for marker in _STARTING_MARKERS):
        raise BrowserSmokeError(FailureCategory.DEPLOYMENT_STARTING)

    if final_host != expected_host:
        raise BrowserSmokeError(FailureCategory.SHELL_NOT_RENDERED)

    if not set(EXPECTED_PUBLIC_LABELS).issubset(snapshot.visible_labels):
        raise BrowserSmokeError(FailureCategory.SHELL_NOT_RENDERED)

    if set(FORBIDDEN_HOSTED_LABELS) & snapshot.visible_labels:
        raise BrowserSmokeError(FailureCategory.SHELL_NOT_RENDERED)


def _safe_body_text(surface: Any) -> str:
    """Read only enough page text to classify public deployment state."""
    try:
        return str(surface.locator("body").inner_text(timeout=1_000))[:8_000]
    except Exception:  # Browser state can disappear during a failed navigation.
        return ""


def _browser_snapshot(surface: Any) -> BrowserSnapshot:
    """Capture only the public UI evidence required by the release contract."""
    visible_labels = frozenset(
        label
        for label in (*EXPECTED_PUBLIC_LABELS, *FORBIDDEN_HOSTED_LABELS)
        if surface.get_by_text(label, exact=True).first.is_visible()
    )
    return BrowserSnapshot(
        final_url=surface.url,
        visible_labels=visible_labels,
        body_text=_safe_body_text(surface),
    )


def _find_application_frame(page: Any, app_url: str, timeout_ms: int) -> Any:
    """Locate Streamlit Community Cloud's same-origin application frame."""
    expected_host = urllib.parse.urlparse(app_url).hostname
    deadline = time.monotonic() + timeout_ms / 1_000

    while time.monotonic() < deadline:
        for frame in page.frames:
            parsed = urllib.parse.urlparse(frame.url)
            if parsed.hostname == expected_host and parsed.path.startswith("/~/"):
                return frame
        page.wait_for_timeout(250)

    try:
        validate_browser_snapshot(_browser_snapshot(page), app_url)
    except BrowserSmokeError as error:
        if error.category in {
            FailureCategory.AUTHENTICATION_BLOCKED,
            FailureCategory.DEPLOYMENT_STARTING,
        }:
            raise
    raise BrowserSmokeError(FailureCategory.DEPLOYMENT_STARTING)


def probe_with_playwright(app_url: str, navigation_timeout: float) -> BrowserSnapshot:
    """Open a fresh headless Chromium session and capture rendered shell state."""
    try:
        from playwright.sync_api import (
            Error as PlaywrightError,
            TimeoutError as PlaywrightTimeoutError,
            sync_playwright,
        )
    except ImportError as error:
        raise BrowserSmokeError(FailureCategory.BROWSER_RUNTIME_ERROR) from error

    timeout_ms = max(1_000, int(navigation_timeout * 1_000))
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                context = browser.new_context()
                page = context.new_page()
                try:
                    page.goto(app_url, wait_until="domcontentloaded", timeout=timeout_ms)
                except PlaywrightTimeoutError:
                    final_host = urllib.parse.urlparse(page.url).hostname
                    expected_host = urllib.parse.urlparse(app_url).hostname
                    if final_host == expected_host:
                        raise BrowserSmokeError(
                            FailureCategory.DEPLOYMENT_STARTING
                        )
                    raise BrowserSmokeError(FailureCategory.BROWSER_UNREACHABLE)

                if urllib.parse.urlparse(page.url).hostname == "share.streamlit.io":
                    validate_browser_snapshot(_browser_snapshot(page), app_url)

                app_frame = _find_application_frame(page, app_url, timeout_ms)
                try:
                    app_frame.get_by_text("DeFi Data Lab", exact=True).first.wait_for(
                        state="visible", timeout=timeout_ms
                    )
                except PlaywrightTimeoutError:
                    snapshot = _browser_snapshot(app_frame)
                    validate_browser_snapshot(snapshot, app_url)
                    raise BrowserSmokeError(FailureCategory.SHELL_NOT_RENDERED)

                snapshot = _browser_snapshot(app_frame)
            finally:
                browser.close()
    except BrowserSmokeError:
        raise
    except PlaywrightError as error:
        category = (
            FailureCategory.BROWSER_UNREACHABLE
            if "net::" in str(error).casefold()
            else FailureCategory.BROWSER_RUNTIME_ERROR
        )
        raise BrowserSmokeError(category) from error
    except Exception as error:
        raise BrowserSmokeError(FailureCategory.BROWSER_RUNTIME_ERROR) from error

    validate_browser_snapshot(snapshot, app_url)
    return snapshot


def wait_until_healthy(
    app_url: str,
    policy: SmokePolicy,
    *,
    probe: Callable[[str, float], BrowserSnapshot] = probe_with_playwright,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> int:
    """Retry clean browser probes without exceeding attempt or time limits."""
    validated_url = validate_public_url(app_url)
    deadline = monotonic() + policy.timeout_seconds
    last_category = FailureCategory.BROWSER_RUNTIME_ERROR

    for attempt in range(1, policy.max_attempts + 1):
        remaining_seconds = deadline - monotonic()
        if remaining_seconds <= 0:
            break
        try:
            snapshot = probe(validated_url, min(30.0, remaining_seconds))
            validate_browser_snapshot(snapshot, validated_url)
        except BrowserSmokeError as error:
            last_category = error.category
            if attempt == policy.max_attempts or monotonic() >= deadline:
                break
            print(
                f"Streamlit deployment not ready (attempt {attempt}; "
                f"category={error.category.value}); retrying"
            )
            sleep(policy.retry_interval_seconds)
        else:
            print(
                "Streamlit public application rendered successfully after "
                f"{attempt} attempt(s)"
            )
            return attempt

    raise RuntimeError(
        "Streamlit deployment did not become healthy within the bounded "
        f"retry window: {last_category.value}"
    )


def main() -> None:
    """Run the hosted browser smoke check from GitHub Actions or locally."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--url")
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    arguments = parser.parse_args()
    policy = SmokePolicy.from_file(arguments.policy)
    wait_until_healthy(arguments.url or policy.app_url, policy)


if __name__ == "__main__":
    main()
