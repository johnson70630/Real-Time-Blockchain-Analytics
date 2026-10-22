"""Local-only read-only operational dashboard."""

from __future__ import annotations

import logging
from typing import Any

import streamlit as st

from app.components.operations import (
    alert_display_rows,
    build_duration_chart,
    build_records_chart,
    feed_freshness_rows,
    filter_run_rows,
    operational_summary,
    run_display_rows,
    source_freshness_rows,
    status_label,
    warehouse_freshness_rows,
    watermark_display_rows,
)
from app.components.shared import format_timestamp, render_page_intro
from app.config import AppConfig
from app.data import SnowflakeOperationsRepository
from config.settings import CHAIN, CHAINLINK_FEEDS_CONFIG
from market_data.feeds import load_chainlink_feeds
from orchestration.freshness import FreshnessReport, FreshnessState
from orchestration.run_freshness_checks import collect_freshness

LOGGER = logging.getLogger(__name__)
OPERATIONS_CACHE_TTL_SECONDS = 45


@st.cache_data(ttl=OPERATIONS_CACHE_TTL_SECONDS, show_spinner=False)
def load_operations_alerts(config: AppConfig) -> list[dict[str, Any]]:
    """Cache approved alert fields for less than one minute."""
    return SnowflakeOperationsRepository(config).list_alerts()


@st.cache_data(ttl=OPERATIONS_CACHE_TTL_SECONDS, show_spinner=False)
def load_operations_watermarks(
    config: AppConfig,
    feed_addresses: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Cache only production watermark identities."""
    return SnowflakeOperationsRepository(config).list_watermarks(feed_addresses)


@st.cache_data(ttl=OPERATIONS_CACHE_TTL_SECONDS, show_spinner=False)
def load_operations_runs(config: AppConfig) -> list[dict[str, Any]]:
    """Cache recent approved run metrics without raw metric JSON."""
    return SnowflakeOperationsRepository(config).list_recent_runs(limit=100)


@st.cache_data(ttl=OPERATIONS_CACHE_TTL_SECONDS, show_spinner=False)
def load_operations_freshness() -> FreshnessReport:
    """Reuse the production freshness service and its established policy."""
    return collect_freshness()


def _latest_warehouse_run(runs: list[dict[str, Any]]) -> dict[str, Any] | None:
    return next(
        (
            row
            for row in runs
            if row.get("pipeline_name") == "daily_warehouse_pipeline"
            and row.get("status") in {"SUCCEEDED", "NO_OP"}
        ),
        None,
    )


def _mart_state(report: FreshnessReport) -> str:
    ranking = {
        FreshnessState.FRESH: 0,
        FreshnessState.WARNING: 1,
        FreshnessState.STALE: 2,
    }
    transformations = [
        result
        for result in report.results
        if result.blocking and result.dimension.value == "transformation"
    ]
    if not transformations:
        return "Unavailable"
    return max(transformations, key=lambda item: ranking[item.state]).state.value


def _render_summary(
    alerts: list[dict[str, Any]],
    report: FreshnessReport,
    runs: list[dict[str, Any]],
) -> None:
    summary = operational_summary(alerts, report)
    warehouse = _latest_warehouse_run(runs)
    columns = st.columns(6)
    columns[0].metric("Critical alerts", summary["critical_alerts"])
    columns[1].metric("Warning alerts", summary["warning_alerts"])
    columns[2].metric("Fresh sources", summary["fresh_sources"])
    columns[3].metric("Stale sources", summary["stale_sources"])
    columns[4].metric(
        "Warehouse refresh",
        format_timestamp(warehouse.get("finished_at") if warehouse else None),
    )
    columns[5].metric("MART health", status_label(_mart_state(report)))


def _render_alerts(alerts: list[dict[str, Any]]) -> None:
    st.subheader("Open alerts")
    open_rows = alert_display_rows(alerts)
    if open_rows:
        st.dataframe(open_rows, hide_index=True, width="stretch")
    else:
        st.success("No alerts are currently open.")

    resolved = [row for row in alerts if row.get("state") == "RESOLVED"]
    with st.expander(f"Resolved alert identities ({len(resolved)})"):
        if resolved:
            st.dataframe(
                [
                    {
                        "Alert key": row.get("alert_key"),
                        "Last resolved": format_timestamp(row.get("resolved_at")),
                        "Incident generation": row.get("incident_generation"),
                    }
                    for row in resolved
                ],
                hide_index=True,
                width="stretch",
            )
        else:
            st.info("No resolved alert identities are available.")


def _render_freshness(report: FreshnessReport) -> None:
    st.subheader("Source freshness")
    source_rows = source_freshness_rows(report)
    if source_rows:
        st.dataframe(source_rows, hide_index=True, width="stretch")
    else:
        st.info("Source freshness is unavailable.")

    st.markdown("**Chainlink feed freshness**")
    feed_rows = feed_freshness_rows(report)
    if feed_rows:
        st.dataframe(feed_rows, hide_index=True, width="stretch")
    else:
        st.info("Chainlink feed freshness is unavailable.")


def _render_watermarks(
    watermarks: list[dict[str, Any]],
    feed_names: dict[str, str],
) -> None:
    st.subheader("Production watermarks")
    primary, feeds = watermark_display_rows(watermarks, feed_names)
    if primary:
        st.dataframe(primary, hide_index=True, width="stretch")
    else:
        st.info("Production block watermarks are unavailable.")
    with st.expander("Chainlink feed round cursors"):
        if feeds:
            st.dataframe(feeds, hide_index=True, width="stretch")
        else:
            st.info("Configured feed cursors are unavailable.")


def _render_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    st.subheader("Recent pipeline runs")
    pipelines = sorted(
        {str(row["pipeline_name"]) for row in runs if row.get("pipeline_name")}
    )
    statuses = sorted(
        {str(row["status"]) for row in runs if row.get("status")}
    )
    left, right = st.columns(2)
    selected_pipeline = left.selectbox("Pipeline", ["All", *pipelines])
    selected_status = right.selectbox("Status", ["All", *statuses])
    filtered = filter_run_rows(
        runs,
        pipeline=None if selected_pipeline == "All" else selected_pipeline,
        status=None if selected_status == "All" else selected_status,
    )
    if filtered:
        st.dataframe(run_display_rows(filtered), hide_index=True, width="stretch")
        chart_left, chart_right = st.columns(2)
        with chart_left:
            st.altair_chart(build_duration_chart(filtered), width="stretch")
        with chart_right:
            st.altair_chart(build_records_chart(filtered), width="stretch")
    else:
        st.info("No recent runs match the selected filters.")
    return filtered


def _render_warehouse_and_dq(
    report: FreshnessReport,
    runs: list[dict[str, Any]],
) -> None:
    st.subheader("Warehouse, MART, and data quality")
    freshness_rows = warehouse_freshness_rows(report)
    if freshness_rows:
        st.dataframe(freshness_rows, hide_index=True, width="stretch")
    else:
        st.info("Warehouse and MART freshness is unavailable.")

    latest = _latest_warehouse_run(runs)
    recent_quarantine = sum(int(row.get("records_quarantined") or 0) for row in runs)
    failed_runs = sum(row.get("status") == "FAILED" for row in runs)
    columns = st.columns(4)
    columns[0].metric("Recent quarantine", recent_quarantine)
    columns[1].metric("Recent failed runs", failed_runs)
    columns[2].metric(
        "Latest dbt models",
        int(latest.get("dbt_models") or 0) if latest else "Unavailable",
    )
    columns[3].metric(
        "Latest dbt tests",
        int(latest.get("dbt_tests") or 0) if latest else "Unavailable",
    )
    if latest and latest.get("status") in {"SUCCEEDED", "NO_OP"}:
        st.success("Latest warehouse reconciliation and dbt checks succeeded.")
    else:
        st.warning("No successful recent warehouse reconciliation is available.")


def render_operations(config: AppConfig) -> None:
    """Render local operational state without exposing mutating actions."""
    render_page_intro(
        "Operations",
        "Read-only pipeline health, durable progress, and recent run metrics.",
    )
    st.caption(
        "Local operator view using a separate Snowflake operations session. "
        "This page is intentionally excluded from the hosted public app."
    )
    if config.is_hosted:
        st.error("Operations is unavailable in the public hosted application.")
        return

    try:
        feeds = load_chainlink_feeds(
            CHAINLINK_FEEDS_CONFIG,
            expected_chain=CHAIN,
        )
        addresses = tuple(feed.feed_address for feed in feeds)
        feed_names = {
            feed.feed_address.lower(): feed.name for feed in feeds
        }
        alerts = load_operations_alerts(config)
        watermarks = load_operations_watermarks(config, addresses)
        runs = load_operations_runs(config)
        report = load_operations_freshness()
    except Exception as error:
        LOGGER.error(
            "Operational dashboard data unavailable error_class=%s",
            type(error).__name__,
        )
        st.error("Operational data is currently unavailable.")
        st.caption(
            "Verify the local operations connection and source availability, "
            "then try again. No fallback data is displayed."
        )
        return

    _render_summary(alerts, report, runs)
    _render_alerts(alerts)
    _render_freshness(report)
    _render_watermarks(watermarks, feed_names)
    _render_runs(runs)
    _render_warehouse_and_dq(report, runs)
