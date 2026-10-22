"""Pure presentation helpers for the operational dashboard."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import altair as alt

from app.components.shared import format_decimal, format_timestamp
from orchestration.freshness import (
    FreshnessDimension,
    FreshnessReport,
    FreshnessState,
)

_FRIENDLY_ALERT_NAMES = {
    "freshness:aave_forward": "Aave source coverage",
    "freshness:uniswap_forward": "Uniswap source coverage",
    "freshness:chainlink_global": "Chainlink source coverage",
    "freshness:chainlink_eth_usd": "Chainlink ETH/USD feed",
    "freshness:chainlink_usdc_usd": "Chainlink USDC/USD feed",
    "freshness:chainlink_usdt_usd": "Chainlink USDT/USD feed",
    "freshness:chainlink_wbtc_usd": "Chainlink WBTC/USD feed",
    "freshness:daily_warehouse": "Daily warehouse refresh",
    "freshness:marts": "MART transformations",
    "pipeline_failure:aave_forward": "Aave pipeline execution",
    "pipeline_failure:chainlink_forward": "Chainlink pipeline execution",
    "pipeline_failure:uniswap_forward": "Uniswap pipeline execution",
    "pipeline_failure:daily_warehouse_pipeline": "Warehouse pipeline execution",
}

_SOURCE_CHECKS = {
    "aave_forward_source_position": "Aave",
    "uniswap_forward_source_position": "Uniswap",
    "chainlink_forward_source_position": "Chainlink global",
}


def status_label(status: str | None) -> str:
    """Return a stable human-readable label without hiding the source state."""
    if not status:
        return "Unavailable"
    return status.replace("_", " ").title()


def operational_summary(
    alerts: list[dict[str, Any]],
    report: FreshnessReport,
) -> dict[str, int]:
    """Count independent alert and source-health categories."""
    open_alerts = [row for row in alerts if row.get("state") == "OPEN"]
    source_results = [
        result
        for result in report.results
        if result.check_name in _SOURCE_CHECKS
    ]
    return {
        "critical_alerts": sum(
            row.get("severity") == "CRITICAL" for row in open_alerts
        ),
        "warning_alerts": sum(
            row.get("severity") == "WARNING" for row in open_alerts
        ),
        "fresh_sources": sum(
            result.state is FreshnessState.FRESH for result in source_results
        ),
        "warning_sources": sum(
            result.state is FreshnessState.WARNING for result in source_results
        ),
        "stale_sources": sum(
            result.state is FreshnessState.STALE for result in source_results
        ),
    }


def alert_display_rows(alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Render approved alert fields with friendly labels and no raw payload."""
    return [
        {
            "Alert": _FRIENDLY_ALERT_NAMES.get(
                str(row.get("alert_key")), str(row.get("check_name", "Unknown"))
            ),
            "Alert key": row.get("alert_key"),
            "Severity": status_label(row.get("severity")),
            "Reason": status_label(row.get("reason_code")),
            "Current": format_decimal(row.get("current_value")),
            "Warning threshold": format_decimal(row.get("warning_threshold")),
            "Critical threshold": format_decimal(row.get("stale_threshold")),
            "Opened": format_timestamp(row.get("opened_at")),
            "Last observed": format_timestamp(row.get("last_observed_at")),
            "Occurrences": int(row.get("occurrence_count") or 0),
            "Generation": int(row.get("incident_generation") or 0),
        }
        for row in alerts
        if row.get("state") == "OPEN"
    ]


def source_freshness_rows(report: FreshnessReport) -> list[dict[str, Any]]:
    """Expose the existing source-lag policy without recalculating it."""
    rows = []
    for result in report.results:
        name = _SOURCE_CHECKS.get(result.check_name)
        if name is None:
            continue
        rows.append(
            {
                "Source": name,
                "Source head": result.details.get("source_head"),
                "Safe head": result.details.get("safe_head"),
                "Committed": result.details.get("committed_position"),
                "Eligible lag": result.observed_value,
                "Warning": result.threshold.warning,
                "Stale": result.threshold.stale,
                "State": result.state.value,
            }
        )
    return rows


def feed_freshness_rows(report: FreshnessReport) -> list[dict[str, Any]]:
    """Expose Chainlink feed ages from the shared freshness service."""
    return [
        {
            "Feed": result.check_name.removeprefix("chainlink_")
            .replace("_", "/")
            .upper(),
            "Age (seconds)": result.observed_value,
            "Warning": result.threshold.warning,
            "Stale": result.threshold.stale,
            "State": result.state.value,
            "Latest effective observation": result.details.get(
                "latest_feed_updated_at"
            ),
        }
        for result in report.results
        if result.dimension is FreshnessDimension.FEED
    ]


def warehouse_freshness_rows(report: FreshnessReport) -> list[dict[str, Any]]:
    """Return load and MART transformation health as separate checks."""
    return [
        {
            "Check": result.check_name,
            "Age (seconds)": result.observed_value,
            "Warning": result.threshold.warning,
            "Stale": result.threshold.stale,
            "State": result.state.value,
            "Latest successful refresh": result.details.get(
                "latest_successful_refresh"
            ),
        }
        for result in report.results
        if result.blocking
        and result.dimension
        in {FreshnessDimension.LOAD, FreshnessDimension.TRANSFORMATION}
    ]


def watermark_display_rows(
    watermarks: list[dict[str, Any]],
    feed_names: dict[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Separate primary production positions from optional feed cursors."""
    primary = []
    feeds = []
    source_labels = {
        "aave_v3_pool": "Aave global block",
        "uniswap_v3_pools": "Uniswap source coverage",
        "chainlink_feeds": "Chainlink global block",
    }
    for row in watermarks:
        common = {
            "Position": row.get("committed_value"),
            "Updated": format_timestamp(row.get("updated_at")),
            "Successful run": row.get("successful_run_id"),
        }
        if row.get("watermark_type") == "block_number":
            primary.append(
                {
                    "Watermark": source_labels.get(
                        str(row.get("source_name")),
                        str(row.get("pipeline_name")),
                    ),
                    **common,
                }
            )
        else:
            address = str(row.get("source_name", "")).lower()
            feeds.append(
                {
                    "Feed": feed_names.get(address, address),
                    # Chainlink proxy round IDs can exceed signed 64-bit
                    # integer limits used by Streamlit's Arrow conversion.
                    "Round cursor": str(row.get("committed_value")),
                    "Updated": common["Updated"],
                }
            )
    return primary, feeds


def filter_run_rows(
    runs: list[dict[str, Any]],
    *,
    pipeline: str | None,
    status: str | None,
) -> list[dict[str, Any]]:
    """Apply the two lightweight UI filters without changing source rows."""
    return [
        row
        for row in runs
        if (pipeline is None or row.get("pipeline_name") == pipeline)
        and (status is None or row.get("status") == status)
    ]


def run_display_rows(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Render approved run metrics without exposing METRIC_DETAILS JSON."""
    return [
        {
            "Pipeline": row.get("pipeline_name"),
            "Type": status_label(row.get("run_type")),
            "Status": status_label(row.get("status")),
            "Started": format_timestamp(row.get("started_at")),
            "Finished": format_timestamp(row.get("finished_at")),
            "Duration (s)": format_decimal(row.get("duration_seconds")),
            "Watermark before": row.get("watermark_before"),
            "Watermark after": row.get("watermark_after"),
            "Progress": "Yes" if row.get("made_progress") else "No",
            "Written": int(row.get("records_written") or 0),
            "Inserted": int(row.get("records_inserted") or 0),
            "Updated": int(row.get("records_updated") or 0),
            "Quarantined": int(row.get("records_quarantined") or 0),
            "Retries": int(row.get("retry_count") or 0),
        }
        for row in runs
    ]


def _chart_time(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def build_duration_chart(runs: list[dict[str, Any]]) -> alt.Chart:
    """Build a lightweight recent-duration trend by pipeline."""
    values = [
        {
            "Pipeline": row.get("pipeline_name"),
            "Started": _chart_time(row.get("started_at")),
            "Duration": float(Decimal(str(row.get("duration_seconds") or 0))),
        }
        for row in runs
        if row.get("started_at") is not None
    ]
    return (
        alt.Chart(alt.Data(values=values))
        .mark_line(point=True)
        .encode(
            x=alt.X("Started:T", title="Run start"),
            y=alt.Y("Duration:Q", title="Duration (seconds)"),
            color=alt.Color("Pipeline:N", title="Pipeline"),
            tooltip=("Pipeline:N", "Started:T", "Duration:Q"),
        )
    )


def build_records_chart(runs: list[dict[str, Any]]) -> alt.Chart:
    """Build a lightweight records-written chart from run metadata."""
    values = [
        {
            "Pipeline": row.get("pipeline_name"),
            "Started": _chart_time(row.get("started_at")),
            "Records written": int(row.get("records_written") or 0),
        }
        for row in runs
        if row.get("started_at") is not None
    ]
    return (
        alt.Chart(alt.Data(values=values))
        .mark_bar()
        .encode(
            x=alt.X("Started:T", title="Run start"),
            y=alt.Y("Records written:Q", title="Records written"),
            color=alt.Color("Pipeline:N", title="Pipeline"),
            tooltip=("Pipeline:N", "Started:T", "Records written:Q"),
        )
    )
