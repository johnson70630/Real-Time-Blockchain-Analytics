"""Interactive Aave Borrow and Repay lesson."""

from __future__ import annotations

from typing import Any

import streamlit as st

from app.components.aave_lesson import render_aave_lesson
from app.config import AppConfig
from app.data import SnowflakeTutorialRepository

_ACTIVITY_TYPES = ("BORROW", "REPAY")


@st.cache_data(ttl=300, show_spinner=False)
def load_aave_tokens(config: AppConfig, activity_type: str) -> list[str]:
    """Cache available tokens for the selected activity type."""
    return SnowflakeTutorialRepository(config).list_aave_tokens(activity_type)


@st.cache_data(ttl=300, show_spinner=False)
def load_aave_activities(
    config: AppConfig,
    activity_type: str,
    token_symbol: str | None,
) -> list[dict[str, Any]]:
    """Cache selector rows without caching credentials."""
    return SnowflakeTutorialRepository(config).list_aave_activities(
        activity_type, token_symbol=token_symbol
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_aave_activity(
    config: AppConfig, event_id: str
) -> dict[str, Any] | None:
    """Cache one immutable Aave tutorial event."""
    return SnowflakeTutorialRepository(config).get_aave_activity(event_id)


def aave_activity_label(activity: dict[str, Any]) -> str:
    """Build a unique, useful event selector label."""
    timestamp = activity["block_timestamp"].isoformat()
    transaction_suffix = activity["transaction_hash"][-10:]
    return (
        f"{timestamp} | {activity['token_symbol']} | "
        f"tx …{transaction_suffix} | log {activity['log_index']}"
    )


def render_aave(config: AppConfig) -> None:
    """Select and teach one canonical Borrow or Repay event."""
    st.title("Aave V3 Lending")
    st.caption("Choose a real Borrow or Repay event from Snowflake MARTS.")

    activity_type = st.selectbox("Activity type", _ACTIVITY_TYPES)
    tokens = load_aave_tokens(config, activity_type)
    selected_token = st.selectbox("Token", ["All tokens", *tokens])
    token_filter = None if selected_token == "All tokens" else selected_token

    activities = load_aave_activities(
        config, activity_type, token_filter
    )
    if not activities:
        st.warning("No events match the selected activity and token.")
        return

    by_event_id = {activity["event_id"]: activity for activity in activities}
    event_id = st.selectbox(
        "Event timestamp and identifier",
        list(by_event_id),
        format_func=lambda value: aave_activity_label(by_event_id[value]),
    )
    selected = load_aave_activity(config, event_id)
    if selected is None:
        st.error("The selected Aave event is no longer available in the mart.")
        return
    render_aave_lesson(selected)
