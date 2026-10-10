"""Interactive Uniswap V3 swap lesson."""

from __future__ import annotations

from typing import Any

import streamlit as st

from app.components.swap_lesson import render_swap_lesson
from app.config import AppConfig
from app.data import SnowflakeTutorialRepository


@st.cache_data(ttl=300, show_spinner=False)
def load_pool_pairs(config: AppConfig) -> list[str]:
    """Cache the small selector vocabulary from Snowflake."""
    return SnowflakeTutorialRepository(config).list_pool_pairs()


@st.cache_data(ttl=300, show_spinner=False)
def load_swaps(
    config: AppConfig, pool_pair: str | None
) -> list[dict[str, Any]]:
    """Cache recent swap choices without caching credentials."""
    return SnowflakeTutorialRepository(config).list_swaps(pool_pair=pool_pair)


@st.cache_data(ttl=300, show_spinner=False)
def load_swap(config: AppConfig, event_id: str) -> dict[str, Any] | None:
    """Cache one immutable tutorial row by event ID."""
    return SnowflakeTutorialRepository(config).get_swap(event_id)


def _swap_label(swap: dict[str, Any]) -> str:
    timestamp = swap["block_timestamp"].isoformat()
    return (
        f"{timestamp} | tx {swap['transaction_hash'][:12]}… | "
        f"log {swap['log_index']}"
    )


def render_uniswap(config: AppConfig) -> None:
    """Select and teach one canonical swap from the tutorial mart."""
    st.title("Uniswap V3 Swap")
    st.caption("Choose a real canonical swap from Snowflake MARTS.")

    pairs = load_pool_pairs(config)
    pair_options = ["All pool pairs", *pairs]
    selected_pair = st.selectbox("Pool pair", pair_options)
    pair_filter = None if selected_pair == "All pool pairs" else selected_pair

    swaps = load_swaps(config, pair_filter)
    if not swaps:
        st.warning("No swaps match the selected pool pair.")
        return

    by_event_id = {swap["event_id"]: swap for swap in swaps}
    event_id = st.selectbox(
        "Swap timestamp and identifier",
        list(by_event_id),
        format_func=lambda value: _swap_label(by_event_id[value]),
    )
    selected = load_swap(config, event_id)
    if selected is None:
        st.error("The selected swap is no longer available in the mart.")
        return
    render_swap_lesson(selected)
