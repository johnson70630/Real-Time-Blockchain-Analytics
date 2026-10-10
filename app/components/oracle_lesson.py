"""Presentation helpers for the Chainlink oracle tutorial."""

from __future__ import annotations

from typing import Any

import streamlit as st

from app.components.swap_lesson import display_value


def oracle_price_equation(observation: dict[str, Any]) -> str:
    """Explain scaling while displaying the mart's canonical price."""
    return (
        f"{observation['answer_raw']} ÷ 10^{observation['feed_decimals']} "
        f"= {display_value(observation['price'])}"
    )


def render_oracle_lesson(observation: dict[str, Any]) -> None:
    """Render one observation and its previous-round context."""
    pair = f"{observation['base_asset']}/{observation['quote_asset']}"
    st.header(f"{pair} oracle observation")
    st.caption(f"Observation ID: {observation['observation_id']}")

    st.subheader("1. Oracle round")
    st.json(
        {
            "feed_address": observation["feed_address"],
            "base_asset": observation["base_asset"],
            "quote_asset": observation["quote_asset"],
            "round_id": observation["round_id"],
            "answer_raw": observation["answer_raw"],
            "feed_decimals": observation["feed_decimals"],
            "feed_updated_at": display_value(observation["feed_updated_at"]),
        }
    )

    st.subheader("2. Scale the raw answer")
    st.write("raw oracle answer → feed decimals → human-readable price")
    st.code(oracle_price_equation(observation))
    st.caption(
        "The canonical price comes from the trusted mart; the application "
        "does not recalculate it for production display."
    )

    st.subheader("3. Previous-round comparison")
    previous, current, change = st.columns(3)
    previous.metric(
        "Previous price", display_value(observation["previous_price"])
    )
    current.metric("Current price", display_value(observation["price"]))
    change.metric("Price change", display_value(observation["price_change"]))
    st.metric(
        "Price change (%)", display_value(observation["price_change_pct"])
    )

    st.subheader("4. Oracle timing")
    previous_time, current_time, interval = st.columns(3)
    previous_time.metric(
        "Previous observation",
        display_value(observation["previous_feed_updated_at"]),
    )
    current_time.metric(
        "Current observation", display_value(observation["feed_updated_at"])
    )
    interval.metric(
        "Seconds since previous round",
        display_value(observation["seconds_since_previous_round"]),
    )
    st.caption(
        "Oracle rounds do not necessarily arrive at fixed intervals. The "
        "timing above is the real interval for these two observations."
    )
