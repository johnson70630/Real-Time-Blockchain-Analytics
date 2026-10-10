"""Presentation helpers for the Chainlink oracle tutorial."""

from __future__ import annotations

from typing import Any

import streamlit as st

from app.components.shared import (
    display_value,
    format_price,
    render_identifier,
    render_lesson_link,
)


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

    st.subheader("2. Inspect the oracle round")
    render_identifier("Feed contract", observation["feed_address"])
    st.write(
        f"Pair: **{pair}** · round `{observation['round_id']}` · "
        f"{display_value(observation['feed_updated_at'])}"
    )
    st.code(
        f"answer_raw: {observation['answer_raw']}\n"
        f"feed_decimals: {observation['feed_decimals']}"
    )

    st.subheader("3. Transform the raw answer")
    st.write("raw oracle answer → feed decimals → human-readable price")
    st.code(oracle_price_equation(observation))
    st.caption(
        "The canonical price comes from the trusted mart; the application "
        "does not recalculate it for production display."
    )

    st.subheader("4. Compare with the previous round")
    if observation["previous_observation_id"] is None:
        st.info("No previous observation is available for this round.")
    else:
        st.caption(
            f"Previous observation: {observation['previous_observation_id']}"
        )
    previous, current, change = st.columns(3)
    previous.metric(
        "Previous price", format_price(observation["previous_price"])
    )
    current.metric("Current price", format_price(observation["price"]))
    change.metric("Price change", format_price(observation["price_change"]))
    st.metric(
        "Price change (%)", display_value(observation["price_change_pct"])
    )

    st.subheader("5. Understand oracle timing")
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
    render_lesson_link(
        "See how oracle prices are applied to DeFi events",
        "Point-in-Time Pricing",
        key="chainlink_to_pit",
    )
