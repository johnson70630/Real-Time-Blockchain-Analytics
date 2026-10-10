"""Interactive Chainlink oracle observation lesson."""

from __future__ import annotations

from typing import Any

import altair as alt
import streamlit as st

from app.components.oracle_lesson import render_oracle_lesson
from app.components.shared import format_timestamp, render_page_intro
from app.config import AppConfig
from app.data import SnowflakeTutorialRepository


@st.cache_data(ttl=300, show_spinner=False)
def load_chainlink_feeds(config: AppConfig) -> list[dict[str, Any]]:
    """Cache the canonical feed selector values."""
    return SnowflakeTutorialRepository(config).list_chainlink_feeds()


@st.cache_data(ttl=300, show_spinner=False)
def load_chainlink_observations(
    config: AppConfig, feed_address: str
) -> list[dict[str, Any]]:
    """Cache recent observation choices for one feed."""
    return SnowflakeTutorialRepository(config).list_chainlink_observations(
        feed_address
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_chainlink_observation(
    config: AppConfig, observation_id: str
) -> dict[str, Any] | None:
    """Cache one immutable oracle observation."""
    return SnowflakeTutorialRepository(config).get_chainlink_observation(
        observation_id
    )


def feed_label(feed: dict[str, Any]) -> str:
    """Build a feed selector label from canonical mart values."""
    return f"{feed['base_asset']}/{feed['quote_asset']}"


def observation_label(observation: dict[str, Any]) -> str:
    """Build a unique observation selector label."""
    timestamp = format_timestamp(observation["feed_updated_at"])
    return f"{timestamp} | round {observation['round_id']}"


def prepare_price_history(
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Prepare chronological mart prices for Streamlit's native chart."""
    points = [
        {
            "Observation time": observation["feed_updated_at"],
            "Oracle price": observation["price"],
        }
        for observation in observations
        if observation.get("feed_updated_at") is not None
        and observation.get("price") is not None
    ]
    return sorted(points, key=lambda point: point["Observation time"])


def build_price_history_chart(history: list[dict[str, Any]]) -> alt.Chart:
    """Build a quantitative chart without changing exact display values."""
    chart_values = [
        {
            "Observation time": point["Observation time"].isoformat(),
            "Oracle price": format(point["Oracle price"], "f"),
        }
        for point in history
    ]
    return (
        alt.Chart(alt.Data(values=chart_values))
        .mark_line(point=True)
        .encode(
            x=alt.X("Observation time:T", title="Observation time"),
            y=alt.Y("Oracle price:Q", title="Oracle price", scale=alt.Scale(zero=False)),
            tooltip=(
                alt.Tooltip("Observation time:T", title="Observation time"),
                alt.Tooltip("Oracle price:Q", title="Oracle price"),
            ),
        )
    )


def render_chainlink(config: AppConfig) -> None:
    """Select and teach one canonical Chainlink observation."""
    render_page_intro(
        "Chainlink Oracle",
        "Explore how real oracle rounds represent reference prices and "
        "change over time.",
    )

    st.subheader("1. Choose a real example")
    feeds = load_chainlink_feeds(config)
    if not feeds:
        st.info("No Chainlink feeds are currently available.")
        return
    feeds_by_address = {feed["feed_address"]: feed for feed in feeds}
    feed_address = st.selectbox(
        "Feed",
        list(feeds_by_address),
        format_func=lambda value: feed_label(feeds_by_address[value]),
        key="chainlink_feed",
    )
    observations = load_chainlink_observations(config, feed_address)
    if not observations:
        st.info("No observations are available for the selected feed.")
        return

    history = prepare_price_history(observations)
    st.subheader("Price history")
    if history:
        st.altair_chart(build_price_history_chart(history), width="stretch")
        st.caption(
            "Historical prices for the observations currently loaded in the "
            "selector. Oracle update intervals can vary."
        )
    else:
        st.info("No priced observations are available to chart.")

    by_observation_id = {
        observation["observation_id"]: observation
        for observation in observations
    }
    observation_id = st.selectbox(
        "Observation time and round",
        list(by_observation_id),
        format_func=lambda value: observation_label(by_observation_id[value]),
        key="chainlink_observation",
    )
    selected = load_chainlink_observation(config, observation_id)
    if selected is None:
        st.error("The selected observation is no longer available in the mart.")
        return
    render_oracle_lesson(selected)
