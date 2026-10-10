"""Interactive Chainlink oracle observation lesson."""

from __future__ import annotations

from typing import Any

import streamlit as st

from app.components.oracle_lesson import render_oracle_lesson
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
    timestamp = observation["feed_updated_at"].isoformat()
    return f"{timestamp} | round {observation['round_id']}"


def render_chainlink(config: AppConfig) -> None:
    """Select and teach one canonical Chainlink observation."""
    st.title("Chainlink Oracle")
    st.caption("Explore real oracle observations from Snowflake MARTS.")

    feeds = load_chainlink_feeds(config)
    feeds_by_address = {feed["feed_address"]: feed for feed in feeds}
    feed_address = st.selectbox(
        "Feed",
        list(feeds_by_address),
        format_func=lambda value: feed_label(feeds_by_address[value]),
    )
    observations = load_chainlink_observations(config, feed_address)
    by_observation_id = {
        observation["observation_id"]: observation
        for observation in observations
    }
    observation_id = st.selectbox(
        "Observation time and round",
        list(by_observation_id),
        format_func=lambda value: observation_label(by_observation_id[value]),
    )
    selected = load_chainlink_observation(config, observation_id)
    if selected is None:
        st.error("The selected observation is no longer available in the mart.")
        return
    render_oracle_lesson(selected)
