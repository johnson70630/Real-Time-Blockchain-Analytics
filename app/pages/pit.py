"""Interactive point-in-time pricing lesson."""

from __future__ import annotations

from typing import Any

import streamlit as st

from app.components.pit_lesson import render_pit_lesson
from app.components.shared import (
    format_timestamp,
    price_status_label,
    render_page_intro,
)
from app.config import AppConfig
from app.data import SnowflakeTutorialRepository


@st.cache_data(ttl=300, show_spinner=False)
def load_pit_protocols(config: AppConfig) -> list[str]:
    return SnowflakeTutorialRepository(config).list_pit_protocols()


@st.cache_data(ttl=300, show_spinner=False)
def load_pit_tokens(config: AppConfig, protocol: str) -> list[str]:
    return SnowflakeTutorialRepository(config).list_pit_tokens(protocol)


@st.cache_data(ttl=300, show_spinner=False)
def load_pit_statuses(
    config: AppConfig, protocol: str, token_symbol: str | None
) -> list[str]:
    return SnowflakeTutorialRepository(config).list_pit_price_statuses(
        protocol, token_symbol
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_pit_rows(
    config: AppConfig,
    protocol: str,
    token_symbol: str | None,
    price_status: str | None,
) -> list[dict[str, Any]]:
    return SnowflakeTutorialRepository(config).list_pit_rows(
        protocol,
        token_symbol=token_symbol,
        price_status=price_status,
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_pit_row(
    config: AppConfig,
    event_domain: str,
    event_id: str,
    token_side: str,
) -> dict[str, Any] | None:
    return SnowflakeTutorialRepository(config).get_pit_row(
        event_domain, event_id, token_side
    )


def pit_row_label(row: dict[str, Any]) -> str:
    """Build a unique event-token selector label."""
    timestamp = format_timestamp(row["event_timestamp"])
    event_suffix = row["event_id"][-12:]
    return (
        f"{timestamp} | {row['event_type']} | {row['token_symbol']} "
        f"({row['token_side']}) | …{event_suffix}"
    )


def render_pit(config: AppConfig) -> None:
    """Select and teach one canonical event-token price decision."""
    render_page_intro(
        "Point-in-Time Pricing",
        "See how a real DeFi event is matched to the latest eligible oracle "
        "observation without using future information.",
    )

    st.subheader("1. Choose a real example")
    protocols = load_pit_protocols(config)
    if not protocols:
        st.info("No point-in-time pricing rows are currently available.")
        return
    protocol = st.selectbox("Protocol", protocols, key="pit_protocol")
    token_choice = st.selectbox(
        "Token",
        ["All tokens", *load_pit_tokens(config, protocol)],
        key="pit_token",
    )
    token_symbol = None if token_choice == "All tokens" else token_choice
    status_choice = st.selectbox(
        "Price status",
        ["All statuses", *load_pit_statuses(config, protocol, token_symbol)],
        format_func=lambda status: (
            status if status == "All statuses" else price_status_label(status)
        ),
        key="pit_status",
    )
    price_status = None if status_choice == "All statuses" else status_choice

    rows = load_pit_rows(config, protocol, token_symbol, price_status)
    if not rows:
        st.warning("No event-token rows match the selected filters.")
        return

    by_identity = {
        (row["event_domain"], row["event_id"], row["token_side"]): row
        for row in rows
    }
    identity = st.selectbox(
        "Event-token timestamp and identifier",
        list(by_identity),
        format_func=lambda value: pit_row_label(by_identity[value]),
        key="pit_event_token",
    )
    selected = load_pit_row(config, *identity)
    if selected is None:
        st.error("The selected event-token row is no longer available.")
        return
    render_pit_lesson(selected)
