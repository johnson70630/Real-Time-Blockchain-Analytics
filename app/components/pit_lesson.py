"""Presentation helpers for the point-in-time pricing tutorial."""

from __future__ import annotations

from typing import Any

import streamlit as st

from app.components.aave_lesson import (
    valid_amount_usd_display,
    valid_price_display,
)
from app.components.swap_lesson import display_value

_PIT_STATUS_EXPLANATIONS = {
    "priced": "A valid prior Chainlink observation was available.",
    "stale": (
        "A prior observation exists, but it is older than the accepted "
        "300-second threshold and is rejected for valuation."
    ),
    "unmapped": "This token has no approved project price-feed mapping.",
    "no_prior_price": (
        "The token is mapped to a feed, but no observation existed at or "
        "before the event."
    ),
}


def as_of_timeline(row: dict[str, Any]) -> str:
    """Describe the mart-provided selected/event/next ordering."""
    selected = display_value(row["feed_updated_at"])
    event = display_value(row["event_timestamp"])
    next_update = display_value(row["next_feed_updated_at"])
    return f"selected: {selected}\nEVENT:    {event}\nnext:     {next_update}"


def pit_status_explanation(status: str, reason: str | None = None) -> str:
    """Explain a canonical PIT status without implying a valid valuation."""
    explanation = _PIT_STATUS_EXPLANATIONS.get(
        status, "The warehouse returned an unknown price status."
    )
    return f"{explanation} Warehouse reason: {reason}." if reason else explanation


def render_pit_lesson(row: dict[str, Any]) -> None:
    """Render one canonical event-token as-of price decision."""
    st.header(f"{row['protocol']} {row['event_type']} — {row['token_symbol']}")
    st.caption(
        f"Event ID: {row['event_id']} | token side: {row['token_side']}"
    )

    st.subheader("1. Event-token side")
    st.json(
        {
            "protocol": row["protocol"],
            "event_type": row["event_type"],
            "activity_type": row["activity_type"],
            "event_id": row["event_id"],
            "event_timestamp": display_value(row["event_timestamp"]),
            "token_symbol": row["token_symbol"],
            "normalized_amount": display_value(row["normalized_amount"]),
        }
    )

    st.subheader("2. Token-to-feed mapping")
    st.json(
        {
            "feed_address": row["feed_address"],
            "base_asset": row["base_asset"],
            "quote_asset": row["quote_asset"],
        }
    )

    st.subheader("3. Selected historical observation")
    st.markdown(f"**Price status:** `{row['price_status']}`")
    st.json(
        {
            "observation_id": row["observation_id"],
            "round_id": row["round_id"],
            "observation_price_audit": display_value(row["price"]),
            "feed_updated_at": display_value(row["feed_updated_at"]),
            "price_age_seconds": row["price_age_seconds"],
        }
    )
    st.caption(
        pit_status_explanation(
            row["price_status"], row["price_status_reason"]
        )
    )
    price, amount = st.columns(2)
    price.metric(
        "Valid valuation price",
        valid_price_display(row["price_status"], row["price"]),
    )
    amount.metric(
        "Amount USD",
        valid_amount_usd_display(row["price_status"], row["amount_usd"]),
    )

    st.subheader("4. The as-of join")
    st.code(as_of_timeline(row))
    st.write("Core rule: `feed_updated_at <= event_timestamp`")
    st.caption(
        "The selected round is the latest known observation at or before "
        "the event. This result comes from the mart and is not recomputed here."
    )

    st.subheader("5. Next observation — comparison only")
    if row["next_observation_id"] is None:
        st.info("No later observation is available in the current dataset.")
        return

    st.json(
        {
            "next_observation_id": row["next_observation_id"],
            "next_round_id": row["next_round_id"],
            "next_price": display_value(row["next_price"]),
            "next_feed_updated_at": display_value(
                row["next_feed_updated_at"]
            ),
            "seconds_until_next_round": row["seconds_until_next_round"],
        }
    )
    st.warning(
        "The next oracle update happened after the transaction, so using it "
        "would introduce future information. It is never used for valuation."
    )
