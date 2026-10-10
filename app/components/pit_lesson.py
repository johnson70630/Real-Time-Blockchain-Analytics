"""Presentation helpers for the point-in-time pricing tutorial."""

from __future__ import annotations

from typing import Any

import streamlit as st

from app.components.aave_lesson import (
    valid_amount_usd_display,
    valid_price_display,
)
from app.components.shared import (
    display_value,
    format_decimal,
    price_status_explanation,
    render_identifier,
    render_lesson_link,
    render_price_status,
)


def as_of_timeline(row: dict[str, Any]) -> str:
    """Describe the mart-provided selected/event/next ordering."""
    selected = display_value(row["feed_updated_at"])
    event = display_value(row["event_timestamp"])
    next_update = display_value(row["next_feed_updated_at"])
    return f"selected: {selected}\nEVENT:    {event}\nnext:     {next_update}"


def pit_status_explanation(status: str, reason: str | None = None) -> str:
    """Explain a canonical PIT status without implying a valid valuation."""
    return price_status_explanation(status, reason)


def timeline_points(row: dict[str, Any]) -> tuple[tuple[str, Any, str], ...]:
    """Prepare the mart-provided selected/event/next timeline for display."""
    points: list[tuple[str, Any, str]] = []
    if row["feed_updated_at"] is not None:
        points.append(
            (
                "Selected observation",
                row["feed_updated_at"],
                "Latest known price at or before the event",
            )
        )
    points.append(("Event", row["event_timestamp"], "Valuation timestamp"))
    if row["next_feed_updated_at"] is not None:
        points.append(
            (
                "Next observation",
                row["next_feed_updated_at"],
                "Future comparison only — never used for valuation",
            )
        )
    return tuple(points)


def _render_timeline(row: dict[str, Any]) -> None:
    points = timeline_points(row)
    for index, (label, timestamp, detail) in enumerate(points):
        st.markdown(f"**{label}**  \n`{display_value(timestamp)}`  \n{detail}")
        if index < len(points) - 1:
            st.markdown("↓")


def render_pit_lesson(row: dict[str, Any]) -> None:
    """Render one canonical event-token as-of price decision."""
    st.header(f"{row['protocol']} {row['event_type']} — {row['token_symbol']}")
    st.caption(
        f"Event ID: {row['event_id']} | token side: {row['token_side']}"
    )

    st.subheader("2. Inspect the event-token side")
    st.json(
        {
            "protocol": row["protocol"],
            "event_type": row["event_type"],
            "activity_type": row["activity_type"],
            "event_id": row["event_id"],
            "event_timestamp": display_value(row["event_timestamp"]),
            "token_symbol": row["token_symbol"],
            "normalized_amount": format_decimal(row["normalized_amount"]),
        }
    )
    render_identifier("Token contract", row["token_address"])

    st.subheader("3. Understand the token-to-feed mapping")
    render_identifier("Feed contract", row["feed_address"])
    if row["feed_address"] is None:
        st.info("No approved Chainlink mapping exists for this token.")
    else:
        st.write(f"Feed pair: **{row['base_asset']}/{row['quote_asset']}**")

    st.subheader("4. Inspect the selected historical observation")
    render_price_status(row["price_status"], row["price_status_reason"])
    st.json(
        {
            "observation_id": row["observation_id"],
            "round_id": row["round_id"],
            "observation_price_audit": display_value(row["price"]),
            "feed_updated_at": display_value(row["feed_updated_at"]),
            "price_age_seconds": row["price_age_seconds"],
        }
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

    st.subheader("5. Understand the as-of join")
    _render_timeline(row)
    st.write("Core rule: `feed_updated_at <= event_timestamp`")
    st.caption(
        "The selected round is the latest known observation at or before "
        "the event. This result comes from the mart and is not recomputed here."
    )

    st.subheader("6. Compare with the next observation")
    if row["next_observation_id"] is None:
        st.info("No later observation is available in the current dataset.")
    else:
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
            "The next oracle update happened after the transaction, so using "
            "it would introduce future information. It is comparison-only "
            "and is never used for valuation."
        )
    render_lesson_link(
        "Understand the oracle observations themselves",
        "Chainlink Oracle",
        key="pit_to_chainlink",
    )
