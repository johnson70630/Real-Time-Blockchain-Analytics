"""Presentation helpers for the Aave lending tutorial."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import streamlit as st

from app.components.shared import (
    display_value,
    format_decimal,
    format_price,
    format_usd,
    render_identifier,
    render_lesson_link,
    render_price_status,
)
from app.components.swap_lesson import (
    normalization_equation,
)


def actor_roles(activity: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    """Return protocol actor roles without collapsing distinct accounts."""
    if activity["activity_type"] == "BORROW":
        return (
            ("User", activity["user"]),
            ("On behalf of", activity["on_behalf_of"]),
        )
    return (
        ("User", activity["user"]),
        ("Repayer", activity["repayer"]),
    )


def valid_price_display(status: str, price: Decimal | None) -> str:
    """Display a price only when the warehouse marks it as valid."""
    return format_price(price) if status == "priced" else "Unavailable"


def valid_amount_usd_display(status: str, amount: Decimal | None) -> str:
    """Display USD value only for an accepted point-in-time price."""
    return format_usd(amount) if status == "priced" else "Unavailable"


def _render_actors(activity: dict[str, Any]) -> None:
    st.subheader("4. Understand the actor roles")
    columns = st.columns(2)
    for column, (label, value) in zip(
        columns, actor_roles(activity), strict=True
    ):
        column.markdown(f"**{label}**")
        column.code(value)

    if activity["activity_type"] == "BORROW":
        st.caption(
            "The interacting account and the account whose debt is created "
            "can differ. These roles are preserved separately."
        )
    else:
        st.caption(
            "The account whose debt is repaid and the account providing the "
            "repayment can differ. This difference is not a risk judgment."
        )


def _render_activity_details(activity: dict[str, Any]) -> None:
    if activity["activity_type"] == "BORROW":
        st.subheader("5. Inspect Borrow metadata")
        mode, rate, referral = st.columns(3)
        mode.metric(
            "Interest-rate mode",
            display_value(activity["interest_rate_mode"]),
        )
        rate.metric(
            "Borrow rate (raw protocol value)",
            display_value(activity["borrow_rate_raw"]),
        )
        referral.metric(
            "Referral code", display_value(activity["referral_code"])
        )
        st.caption(
            "The borrow rate is shown exactly as emitted by Aave. It is not "
            "converted into APR or APY in the frontend."
        )
    else:
        st.subheader("5. Inspect Repay metadata")
        st.metric("Used aTokens", display_value(activity["use_atokens"]))
        st.caption(
            "This source protocol field indicates whether aTokens were used "
            "in the repayment path."
        )


def render_aave_lesson(activity: dict[str, Any]) -> None:
    """Render the progressive raw-to-USD lesson for one Aave event."""
    activity_type = activity["activity_type"]
    st.header(f"{activity_type.title()} {activity['token_symbol']}")
    st.caption(f"Event ID: {activity['event_id']}")

    st.subheader("2. Inspect the source event")
    if activity_type == "BORROW":
        st.write(
            "A user receives an asset from the Aave lending pool and debt is "
            "created for the relevant on-behalf-of account."
        )
    else:
        st.write("Assets are returned to Aave to reduce outstanding debt.")
    render_identifier("Transaction", activity["transaction_hash"])
    st.write(
        f"Block `{activity['block_number']}` · log `{activity['log_index']}` · "
        f"{display_value(activity['block_timestamp'])}"
    )
    st.write(f"Token: **{activity['token_symbol']} — {activity['token_name']}**")
    render_identifier("Token contract", activity["token_address"])

    st.subheader("3. Transform the raw token amount")
    st.write("raw integer → token decimals → human-readable token amount")
    st.code(
        normalization_equation(
            activity["amount_raw"],
            activity["token_decimals"],
            activity["amount"],
        )
    )
    st.caption(
        "The normalized amount comes from the trusted mart and is not "
        "recalculated in the application."
    )

    _render_actors(activity)
    _render_activity_details(activity)

    st.subheader("6. Apply the historical price")
    st.write(
        "Pricing uses the latest Chainlink observation known before the "
        "event, subject to the established 300-second staleness rule."
    )
    render_price_status(
        activity["price_status"], activity["price_status_reason"]
    )
    price, timestamp, age = st.columns(3)
    price.metric(
        "Historical price",
        valid_price_display(activity["price_status"], activity["price_usd"]),
    )
    timestamp.metric(
        "Oracle timestamp",
        display_value(activity["price_feed_updated_at"]),
    )
    age.metric(
        "Price age (seconds)",
        display_value(activity["price_age_seconds"]),
    )
    st.subheader("7. Interpret the analytical value")
    amount, price_value, usd_value = st.columns(3)
    amount.metric("Normalized amount", format_decimal(activity["amount"]))
    price_value.metric(
        "Valid historical price",
        valid_price_display(activity["price_status"], activity["price_usd"]),
    )
    usd_value.metric(
        "Amount USD",
        valid_amount_usd_display(
            activity["price_status"], activity["amount_usd"]
        ),
    )
    st.caption(
        "Unavailable valuations remain unavailable; NULL is never displayed "
        "as $0. Values retain the mart's original sign and precision."
    )
    render_lesson_link(
        "See how this historical price was selected",
        "Point-in-Time Pricing",
        key="aave_to_pit",
    )
