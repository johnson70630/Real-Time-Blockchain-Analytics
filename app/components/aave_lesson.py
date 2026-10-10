"""Presentation helpers for the Aave lending tutorial."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import streamlit as st

from app.components.swap_lesson import (
    display_usd,
    display_value,
    normalization_equation,
    price_status_explanation,
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
    return display_usd(price) if status == "priced" else "Unavailable"


def valid_amount_usd_display(status: str, amount: Decimal | None) -> str:
    """Display USD value only for an accepted point-in-time price."""
    return display_usd(amount) if status == "priced" else "Unavailable"


def _render_actors(activity: dict[str, Any]) -> None:
    st.subheader("3. Actor roles")
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
        st.subheader("4. Borrow-specific details")
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
        st.subheader("5. Repay-specific details")
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

    st.subheader("1. What happened?")
    if activity_type == "BORROW":
        st.write(
            "A user receives an asset from the Aave lending pool and debt is "
            "created for the relevant on-behalf-of account."
        )
    else:
        st.write("Assets are returned to Aave to reduce outstanding debt.")
    st.json(
        {
            "activity_type": activity_type,
            "block_timestamp": display_value(activity["block_timestamp"]),
            "block_number": activity["block_number"],
            "transaction_hash": activity["transaction_hash"],
            "log_index": activity["log_index"],
            "token_symbol": activity["token_symbol"],
            "token_name": activity["token_name"],
        }
    )

    st.subheader("2. Raw token amount")
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

    st.subheader("6. Historical price")
    st.write(
        "Pricing uses the latest Chainlink observation known before the "
        "event, subject to the established 300-second staleness rule."
    )
    st.markdown(f"**Price status:** `{activity['price_status']}`")
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
    st.caption(
        price_status_explanation(
            activity["price_status"], activity["price_status_reason"]
        )
    )

    st.subheader("7. USD value")
    amount, price_value, usd_value = st.columns(3)
    amount.metric("Normalized amount", display_value(activity["amount"]))
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
