"""Presentation helpers for the Uniswap swap tutorial."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import streamlit as st

_STATUS_EXPLANATIONS = {
    "priced": "A valid prior Chainlink observation was available.",
    "stale": "The prior observation exceeded the accepted age threshold.",
    "unmapped": "This token has no approved project price-feed mapping.",
    "no_prior_price": "No Chainlink observation existed before the swap.",
}


def display_value(value: Any) -> str:
    """Render missing warehouse values explicitly rather than as zero."""
    if value is None:
        return "Unavailable"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def display_usd(value: Decimal | None) -> str:
    """Format known USD amounts while preserving unknown valuations."""
    if value is None:
        return "Unavailable"
    return f"${value:,.8f}"


def price_status_explanation(status: str, reason: str | None = None) -> str:
    """Explain the mart's pricing result without inventing a valuation."""
    explanation = _STATUS_EXPLANATIONS.get(
        status, "The warehouse returned an unknown price status."
    )
    return f"{explanation} Warehouse reason: {reason}." if reason else explanation


def normalization_equation(
    raw_amount: str,
    decimals: int,
    normalized_amount: Decimal,
) -> str:
    """Show the transformation while using the mart's normalized result."""
    return (
        f"{raw_amount} ÷ 10^{decimals} = "
        f"{display_value(normalized_amount)}"
    )


def _render_token_price(swap: dict[str, Any], side: str) -> None:
    symbol = swap[f"{side}_symbol"]
    status = swap[f"{side}_price_status"]
    st.markdown(f"**{symbol}** — `{status}`")
    col1, col2, col3 = st.columns(3)
    col1.metric("Historical price", display_usd(swap[f"{side}_price_usd"]))
    col2.metric(
        "Feed timestamp",
        display_value(swap[f"{side}_price_feed_updated_at"]),
    )
    col3.metric(
        "Price age (seconds)",
        display_value(swap[f"{side}_price_age_seconds"]),
    )
    st.caption(
        price_status_explanation(
            status,
            swap[f"{side}_price_status_reason"],
        )
    )


def render_swap_lesson(swap: dict[str, Any]) -> None:
    """Render the progressive raw-to-USD tutorial for one real swap."""
    st.header(swap["pool_pair"])
    st.caption(f"Event ID: {swap['event_id']}")

    st.subheader("1. Raw blockchain event")
    st.write("ERC-20 token amounts are recorded on-chain as signed integers.")
    st.json(
        {
            "transaction_hash": swap["transaction_hash"],
            "block_number": swap["block_number"],
            "block_timestamp": display_value(swap["block_timestamp"]),
            "pool_address": swap["pool_address"],
            "amount0_raw": swap["amount0_raw"],
            "amount1_raw": swap["amount1_raw"],
        }
    )

    st.subheader("2. Token metadata")
    token0, token1 = st.columns(2)
    token0.markdown(f"**{swap['token0_symbol']} — {swap['token0_name']}**")
    token0.code(
        f"address: {swap['token0_address']}\ndecimals: {swap['token0_decimals']}"
    )
    token1.markdown(f"**{swap['token1_symbol']} — {swap['token1_name']}**")
    token1.code(
        f"address: {swap['token1_address']}\ndecimals: {swap['token1_decimals']}"
    )

    st.subheader("3. Normalize raw amounts")
    st.write("raw integer → token decimals → human-readable token amount")
    st.code(
        normalization_equation(
            swap["amount0_raw"], swap["token0_decimals"], swap["amount0"]
        )
    )
    st.code(
        normalization_equation(
            swap["amount1_raw"], swap["token1_decimals"], swap["amount1"]
        )
    )
    st.caption(
        "The normalized amounts shown above come from the trusted mart; "
        "the application does not recompute them."
    )

    st.subheader("4. Historical price")
    st.write(
        "Pricing uses the latest Chainlink observation known before the "
        "transaction. Future observations are never selected."
    )
    _render_token_price(swap, "token0")
    _render_token_price(swap, "token1")

    st.subheader("5. USD interpretation")
    usd0, usd1 = st.columns(2)
    usd0.metric(
        f"{swap['token0_symbol']} signed USD amount",
        display_usd(swap["amount0_usd"]),
    )
    usd1.metric(
        f"{swap['token1_symbol']} signed USD amount",
        display_usd(swap["amount1_usd"]),
    )
    st.caption(
        "Unknown or invalid valuations remain unavailable; NULL is never "
        "displayed as $0. Signed values are preserved exactly as supplied "
        "by the mart."
    )
