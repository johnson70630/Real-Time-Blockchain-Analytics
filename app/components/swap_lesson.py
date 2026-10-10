"""Presentation helpers for the Uniswap swap tutorial."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import streamlit as st

from app.components.shared import (
    display_value,
    format_decimal,
    format_price,
    format_usd as display_usd,
    price_status_explanation as _price_status_explanation,
    render_identifier,
    render_lesson_link,
    render_price_status,
)


def price_status_explanation(status: str, reason: str | None = None) -> str:
    """Keep the established public helper while using shared status copy."""
    return _price_status_explanation(status, reason)


def normalization_equation(
    raw_amount: str,
    decimals: int,
    normalized_amount: Decimal,
) -> str:
    """Show the transformation while using the mart's normalized result."""
    return (
        f"{raw_amount} ÷ 10^{decimals} = "
        f"{format_decimal(normalized_amount)}"
    )


def _render_token_price(swap: dict[str, Any], side: str) -> None:
    symbol = swap[f"{side}_symbol"]
    status = swap[f"{side}_price_status"]
    st.markdown(f"**{symbol}**")
    render_price_status(status, swap[f"{side}_price_status_reason"])
    col1, col2, col3 = st.columns(3)
    col1.metric("Historical price", format_price(swap[f"{side}_price_usd"]))
    col2.metric(
        "Feed timestamp",
        display_value(swap[f"{side}_price_feed_updated_at"]),
    )
    col3.metric(
        "Price age (seconds)",
        display_value(swap[f"{side}_price_age_seconds"]),
    )


def render_swap_lesson(swap: dict[str, Any]) -> None:
    """Render the progressive raw-to-USD tutorial for one real swap."""
    st.header(swap["pool_pair"])
    st.caption(f"Event ID: {swap['event_id']}")

    st.subheader("2. Inspect the raw blockchain event")
    st.write("ERC-20 token amounts are recorded on-chain as signed integers.")
    render_identifier("Transaction", swap["transaction_hash"])
    render_identifier("Pool", swap["pool_address"])
    st.write(
        f"Block `{swap['block_number']}` · "
        f"{display_value(swap['block_timestamp'])}"
    )
    st.code(
        f"amount0_raw: {swap['amount0_raw']}\n"
        f"amount1_raw: {swap['amount1_raw']}"
    )

    st.subheader("3. Understand the token metadata")
    token0, token1 = st.columns(2)
    token0.markdown(f"**{swap['token0_symbol']} — {swap['token0_name']}**")
    token0.code(
        f"address: {swap['token0_address']}\ndecimals: {swap['token0_decimals']}"
    )
    token1.markdown(f"**{swap['token1_symbol']} — {swap['token1_name']}**")
    token1.code(
        f"address: {swap['token1_address']}\ndecimals: {swap['token1_decimals']}"
    )

    st.subheader("4. Transform raw amounts")
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

    st.subheader("5. Apply historical prices")
    st.write(
        "Pricing uses the latest Chainlink observation known before the "
        "transaction. Future observations are never selected."
    )
    _render_token_price(swap, "token0")
    _render_token_price(swap, "token1")

    st.subheader("6. Interpret the analytical value")
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
    render_lesson_link(
        "See how these historical prices were selected",
        "Point-in-Time Pricing",
        key="swap_to_pit",
    )
