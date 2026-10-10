"""Shared presentation primitives for the DeFi Data Lab lessons."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import streamlit as st

LESSON_PAGES = (
    "Uniswap Swap",
    "Aave Lending",
    "Chainlink Oracle",
    "Point-in-Time Pricing",
)

_PRICE_STATUS_CONTENT = {
    "priced": (
        "Priced",
        "A valid historical Chainlink observation no more than 300 seconds "
        "old was available.",
    ),
    "stale": (
        "Stale",
        "A prior observation exists, but it is older than the accepted "
        "300-second threshold, so valuation is unavailable.",
    ),
    "unmapped": (
        "Unmapped",
        "This token has no approved Chainlink feed mapping, so valuation is "
        "unavailable.",
    ),
    "no_prior_price": (
        "No prior price",
        "An approved feed mapping exists, but no observation was available "
        "at or before the event time.",
    ),
}


def format_decimal(value: Decimal | int | str | None) -> str:
    """Format an exact decimal value readably without converting to float."""
    if value is None:
        return "Unavailable"
    text = format(Decimal(str(value)), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return "0"

    sign = ""
    if text.startswith("-"):
        sign, text = "-", text[1:]
    whole, separator, fraction = text.partition(".")
    grouped = f"{int(whole or '0'):,}"
    return f"{sign}{grouped}{separator}{fraction}"


def format_usd(value: Decimal | None) -> str:
    """Format USD while retaining useful precision for sub-cent values."""
    if value is None:
        return "Unavailable"
    places = 8 if value != 0 and abs(value) < Decimal("0.01") else 2
    return f"${value:,.{places}f}"


def format_price(value: Decimal | None) -> str:
    """Format a price with precision suitable for stablecoin observations."""
    if value is None:
        return "Unavailable"
    return f"${value:,.8f}"


def format_timestamp(value: datetime | None) -> str:
    """Format a timestamp without changing its Snowflake timezone."""
    if value is None:
        return "Unavailable"
    rendered = value.isoformat(sep=" ", timespec="seconds")
    if value.tzinfo is None or value.utcoffset() is None:
        return f"{rendered} (timezone not provided)"
    return rendered


def display_value(value: Any) -> str:
    """Render common warehouse values consistently and preserve NULLs."""
    if value is None:
        return "Unavailable"
    if isinstance(value, datetime):
        return format_timestamp(value)
    if isinstance(value, Decimal):
        return format_decimal(value)
    return str(value)


def shorten_identifier(value: str, *, prefix: int = 6, suffix: int = 4) -> str:
    """Shorten a long identifier for display without changing its value."""
    if len(value) <= prefix + suffix + 1:
        return value
    return f"{value[:prefix]}…{value[-suffix:]}"


def price_status_label(status: str) -> str:
    """Return the consistent human-readable label for a warehouse status."""
    content = _PRICE_STATUS_CONTENT.get(status)
    return content[0] if content else status.replace("_", " ").title()


def price_status_explanation(status: str, reason: str | None = None) -> str:
    """Explain a canonical status without changing warehouse semantics."""
    content = _PRICE_STATUS_CONTENT.get(status)
    explanation = (
        content[1]
        if content
        else "The warehouse returned an unknown price status."
    )
    return f"{explanation} Warehouse reason: {reason}." if reason else explanation


def render_page_intro(title: str, subtitle: str) -> None:
    """Render the common title and subtitle used by every application page."""
    st.title(title)
    st.caption(subtitle)


def render_identifier(label: str, value: str | None) -> None:
    """Show a compact identifier while keeping the complete value accessible."""
    st.markdown(f"**{label}:** {shorten_identifier(value) if value else 'Unavailable'}")
    if value:
        st.code(value)


def render_price_status(status: str, reason: str | None = None) -> None:
    """Render a status with both a text label and its canonical explanation."""
    st.markdown(f"**Price status:** {price_status_label(status)}")
    st.caption(price_status_explanation(status, reason))


def _select_lesson(target: str) -> None:
    st.session_state["tutorial_page"] = target


def render_lesson_link(prompt: str, target: str, *, key: str) -> None:
    """Render navigation that reuses the application's existing page selector."""
    if target not in LESSON_PAGES:
        raise ValueError(f"Unknown lesson target: {target}")
    st.button(
        prompt,
        key=key,
        on_click=_select_lesson,
        args=(target,),
        width="stretch",
    )
