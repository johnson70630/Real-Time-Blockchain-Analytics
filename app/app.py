"""Application renderer for the DeFi Data Lab."""

from __future__ import annotations

import streamlit as st
from snowflake.connector.errors import Error as SnowflakeError

from app.config import AppConfig
from app.pages.aave import render_aave
from app.pages.chainlink import render_chainlink
from app.pages.home import render_home
from app.pages.operations import render_operations
from app.pages.pit import render_pit
from app.pages.uniswap import render_uniswap

_PAGES = (
    "Home",
    "Uniswap Swap",
    "Aave Lending",
    "Chainlink Oracle",
    "Point-in-Time Pricing",
)


def available_pages(config: AppConfig) -> tuple[str, ...]:
    """Expose Operations locally without widening the hosted app boundary."""
    return _PAGES if config.is_hosted else (*_PAGES, "Operations")


def main() -> None:
    """Render navigation and delegate to the selected tutorial page."""
    st.set_page_config(page_title="DeFi Data Lab", page_icon="🧪", layout="wide")
    st.sidebar.title("DeFi Data Lab")
    st.sidebar.caption("Real data. Reproducible transformations.")

    try:
        config = AppConfig.from_runtime()
        page = st.sidebar.radio(
            "Lesson",
            available_pages(config),
            key="tutorial_page",
        )
        if page == "Home":
            render_home()
        elif page == "Uniswap Swap":
            render_uniswap(config)
        elif page == "Aave Lending":
            render_aave(config)
        elif page == "Chainlink Oracle":
            render_chainlink(config)
        elif page == "Point-in-Time Pricing":
            render_pit(config)
        else:
            render_operations(config)
    except (SnowflakeError, ValueError, RuntimeError):
        st.error("Snowflake tutorial data is unavailable.")
        st.caption(
            "The application could not retrieve its trusted tutorial data. "
            "Verify the configured Snowflake connection and try again."
        )
        st.stop()


if __name__ == "__main__":
    main()
