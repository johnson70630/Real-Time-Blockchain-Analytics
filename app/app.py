"""Streamlit entry point for the DeFi Data Lab."""

from __future__ import annotations

import streamlit as st
from snowflake.connector.errors import Error as SnowflakeError

from app.config import AppConfig
from app.pages.aave import render_aave
from app.pages.chainlink import render_chainlink
from app.pages.home import render_home
from app.pages.pit import render_pit
from app.pages.uniswap import render_uniswap

_PAGES = (
    "Home",
    "Uniswap Swap",
    "Aave Lending",
    "Chainlink Oracle",
    "Point-in-Time Pricing",
)


def main() -> None:
    """Render navigation and delegate to the selected tutorial page."""
    st.set_page_config(page_title="DeFi Data Lab", page_icon="🧪", layout="wide")
    st.sidebar.title("DeFi Data Lab")
    st.sidebar.caption("Real data. Reproducible transformations.")
    page = st.sidebar.radio("Lesson", _PAGES, key="tutorial_page")

    try:
        config = AppConfig.from_env()
        if page == "Home":
            render_home()
        elif page == "Uniswap Swap":
            render_uniswap(config)
        elif page == "Aave Lending":
            render_aave(config)
        elif page == "Chainlink Oracle":
            render_chainlink(config)
        else:
            render_pit(config)
    except (SnowflakeError, ValueError, RuntimeError):
        st.error("Snowflake tutorial data is unavailable.")
        st.caption(
            "The application could not retrieve its trusted tutorial data. "
            "Verify the configured Snowflake connection and try again."
        )
        st.stop()


if __name__ == "__main__":
    main()
