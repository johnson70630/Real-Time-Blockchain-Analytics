"""Streamlit entry point for the DeFi Data Lab."""

from __future__ import annotations

import streamlit as st
from snowflake.connector.errors import Error as SnowflakeError

from app.config import AppConfig
from app.pages.aave import render_aave
from app.pages.home import render_home
from app.pages.placeholders import render_placeholder
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
    page = st.sidebar.radio("Tutorial", _PAGES)

    try:
        config = AppConfig.from_env()
        if page == "Home":
            render_home()
        elif page == "Uniswap Swap":
            render_uniswap(config)
        elif page == "Aave Lending":
            render_aave(config)
        else:
            milestone = {
                "Chainlink Oracle": "a later frontend milestone",
                "Point-in-Time Pricing": "a later frontend milestone",
            }[page]
            render_placeholder(page, milestone)
    except (SnowflakeError, ValueError, RuntimeError) as error:
        st.error("Snowflake tutorial data is unavailable.")
        st.caption(str(error))
        st.stop()


if __name__ == "__main__":
    main()
