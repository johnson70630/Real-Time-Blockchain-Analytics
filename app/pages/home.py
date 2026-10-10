"""DeFi Data Lab home page."""

import streamlit as st

from app.components.shared import render_lesson_link, render_page_intro


def render_home() -> None:
    """Introduce the product and its trusted data path."""
    render_page_intro(
        "DeFi Data Lab",
        "Learn DeFi concepts through real on-chain data and the pipeline that "
        "turns it into trusted analytics.",
    )
    st.write(
        "The Data Lab connects protocol behavior to production-style data "
        "engineering, so every lesson can be traced from blockchain source "
        "values to an analytical result."
    )
    st.subheader("From blockchain to the lesson")
    st.code(
        "Blockchain → Kafka → Spark → S3 → Snowflake → dbt → "
        "Tutorial Marts → DeFi Data Lab"
    )
    st.caption(
        "Lessons query trusted Snowflake MARTS models and do not recreate "
        "warehouse business logic in the application."
    )

    st.subheader("Choose a lesson")
    left, right = st.columns(2)
    with left:
        render_lesson_link(
            "Learn how a swap works",
            "Uniswap Swap",
            key="home_uniswap",
        )
        render_lesson_link(
            "Learn how Chainlink oracles work",
            "Chainlink Oracle",
            key="home_chainlink",
        )
    with right:
        render_lesson_link(
            "Learn how lending works",
            "Aave Lending",
            key="home_aave",
        )
        render_lesson_link(
            "Learn point-in-time pricing",
            "Point-in-Time Pricing",
            key="home_pit",
        )
