"""DeFi Data Lab home page."""

import streamlit as st


def render_home() -> None:
    """Introduce the product and its trusted data path."""
    st.title("DeFi Data Lab")
    st.write(
        "Real on-chain data, explained through the DeFi protocols and data "
        "engineering decisions that produced it."
    )
    st.subheader("From blockchain to lesson")
    st.code(
        "Blockchain → Kafka → Spark → S3 → Snowflake → dbt → tutorial marts"
    )
    st.caption(
        "Lessons query trusted Snowflake MARTS models and do not recreate "
        "warehouse business logic in the application."
    )
