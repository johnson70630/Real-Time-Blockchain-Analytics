"""Honest placeholders for tutorials planned after Milestone 10.7A."""

import streamlit as st


def render_placeholder(title: str, milestone: str) -> None:
    """Identify an intentionally unfinished tutorial without fake content."""
    st.title(title)
    st.info(f"This tutorial interface is planned for {milestone}.")
