from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from config.settings import (
    CHAIN,
    GOLD_PIPELINE_SUMMARY_GLOB,
    GOLD_RECENT_SWAPS_GLOB,
    GOLD_SWAPS_PER_MINUTE_GLOB,
    GOLD_TOP_POOLS_GLOB,
    PROTOCOL,
)


def read_parquet(path: Path) -> pd.DataFrame:
    return duckdb.sql(f"SELECT * FROM read_parquet('{path}')").df()


def render_kpis(summary_df: pd.DataFrame) -> None:
    summary = summary_df.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Swaps", f"{summary['total_swaps']:,}")
    col2.metric("Unique Transactions", f"{summary['unique_transactions']:,}")
    col3.metric("Unique Pools", f"{summary['unique_pools']:,}")
    col4.metric("Latest Block", f"{summary['latest_block_number']:,}")


def render_swaps_per_minute(swaps_per_minute_df: pd.DataFrame) -> None:
    st.subheader("Swaps per Minute")
    st.line_chart(
        swaps_per_minute_df,
        x="minute_ts",
        y="swap_count",
    )


def render_top_pools(top_pools_df: pd.DataFrame) -> None:
    st.subheader("Top Pools by Swap Count")
    st.dataframe(
        top_pools_df[
            [
                "pool_address",
                "swap_count",
                "first_block_seen",
                "latest_block_seen",
                "first_seen_at",
                "latest_seen_at",
            ]
        ],
        use_container_width=True,
    )


def render_recent_swaps(recent_swaps_df: pd.DataFrame) -> None:
    st.subheader("Recent Swap Events")
    st.dataframe(
        recent_swaps_df[
            [
                "event_id",
                "block_number",
                "transaction_hash",
                "pool_address",
                "log_index",
                "kafka_timestamp",
            ]
        ],
        use_container_width=True,
    )


def main() -> None:
    st.set_page_config(
        page_title="Real-Time Blockchain Analytics",
        layout="wide",
    )

    st.title("Real-Time Blockchain Analytics")
    st.caption(
        f"Live {PROTOCOL.replace('_', ' ').title()} swap analytics from "
        f"{CHAIN.title()} using Alchemy, Kafka, Spark, DuckDB, and Parquet"
    )

    summary_df = read_parquet(GOLD_PIPELINE_SUMMARY_GLOB)
    swaps_per_minute_df = read_parquet(GOLD_SWAPS_PER_MINUTE_GLOB)
    top_pools_df = read_parquet(GOLD_TOP_POOLS_GLOB)
    recent_swaps_df = read_parquet(GOLD_RECENT_SWAPS_GLOB)

    render_kpis(summary_df)

    st.divider()

    render_swaps_per_minute(swaps_per_minute_df)
    render_top_pools(top_pools_df)
    render_recent_swaps(recent_swaps_df)


if __name__ == "__main__":
    main()
