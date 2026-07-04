from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

GOLD_DIR = Path("data/gold")

PIPELINE_SUMMARY_PATH = GOLD_DIR / "pipeline_summary" / "*.parquet"
SWAPS_PER_MINUTE_PATH = GOLD_DIR / "swaps_per_minute" / "*.parquet"
TOP_POOLS_PATH = GOLD_DIR / "top_pools" / "*.parquet"
RECENT_SWAPS_PATH = GOLD_DIR / "recent_swaps" / "*.parquet"


def read_parquet(path: Path) -> pd.DataFrame:
    return duckdb.sql(f"SELECT * FROM read_parquet('{path}')").df()


st.set_page_config(
    page_title="Real-Time Blockchain Analytics",
    layout="wide",
)

st.title("Real-Time Blockchain Analytics")
st.caption("Live Uniswap V3 swap analytics from Arbitrum using Alchemy, Kafka, Spark, DuckDB, and Parquet")

summary_df = read_parquet(PIPELINE_SUMMARY_PATH)
swaps_per_minute_df = read_parquet(SWAPS_PER_MINUTE_PATH)
top_pools_df = read_parquet(TOP_POOLS_PATH)
recent_swaps_df = read_parquet(RECENT_SWAPS_PATH)

summary = summary_df.iloc[0]

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Swaps", f"{summary['total_swaps']:,}")
col2.metric("Unique Transactions", f"{summary['unique_transactions']:,}")
col3.metric("Unique Pools", f"{summary['unique_pools']:,}")
col4.metric("Latest Block", f"{summary['latest_block_number']:,}")

st.divider()

st.subheader("Swaps per Minute")
st.line_chart(
    swaps_per_minute_df,
    x="minute_ts",
    y="swap_count",
)

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