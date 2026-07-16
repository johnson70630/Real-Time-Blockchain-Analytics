"""Central settings for the local blockchain analytics pipeline."""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def _project_path(env_name: str, default: str | Path) -> Path:
    """Return an absolute path, resolving relative values from the project root."""
    path = Path(os.getenv(env_name, str(default))).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def get_required_env(name: str) -> str:
    """Read a required environment variable or raise a clear error."""
    value = os.getenv(name)

    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def get_alchemy_websocket_url() -> str:
    """Return the Alchemy WebSocket URL required when starting the producer."""
    return get_required_env("ALCHEMY_WEBSOCKET_URL")


PROTOCOL = os.getenv("PROTOCOL", "uniswap_v3")
CHAIN = os.getenv("CHAIN", "arbitrum")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "uniswap_v3_swaps")

BRONZE_OUTPUT_PATH = _project_path("BRONZE_OUTPUT_PATH", "data/bronze/swaps")
BRONZE_PARQUET_GLOB = BRONZE_OUTPUT_PATH / "**" / "*.parquet"

SILVER_DIR = _project_path("SILVER_DIR", "data/silver/swaps")
SILVER_OUTPUT_FILE = _project_path(
    "SILVER_OUTPUT_FILE",
    SILVER_DIR / "swaps_silver.parquet",
)
SILVER_PARQUET_GLOB = SILVER_DIR / "*.parquet"

STATE_DIR = _project_path("STATE_DIR", "data/state")
SILVER_PROCESSED_FILES_MANIFEST = _project_path(
    "SILVER_PROCESSED_FILES_MANIFEST",
    STATE_DIR / "silver_processed_files.json",
)

GOLD_DIR = _project_path("GOLD_DIR", "data/gold")
GOLD_SWAPS_PER_MINUTE_FILE = _project_path(
    "GOLD_SWAPS_PER_MINUTE_FILE",
    GOLD_DIR / "swaps_per_minute" / "swaps_per_minute.parquet",
)
GOLD_TOP_POOLS_FILE = _project_path(
    "GOLD_TOP_POOLS_FILE",
    GOLD_DIR / "top_pools" / "top_pools.parquet",
)
GOLD_PIPELINE_SUMMARY_FILE = _project_path(
    "GOLD_PIPELINE_SUMMARY_FILE",
    GOLD_DIR / "pipeline_summary" / "pipeline_summary.parquet",
)
GOLD_RECENT_SWAPS_FILE = _project_path(
    "GOLD_RECENT_SWAPS_FILE",
    GOLD_DIR / "recent_swaps" / "recent_swaps.parquet",
)
GOLD_SWAPS_PER_MINUTE_GLOB = GOLD_SWAPS_PER_MINUTE_FILE.parent / "*.parquet"
GOLD_TOP_POOLS_GLOB = GOLD_TOP_POOLS_FILE.parent / "*.parquet"
GOLD_PIPELINE_SUMMARY_GLOB = GOLD_PIPELINE_SUMMARY_FILE.parent / "*.parquet"
GOLD_RECENT_SWAPS_GLOB = GOLD_RECENT_SWAPS_FILE.parent / "*.parquet"

SPARK_CHECKPOINT_PATH = _project_path(
    "SPARK_CHECKPOINT_PATH",
    "data/checkpoints/swaps_bronze",
)
SPARK_KAFKA_CONNECTOR_PACKAGE = os.getenv(
    "SPARK_KAFKA_CONNECTOR_PACKAGE",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
)
