import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def get_required_env(name: str) -> str:
    """Read a required environment variable or raise a clear error."""
    value = os.getenv(name)

    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def get_alchemy_websocket_url() -> str:
    """Read the Alchemy WebSocket URL required by the producer."""
    return get_required_env("ALCHEMY_WEBSOCKET_URL")


CHAIN = os.getenv("CHAIN", "arbitrum")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "uniswap_v3_swaps")