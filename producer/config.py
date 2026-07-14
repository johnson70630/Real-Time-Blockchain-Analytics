"""Compatibility wrapper for the shared project settings."""

from config.settings import (
    CHAIN,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PROJECT_ROOT,
    PROTOCOL,
    get_alchemy_websocket_url,
    get_required_env,
)

__all__ = [
    "CHAIN",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "PROJECT_ROOT",
    "PROTOCOL",
    "get_alchemy_websocket_url",
    "get_required_env",
]
