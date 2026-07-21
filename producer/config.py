"""Compatibility wrapper for legacy imports.

New code should import configuration from :mod:`config.settings` directly.
"""

from config.settings import (
    CHAIN,
    ENABLED_PROTOCOLS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PROJECT_ROOT,
    PROTOCOL,
    get_aave_v3_pool_address,
    get_alchemy_websocket_url,
    get_enabled_protocols,
    get_required_env,
)

__all__ = [
    "CHAIN",
    "ENABLED_PROTOCOLS",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "PROJECT_ROOT",
    "PROTOCOL",
    "get_aave_v3_pool_address",
    "get_alchemy_websocket_url",
    "get_enabled_protocols",
    "get_required_env",
]
