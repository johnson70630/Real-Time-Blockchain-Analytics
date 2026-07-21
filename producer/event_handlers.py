"""Compatibility exports for the Uniswap V3 plugin event handlers."""

from producer.protocols.uniswap_v3.abi import (
    BURN_EVENT_ABI,
    MINT_EVENT_ABI,
    SWAP_EVENT_ABI,
)
from producer.protocols.uniswap_v3.handlers import (
    EVENT_HANDLERS,
    EVENT_HANDLERS_BY_TOPIC,
    SUPPORTED_EVENT_TOPICS,
    BurnEventHandler,
    MintEventHandler,
    SwapEventHandler,
    UniswapV3EventHandler,
    get_event_handler,
)

__all__ = [
    "BURN_EVENT_ABI",
    "EVENT_HANDLERS",
    "EVENT_HANDLERS_BY_TOPIC",
    "MINT_EVENT_ABI",
    "SUPPORTED_EVENT_TOPICS",
    "SWAP_EVENT_ABI",
    "BurnEventHandler",
    "MintEventHandler",
    "SwapEventHandler",
    "UniswapV3EventHandler",
    "get_event_handler",
]
