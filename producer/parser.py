"""Compatibility exports for the legacy Uniswap V3 Swap parser import.

New code should import from :mod:`producer.protocols.uniswap_v3.parser`.
"""

from producer.protocols.uniswap_v3.parser import (
    UNISWAP_V3_SWAP_TOPIC,
    parse_swap_log,
)

__all__ = ["UNISWAP_V3_SWAP_TOPIC", "parse_swap_log"]
