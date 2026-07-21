"""Compatibility parser for normalized Uniswap V3 Swap events."""

from typing import Any

from config.settings import CHAIN
from producer.protocols.uniswap_v3.constants import PROTOCOL_NAME
from producer.protocols.uniswap_v3.handlers import SwapEventHandler

UNISWAP_V3_SWAP_TOPIC = SwapEventHandler.topic


def parse_swap_log(
    log: dict[str, Any],
    chain: str = CHAIN,
    protocol: str = PROTOCOL_NAME,
    block_timestamp: str | None = None,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Normalize a Swap log through the Uniswap V3 event handler."""
    return SwapEventHandler().build_event(
        log,
        block_timestamp,
        chain=chain,
        protocol=protocol,
        ingested_at=ingested_at,
    ).to_dict()
