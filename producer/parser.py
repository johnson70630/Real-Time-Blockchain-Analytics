from typing import Any

from config.settings import CHAIN, PROTOCOL
from producer.event_handlers import SwapEventHandler

UNISWAP_V3_SWAP_TOPIC = SwapEventHandler.topic


def parse_swap_log(
    log: dict[str, Any],
    chain: str = CHAIN,
    protocol: str = PROTOCOL,
    block_timestamp: str | None = None,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Compatibility wrapper around the Swap event handler."""
    return SwapEventHandler().build_event(
        log,
        block_timestamp,
        chain=chain,
        protocol=protocol,
        ingested_at=ingested_at,
    )
