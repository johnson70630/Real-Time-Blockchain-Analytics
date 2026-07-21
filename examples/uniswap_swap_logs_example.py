"""Print live Uniswap V3 Swap log identifiers from Arbitrum."""

import json
import logging
import os

from websocket import create_connection

from config.logging import configure_logging
from config.settings import get_alchemy_websocket_url
from producer.protocols.uniswap_v3.handlers import SwapEventHandler

logger = logging.getLogger(__name__)

DEFAULT_WETH_USDC_POOL = "0xC6962004f452bE9203591991D15f6b388e09E8D0"


def main() -> None:
    configure_logging()
    target_pool = os.getenv(
        "UNISWAP_V3_POOL_ADDRESS",
        DEFAULT_WETH_USDC_POOL,
    ).lower()
    websocket = create_connection(get_alchemy_websocket_url(), timeout=30)
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": [
            "logs",
            {
                "address": target_pool,
                "topics": [SwapEventHandler.topic],
            },
        ],
    }

    try:
        websocket.send(json.dumps(request))
        logger.info("Subscribed to Uniswap V3 Swap logs for: %s", target_pool)

        while True:
            message = json.loads(websocket.recv())
            if "error" in message:
                raise RuntimeError(
                    f"Alchemy subscription error: {message['error']}"
                )
            if message.get("method") != "eth_subscription":
                continue

            event_log = message["params"]["result"]
            logger.info(
                "block_number=%s transaction_hash=%s log_index=%s",
                int(event_log["blockNumber"], 16),
                event_log["transactionHash"],
                int(event_log["logIndex"], 16),
            )
    except KeyboardInterrupt:
        logger.info("Swap log stream stopped by user")
    finally:
        websocket.close()


if __name__ == "__main__":
    main()
