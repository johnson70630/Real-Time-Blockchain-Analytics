"""Print live Arbitrum block headers from an Alchemy WebSocket connection."""

import json
import logging
from datetime import UTC, datetime

from websocket import create_connection

from config.logging import configure_logging
from config.settings import get_alchemy_websocket_url

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    websocket = create_connection(get_alchemy_websocket_url(), timeout=30)
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["newHeads"],
    }

    try:
        websocket.send(json.dumps(request))
        logger.info("Subscribed to Arbitrum block headers")

        while True:
            message = json.loads(websocket.recv())
            if message.get("method") != "eth_subscription":
                continue

            block = message["params"]["result"]
            timestamp = datetime.fromtimestamp(
                int(block["timestamp"], 16),
                tz=UTC,
            )
            logger.info(
                "block_number=%s timestamp=%s hash=%s",
                int(block["number"], 16),
                timestamp.isoformat(),
                block["hash"],
            )
    except KeyboardInterrupt:
        logger.info("Block stream stopped by user")
    finally:
        websocket.close()


if __name__ == "__main__":
    main()
