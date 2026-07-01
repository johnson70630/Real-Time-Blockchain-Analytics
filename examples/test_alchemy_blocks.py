import json
import os
from datetime import UTC, datetime

from dotenv import load_dotenv
from websocket import create_connection

load_dotenv()

ALCHEMY_WEBSOCKET_URL = os.getenv("ALCHEMY_WEBSOCKET_URL")

if not ALCHEMY_WEBSOCKET_URL:
    raise ValueError("ALCHEMY_WEBSOCKET_URL is missing from .env")

ws = create_connection(ALCHEMY_WEBSOCKET_URL)

subscribe_request = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "eth_subscribe",
    "params": ["newHeads"],
}

ws.send(json.dumps(subscribe_request))
print("Subscribed to Arbitrum new blocks...")

while True:
    message = json.loads(ws.recv())

    if message.get("method") == "eth_subscription":
        block = message["params"]["result"]

        block_number = int(block["number"], 16)
        block_hash = block["hash"]
        timestamp = datetime.fromtimestamp(int(block["timestamp"], 16), UTC)

        print(
            f"block_number={block_number} "
            f"timestamp={timestamp.isoformat()} "
            f"hash={block_hash}"
        )