import json
from typing import Any

from websocket import WebSocket, create_connection


class AlchemyClient:
    """Simple WebSocket client for Alchemy JSON-RPC subscriptions."""

    def __init__(self, websocket_url: str, timeout: int = 30) -> None:
        self.websocket_url = websocket_url
        self.timeout = timeout
        self.ws: WebSocket = create_connection(websocket_url, timeout=timeout)

    def subscribe_logs(self, topics: list[str]) -> None:
        """Subscribe to Ethereum logs matching the given topic filters."""
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "logs",
                {
                    "topics": topics,
                },
            ],
        }

        self.ws.send(json.dumps(request))

    def receive(self) -> dict[str, Any]:
        """Receive and decode one WebSocket message."""
        return json.loads(self.ws.recv())

    def close(self) -> None:
        """Close the WebSocket connection."""
        self.ws.close()