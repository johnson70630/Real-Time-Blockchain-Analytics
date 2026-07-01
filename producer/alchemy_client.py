import json
from typing import Any

from websocket import create_connection


class AlchemyClient:
    def __init__(self, websocket_url: str) -> None:
        self.websocket_url = websocket_url
        self.ws = create_connection(websocket_url)

    def subscribe_logs(self, topics: list[str]) -> None:
        subscribe_request = {
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

        self.ws.send(json.dumps(subscribe_request))

    def receive(self) -> dict[str, Any]:
        return json.loads(self.ws.recv())

    def close(self) -> None:
        self.ws.close()