import json
from collections import OrderedDict, deque
from datetime import UTC, datetime
from itertools import count
from typing import Any

from websocket import WebSocket, create_connection


class AlchemyClient:
    """Simple WebSocket client for Alchemy JSON-RPC subscriptions."""

    def __init__(self, websocket_url: str, timeout: int = 30) -> None:
        self.websocket_url = websocket_url
        self.timeout = timeout
        self.ws: WebSocket = create_connection(websocket_url, timeout=timeout)
        self._request_ids = count(2)
        self._pending_messages: deque[dict[str, Any]] = deque()
        self._block_timestamps: OrderedDict[str, str] = OrderedDict()

    def subscribe_logs(self, topics: list[str]) -> None:
        """Subscribe to Ethereum logs matching the given topic filters."""
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "logs",
                {
                    "topics": [topics],
                },
            ],
        }

        self.ws.send(json.dumps(request))

    def receive(self) -> dict[str, Any]:
        """Receive and decode one WebSocket message."""
        if self._pending_messages:
            return self._pending_messages.popleft()
        return json.loads(self.ws.recv())

    def get_block_timestamp(self, block_number: str) -> str:
        """Fetch and cache an ISO-8601 timestamp for a block number."""
        cached_timestamp = self._block_timestamps.get(block_number)
        if cached_timestamp is not None:
            self._block_timestamps.move_to_end(block_number)
            return cached_timestamp

        request_id = next(self._request_ids)
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "eth_getBlockByNumber",
            "params": [block_number, False],
        }
        self.ws.send(json.dumps(request))

        while True:
            message = json.loads(self.ws.recv())
            if message.get("id") != request_id:
                self._pending_messages.append(message)
                continue

            if "error" in message:
                raise RuntimeError(
                    f"Alchemy block request failed: {message['error']}"
                )

            block = message.get("result")
            if not block or "timestamp" not in block:
                raise RuntimeError(
                    f"Alchemy returned no timestamp for block {block_number}"
                )

            timestamp = datetime.fromtimestamp(
                int(block["timestamp"], 16),
                tz=UTC,
            ).isoformat()
            self._block_timestamps[block_number] = timestamp
            self._block_timestamps.move_to_end(block_number)

            if len(self._block_timestamps) > 256:
                self._block_timestamps.popitem(last=False)

            return timestamp

    def close(self) -> None:
        """Close the WebSocket connection."""
        self.ws.close()
