import json

from producer import alchemy_client


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send(self, message: str) -> None:
        self.messages.append(message)

    def close(self) -> None:
        pass


def test_log_subscriptions_support_independent_address_filters(monkeypatch) -> None:
    websocket = FakeWebSocket()
    monkeypatch.setattr(
        alchemy_client,
        "create_connection",
        lambda _url, timeout: websocket,
    )
    client = alchemy_client.AlchemyClient("wss://example.invalid")

    uniswap_request_id = client.subscribe_logs(("0xswap",))
    aave_request_id = client.subscribe_logs(
        ("0xborrow", "0xrepay"),
        ("0x" + "10" * 20,),
    )

    uniswap_request, aave_request = map(json.loads, websocket.messages)
    assert (uniswap_request_id, aave_request_id) == (1, 2)
    assert "address" not in uniswap_request["params"][1]
    assert aave_request["params"][1]["address"] == "0x" + "10" * 20
    assert aave_request["params"][1]["topics"] == [
        ["0xborrow", "0xrepay"]
    ]
