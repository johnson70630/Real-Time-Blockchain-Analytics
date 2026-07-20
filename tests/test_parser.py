from config.settings import CHAIN, PROTOCOL
from producer.event_handlers import SwapEventHandler
from producer.parser import parse_swap_log


def test_parse_swap_log_uses_common_envelope(monkeypatch) -> None:
    log = {
        "blockNumber": "0x10",
        "transactionHash": "0xtransaction",
        "address": "0xABCDEF",
        "logIndex": "0x2",
        "data": "0xdata",
        "topics": ["0xtopic"],
    }
    monkeypatch.setattr(
        SwapEventHandler,
        "decode_payload",
        lambda _handler, _log: {"pool_address": "0xabcdef"},
    )

    event = parse_swap_log(
        log,
        block_timestamp="2026-07-20T00:00:00+00:00",
        ingested_at="2026-07-20T00:00:01+00:00",
    )

    assert event["protocol"] == PROTOCOL
    assert event["chain"] == CHAIN
    assert event["event_type"] == "swap"
    assert event["payload"] == {"pool_address": "0xabcdef"}
