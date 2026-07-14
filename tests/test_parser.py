from config.settings import CHAIN, PROTOCOL
from producer.parser import parse_swap_log


def test_parse_swap_log_includes_partition_dimensions() -> None:
    log = {
        "blockNumber": "0x10",
        "transactionHash": "0xtransaction",
        "address": "0xABCDEF",
        "logIndex": "0x2",
        "data": "0xdata",
        "topics": ["0xtopic"],
    }

    event = parse_swap_log(log)

    assert event["protocol"] == PROTOCOL
    assert event["chain"] == CHAIN
    assert event["event_type"] == "swap"
