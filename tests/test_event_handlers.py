import json

import pytest
from web3 import Web3

from producer.event_handlers import (
    BurnEventHandler,
    MintEventHandler,
    SwapEventHandler,
    get_event_handler,
)

CODEC = Web3().codec
POOL = Web3.to_checksum_address("0x" + "10" * 20)
SENDER = Web3.to_checksum_address("0x" + "20" * 20)
RECIPIENT = Web3.to_checksum_address("0x" + "30" * 20)
OWNER = Web3.to_checksum_address("0x" + "40" * 20)


def _encoded_topic(abi_type: str, value: object) -> str:
    return "0x" + CODEC.encode([abi_type], [value]).hex()


def _event_log(
    handler,
    indexed_topics: list[str],
    data_types: list[str],
    data_values: list[object],
) -> dict:
    return {
        "address": POOL,
        "topics": [handler.topic, *indexed_topics],
        "data": "0x" + CODEC.encode(data_types, data_values).hex(),
        "blockNumber": "0x10",
        "transactionHash": "0x" + "ab" * 32,
        "transactionIndex": "0x0",
        "blockHash": "0x" + "cd" * 32,
        "logIndex": "0x2",
        "removed": False,
    }


def _swap_log() -> dict:
    return _event_log(
        SwapEventHandler(),
        [
            _encoded_topic("address", SENDER),
            _encoded_topic("address", RECIPIENT),
        ],
        ["int256", "int256", "uint160", "uint128", "int24"],
        [-10, 20, 2**96, 1_000_000, -120],
    )


def _mint_log() -> dict:
    return _event_log(
        MintEventHandler(),
        [
            _encoded_topic("address", OWNER),
            _encoded_topic("int24", -240),
            _encoded_topic("int24", 240),
        ],
        ["address", "uint128", "uint256", "uint256"],
        [SENDER, 500, 1000, 2000],
    )


def _burn_log() -> dict:
    return _event_log(
        BurnEventHandler(),
        [
            _encoded_topic("address", OWNER),
            _encoded_topic("int24", -120),
            _encoded_topic("int24", 120),
        ],
        ["uint128", "uint256", "uint256"],
        [250, 400, 800],
    )


def test_swap_handler_decodes_event() -> None:
    payload = SwapEventHandler().decode_payload(_swap_log())

    assert payload["sender"] == SENDER.lower()
    assert payload["recipient"] == RECIPIENT.lower()
    assert payload["amount0"] == "-10"
    assert payload["amount1"] == "20"
    assert payload["tick"] == -120


def test_mint_handler_decodes_event() -> None:
    payload = MintEventHandler().decode_payload(_mint_log())

    assert payload["sender"] == SENDER.lower()
    assert payload["owner"] == OWNER.lower()
    assert payload["tick_lower"] == -240
    assert payload["tick_upper"] == 240
    assert payload["amount"] == "500"


def test_burn_handler_decodes_event() -> None:
    payload = BurnEventHandler().decode_payload(_burn_log())

    assert payload["owner"] == OWNER.lower()
    assert payload["tick_lower"] == -120
    assert payload["tick_upper"] == 120
    assert payload["amount0"] == "400"
    assert payload["amount1"] == "800"


@pytest.mark.parametrize(
    ("log", "handler_type"),
    [
        (_swap_log(), SwapEventHandler),
        (_mint_log(), MintEventHandler),
        (_burn_log(), BurnEventHandler),
    ],
)
def test_handler_selection(log: dict, handler_type: type) -> None:
    assert isinstance(get_event_handler(log), handler_type)


@pytest.mark.parametrize(
    ("handler", "log", "event_type"),
    [
        (SwapEventHandler(), _swap_log(), "swap"),
        (MintEventHandler(), _mint_log(), "mint"),
        (BurnEventHandler(), _burn_log(), "burn"),
    ],
)
def test_common_envelope_and_event_type(handler, log: dict, event_type: str) -> None:
    event = handler.build_event(
        log,
        "2026-07-20T00:00:00+00:00",
        ingested_at="2026-07-20T00:00:01+00:00",
    )

    assert set(event) == {
        "protocol",
        "chain",
        "event_type",
        "block_number",
        "transaction_hash",
        "log_index",
        "block_timestamp",
        "ingested_at",
        "payload",
    }
    assert event["event_type"] == event_type
    assert event["block_number"] == 16
    assert event["log_index"] == 2
    assert event["payload"]["pool_address"] == POOL.lower()
    json.dumps(event)
