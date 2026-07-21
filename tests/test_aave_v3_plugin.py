import json

import pytest
from web3 import Web3

from config.settings import get_enabled_protocols
from producer.dispatcher import EventDispatcher, EventParsingError
from producer.protocols.aave_v3 import AaveV3Plugin
from producer.protocols.aave_v3.handlers import (
    BorrowEventHandler,
    LiquidationCallEventHandler,
    RepayEventHandler,
)
from producer.registry import UnsupportedEventError, protocol_registry

CODEC = Web3().codec
POOL = Web3.to_checksum_address("0x" + "10" * 20)
RESERVE = Web3.to_checksum_address("0x" + "20" * 20)
USER = Web3.to_checksum_address("0x" + "30" * 20)
ON_BEHALF_OF = Web3.to_checksum_address("0x" + "40" * 20)
REPAYER = Web3.to_checksum_address("0x" + "50" * 20)
COLLATERAL_ASSET = Web3.to_checksum_address("0x" + "60" * 20)
DEBT_ASSET = Web3.to_checksum_address("0x" + "70" * 20)
LIQUIDATOR = Web3.to_checksum_address("0x" + "80" * 20)
LARGE_AMOUNT = 2**255 + 12345


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


def _borrow_log() -> dict:
    return _event_log(
        BorrowEventHandler(),
        [
            _encoded_topic("address", RESERVE),
            _encoded_topic("address", ON_BEHALF_OF),
            _encoded_topic("uint16", 7),
        ],
        ["address", "uint256", "uint8", "uint256"],
        [USER, LARGE_AMOUNT, 2, LARGE_AMOUNT - 1],
    )


def _repay_log() -> dict:
    return _event_log(
        RepayEventHandler(),
        [
            _encoded_topic("address", RESERVE),
            _encoded_topic("address", USER),
            _encoded_topic("address", REPAYER),
        ],
        ["uint256", "bool"],
        [LARGE_AMOUNT, True],
    )


def _liquidation_log() -> dict:
    return _event_log(
        LiquidationCallEventHandler(),
        [
            _encoded_topic("address", COLLATERAL_ASSET),
            _encoded_topic("address", DEBT_ASSET),
            _encoded_topic("address", USER),
        ],
        ["uint256", "uint256", "address", "bool"],
        [LARGE_AMOUNT, LARGE_AMOUNT - 1, LIQUIDATOR, False],
    )


def test_borrow_event_decoding_preserves_raw_amounts() -> None:
    payload = BorrowEventHandler().decode_payload(_borrow_log())

    assert payload["reserve"] == RESERVE.lower()
    assert payload["user"] == USER.lower()
    assert payload["on_behalf_of"] == ON_BEHALF_OF.lower()
    assert payload["amount_raw"] == str(LARGE_AMOUNT)
    assert payload["interest_rate_mode"] == 2
    assert payload["borrow_rate_raw"] == str(LARGE_AMOUNT - 1)
    assert payload["referral_code"] == 7


def test_repay_event_decoding() -> None:
    payload = RepayEventHandler().decode_payload(_repay_log())

    assert payload["reserve"] == RESERVE.lower()
    assert payload["user"] == USER.lower()
    assert payload["repayer"] == REPAYER.lower()
    assert payload["amount_raw"] == str(LARGE_AMOUNT)
    assert payload["use_atokens"] is True


def test_liquidation_event_decoding() -> None:
    payload = LiquidationCallEventHandler().decode_payload(_liquidation_log())

    assert payload["collateral_asset"] == COLLATERAL_ASSET.lower()
    assert payload["debt_asset"] == DEBT_ASSET.lower()
    assert payload["user"] == USER.lower()
    assert payload["debt_to_cover_raw"] == str(LARGE_AMOUNT)
    assert payload["liquidated_collateral_amount_raw"] == str(LARGE_AMOUNT - 1)
    assert payload["liquidator"] == LIQUIDATOR.lower()
    assert payload["receive_atoken"] is False


@pytest.mark.parametrize(
    ("raw_event", "event_type"),
    [
        (_borrow_log(), "borrow"),
        (_repay_log(), "repay"),
        (_liquidation_log(), "liquidation"),
    ],
)
def test_dispatcher_routes_aave_into_common_envelope(
    raw_event: dict,
    event_type: str,
) -> None:
    dispatcher = EventDispatcher(protocol_registry, ("aave_v3",))
    event = dispatcher.dispatch(
        raw_event,
        "2026-07-20T00:00:00+00:00",
        chain="arbitrum",
    )
    message = event.to_dict()

    assert message["protocol"] == "aave_v3"
    assert message["chain"] == "arbitrum"
    assert message["event_type"] == event_type
    assert message["block_number"] == 16
    assert message["log_index"] == 2
    assert message["ingested_at"]
    json.dumps(message)


def test_unknown_signature_has_defined_unsupported_result() -> None:
    dispatcher = EventDispatcher(protocol_registry, ("aave_v3",))
    unknown_event = {**_borrow_log(), "topics": ["0x" + "ff" * 32]}

    with pytest.raises(UnsupportedEventError, match="No registered"):
        dispatcher.dispatch(unknown_event, None, chain="arbitrum")


def test_malformed_supported_event_has_clear_parsing_error() -> None:
    dispatcher = EventDispatcher(protocol_registry, ("aave_v3",))
    malformed_event = {**_borrow_log(), "data": "0x01"}

    with pytest.raises(EventParsingError, match="Failed to parse aave_v3"):
        dispatcher.dispatch(malformed_event, None, chain="arbitrum")


def test_aave_address_is_validated_only_when_subscription_is_requested(
    monkeypatch,
) -> None:
    plugin = AaveV3Plugin()
    assert plugin.protocol == "aave_v3"

    monkeypatch.delenv("AAVE_V3_POOL_ADDRESS", raising=False)
    with pytest.raises(
        ValueError,
        match="Missing required environment variable: AAVE_V3_POOL_ADDRESS",
    ):
        _ = plugin.subscriptions

    monkeypatch.setenv("AAVE_V3_POOL_ADDRESS", POOL)
    assert plugin.subscriptions[0].addresses == (POOL,)


@pytest.mark.parametrize(
    ("enabled", "expected"),
    [
        ("uniswap_v3", ("uniswap_v3",)),
        ("aave_v3", ("aave_v3",)),
        ("uniswap_v3,aave_v3", ("uniswap_v3", "aave_v3")),
    ],
)
def test_enabled_protocol_configuration(
    monkeypatch,
    enabled: str,
    expected: tuple[str, ...],
) -> None:
    monkeypatch.setenv("ENABLED_PROTOCOLS", enabled)
    assert get_enabled_protocols() == expected
