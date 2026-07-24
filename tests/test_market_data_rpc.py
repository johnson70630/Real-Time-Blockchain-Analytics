from datetime import UTC, datetime

import pytest

from market_data.rpc import ChainlinkRpcClient

FEED_ADDRESS = "0x" + "10" * 20
ROUND_ID = 2**79 + 123
ANSWER_RAW = -(2**200) + 456
UPDATED_AT = 1_784_808_000


class FakeContractCall:
    def __init__(self, response) -> None:
        self.response = response
        self.block_identifier = None

    def call(self, *, block_identifier=None):
        self.block_identifier = block_identifier
        return self.response


class FakeFunctions:
    def __init__(self, round_response) -> None:
        self.decimals_call = FakeContractCall(8)
        self.round_call = FakeContractCall(round_response)

    def decimals(self) -> FakeContractCall:
        return self.decimals_call

    def latestRoundData(self) -> FakeContractCall:
        return self.round_call


class FakeEth:
    def __init__(self, round_response) -> None:
        self.functions = FakeFunctions(round_response)

    def contract(self, **_kwargs):
        return type("Contract", (), {"functions": self.functions})()

    def get_block(self, _identifier):
        return {"number": 123456, "timestamp": UPDATED_AT}


class FakeWeb3:
    def __init__(self, round_response) -> None:
        self.eth = FakeEth(round_response)

    def is_connected(self) -> bool:
        return True


def _client(round_response) -> ChainlinkRpcClient:
    client = object.__new__(ChainlinkRpcClient)
    client.web3 = FakeWeb3(round_response)
    return client


def test_rpc_decodes_exact_round_values_at_pinned_block() -> None:
    client = _client((ROUND_ID, ANSWER_RAW, 1, UPDATED_AT, ROUND_ID))

    block = client.get_latest_block()
    decimals = client.get_feed_decimals(FEED_ADDRESS)
    round_data = client.get_latest_round(FEED_ADDRESS, block.number)

    assert decimals == 8
    assert block.number == 123456
    assert block.timestamp == datetime.fromtimestamp(
        UPDATED_AT,
        tz=UTC,
    ).isoformat()
    assert round_data.round_id == ROUND_ID
    assert round_data.answer_raw == ANSWER_RAW
    assert round_data.updated_at == block.timestamp
    assert client.web3.eth.functions.round_call.block_identifier == 123456


def test_rpc_rejects_malformed_round_timestamp() -> None:
    client = _client((ROUND_ID, ANSWER_RAW, 1, 0, ROUND_ID))

    with pytest.raises(ValueError, match="updated_at must be positive"):
        client.get_latest_round(FEED_ADDRESS, 123456)
