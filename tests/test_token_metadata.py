from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from web3 import Web3

from reference_data.token_metadata import (
    ERC20_CALL_DATA,
    Erc20MetadataRpcClient,
    RawTokenMetadata,
    TokenMetadata,
    bootstrap_token_metadata,
    decode_erc20_decimals,
    decode_erc20_text,
    discover_token_addresses,
    merge_token_records,
)

CODEC = Web3().codec
TOKEN = Web3.to_checksum_address("0x" + "10" * 20)
SECOND_TOKEN = Web3.to_checksum_address("0x" + "20" * 20)
PROCESSED_AT = datetime(2026, 10, 2, tzinfo=UTC)


def _record(**changes) -> TokenMetadata:
    values = {
        "chain": "arbitrum",
        "token_address": TOKEN,
        "symbol": "TEST",
        "name": "Test Token",
        "decimals": 18,
        "metadata_source": "erc20_call",
        "metadata_block_number": 100,
        "ingested_at": PROCESSED_AT,
        "processed_at": PROCESSED_AT,
    }
    values.update(changes)
    return TokenMetadata(**values)


def test_symbol_and_name_decode_standard_abi_strings() -> None:
    assert decode_erc20_text(CODEC.encode(["string"], ["USDC"]), "symbol") == (
        "USDC"
    )
    assert decode_erc20_text(
        CODEC.encode(["string"], ["USD Coin"]),
        "name",
    ) == "USD Coin"


def test_text_decoder_supports_deterministic_bytes32_metadata() -> None:
    encoded = b"USDT" + b"\x00" * 28

    assert decode_erc20_text(encoded, "symbol") == "USDT"


def test_decimals_decoding_preserves_unsigned_integer() -> None:
    assert decode_erc20_decimals((6).to_bytes(32, "big")) == 6


@pytest.mark.parametrize(
    "value",
    [b"", b"\x00" * 31, (256).to_bytes(32, "big")],
)
def test_invalid_decimals_are_rejected(value: bytes) -> None:
    with pytest.raises(ValueError, match="decimals"):
        decode_erc20_decimals(value)


def test_token_address_is_normalized_to_lowercase() -> None:
    assert _record().token_address == TOKEN.lower()


def test_discovery_deduplicates_protocol_values_and_reports_overlap() -> None:
    discovery = discover_token_addresses(
        [TOKEN, TOKEN.lower(), SECOND_TOKEN],
        [TOKEN.lower(), TOKEN],
    )

    assert discovery.uniswap_addresses == (
        TOKEN.lower(),
        SECOND_TOKEN.lower(),
    )
    assert discovery.aave_addresses == (TOKEN.lower(),)
    assert discovery.all_addresses == (
        TOKEN.lower(),
        SECOND_TOKEN.lower(),
    )
    assert discovery.overlap == (TOKEN.lower(),)


def test_discovery_separates_native_and_invalid_values() -> None:
    discovery = discover_token_addresses(
        [None, "0x" + "ee" * 20],
        ["not-an-address"],
    )

    assert discovery.all_addresses == ()
    assert discovery.native_assets == (
        "uniswap_v3:0x" + "ee" * 20,
        "uniswap_v3:null",
    )
    assert discovery.invalid_addresses == (
        ("not-an-address", "token_address must be a valid EVM address"),
    )


def test_malformed_text_result_is_rejected() -> None:
    with pytest.raises(ValueError, match="symbol result is malformed"):
        decode_erc20_text(b"malformed", "symbol")


class FakeEth:
    def __init__(self, *, fail_selector: str | None = None) -> None:
        self.calls = []
        self.fail_selector = fail_selector

    def call(self, request, *, block_identifier: int):
        self.calls.append((request, block_identifier))
        selector = request["data"]
        if selector == self.fail_selector:
            raise RuntimeError("provider details must not escape")
        responses = {
            ERC20_CALL_DATA["symbol"]: CODEC.encode(["string"], ["TEST"]),
            ERC20_CALL_DATA["name"]: CODEC.encode(
                ["string"],
                ["Test Token"],
            ),
            ERC20_CALL_DATA["decimals"]: (18).to_bytes(32, "big"),
        }
        return responses[selector]


def _client(eth: FakeEth) -> Erc20MetadataRpcClient:
    client = object.__new__(Erc20MetadataRpcClient)
    client.web3 = SimpleNamespace(eth=eth)
    return client


def test_rpc_decodes_metadata_and_pins_every_call_to_one_block() -> None:
    eth = FakeEth()

    metadata = _client(eth).get_token_metadata(TOKEN, 12345)

    assert metadata == RawTokenMetadata("TEST", "Test Token", 18)
    assert [block for _request, block in eth.calls] == [12345] * 3
    assert all(call[0]["to"] == TOKEN for call in eth.calls)


def test_reverted_metadata_call_has_safe_method_specific_reason() -> None:
    eth = FakeEth(fail_selector=ERC20_CALL_DATA["symbol"])

    with pytest.raises(RuntimeError, match="symbol eth_call failed"):
        _client(eth).get_token_metadata(TOKEN, 12345)


class FakeRpc:
    def __init__(
        self,
        responses: dict[str, RawTokenMetadata | Exception],
    ) -> None:
        self.responses = {key.lower(): value for key, value in responses.items()}
        self.calls = []

    def get_latest_block_number(self) -> int:
        return 999

    def get_token_metadata(
        self,
        token_address: str,
        block_number: int,
    ) -> RawTokenMetadata:
        self.calls.append((token_address, block_number))
        response = self.responses[token_address]
        if isinstance(response, Exception):
            raise response
        return response


def test_bootstrap_quarantines_failure_without_stopping_other_tokens() -> None:
    rpc = FakeRpc(
        {
            TOKEN: RawTokenMetadata("TEST", "Test Token", 18),
            SECOND_TOKEN: RuntimeError("decimals eth_call failed"),
        }
    )

    result = bootstrap_token_metadata(
        rpc,
        [SECOND_TOKEN, TOKEN, TOKEN.lower()],
        chain="arbitrum",
        clock=lambda: PROCESSED_AT,
    )

    assert len(result.records) == 1
    assert result.records[0].token_address == TOKEN.lower()
    assert result.issues[0].token_address == SECOND_TOKEN.lower()
    assert result.issues[0].reason == "decimals eth_call failed"
    assert all(block == 999 for _address, block in rpc.calls)


def test_idempotent_rerun_preserves_existing_registry_record() -> None:
    existing = _record()
    rerun = _record(
        metadata_block_number=200,
        ingested_at=PROCESSED_AT + timedelta(hours=1),
        processed_at=PROCESSED_AT + timedelta(hours=1),
    )

    assert merge_token_records((existing,), (rerun,)) == (existing,)


def test_canonical_registry_rejects_duplicate_definition_conflicts() -> None:
    with pytest.raises(ValueError, match="Conflicting metadata"):
        merge_token_records((_record(),), (_record(decimals=6),))
