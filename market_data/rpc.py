"""Alchemy RPC access for Chainlink AggregatorV3Interface feeds."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from web3 import Web3

AGGREGATOR_V3_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {
                "internalType": "uint80",
                "name": "answeredInRound",
                "type": "uint80",
            },
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


@dataclass(frozen=True, slots=True)
class BlockSnapshot:
    """One real chain block shared by every query in a polling cycle."""

    number: int
    timestamp: str


@dataclass(frozen=True, slots=True)
class ChainlinkRound:
    """Exact values returned by AggregatorV3Interface.latestRoundData."""

    round_id: int
    answer_raw: int
    updated_at: str


class ChainlinkRpcClient:
    """Read configured Chainlink proxy contracts through HTTP JSON-RPC."""

    def __init__(self, rpc_url: str, timeout_seconds: float = 30) -> None:
        self.web3 = Web3(
            Web3.HTTPProvider(
                rpc_url,
                request_kwargs={"timeout": timeout_seconds},
            )
        )

    def is_connected(self) -> bool:
        """Return whether the configured RPC endpoint is reachable."""
        return self.web3.is_connected()

    def get_latest_block(self) -> BlockSnapshot:
        """Return the latest block number and UTC timestamp in one RPC read."""
        block: dict[str, Any] = self.web3.eth.get_block("latest")
        return BlockSnapshot(
            number=int(block["number"]),
            timestamp=datetime.fromtimestamp(
                int(block["timestamp"]),
                tz=UTC,
            ).isoformat(),
        )

    def get_feed_decimals(self, feed_address: str) -> int:
        """Read the feed decimals once for the service lifetime."""
        contract = self.web3.eth.contract(
            address=Web3.to_checksum_address(feed_address),
            abi=AGGREGATOR_V3_ABI,
        )
        return int(contract.functions.decimals().call())

    def get_latest_round(
        self,
        feed_address: str,
        block_number: int,
    ) -> ChainlinkRound:
        """Read latestRoundData at the cycle's pinned block."""
        contract = self.web3.eth.contract(
            address=Web3.to_checksum_address(feed_address),
            abi=AGGREGATOR_V3_ABI,
        )
        round_id, answer, _started_at, updated_at, _answered_in_round = (
            contract.functions.latestRoundData().call(
                block_identifier=block_number
            )
        )
        round_id = int(round_id)
        answer = int(answer)
        updated_at = int(updated_at)
        if round_id < 0:
            raise ValueError("Chainlink round_id must not be negative")
        if updated_at <= 0:
            raise ValueError("Chainlink updated_at must be positive")
        return ChainlinkRound(
            round_id=round_id,
            answer_raw=answer,
            updated_at=datetime.fromtimestamp(updated_at, tz=UTC).isoformat(),
        )
