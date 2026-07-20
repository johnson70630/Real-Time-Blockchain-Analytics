from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, ClassVar

from web3 import Web3
from web3._utils.events import get_event_data

from config.settings import CHAIN, PROTOCOL

SWAP_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "sender", "type": "address"},
        {"indexed": True, "name": "recipient", "type": "address"},
        {"indexed": False, "name": "amount0", "type": "int256"},
        {"indexed": False, "name": "amount1", "type": "int256"},
        {"indexed": False, "name": "sqrtPriceX96", "type": "uint160"},
        {"indexed": False, "name": "liquidity", "type": "uint128"},
        {"indexed": False, "name": "tick", "type": "int24"},
    ],
    "name": "Swap",
    "type": "event",
}

MINT_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": False, "name": "sender", "type": "address"},
        {"indexed": True, "name": "owner", "type": "address"},
        {"indexed": True, "name": "tickLower", "type": "int24"},
        {"indexed": True, "name": "tickUpper", "type": "int24"},
        {"indexed": False, "name": "amount", "type": "uint128"},
        {"indexed": False, "name": "amount0", "type": "uint256"},
        {"indexed": False, "name": "amount1", "type": "uint256"},
    ],
    "name": "Mint",
    "type": "event",
}

BURN_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "owner", "type": "address"},
        {"indexed": True, "name": "tickLower", "type": "int24"},
        {"indexed": True, "name": "tickUpper", "type": "int24"},
        {"indexed": False, "name": "amount", "type": "uint128"},
        {"indexed": False, "name": "amount0", "type": "uint256"},
        {"indexed": False, "name": "amount1", "type": "uint256"},
    ],
    "name": "Burn",
    "type": "event",
}

_CODEC = Web3().codec


def _event_topic(signature: str) -> str:
    return "0x" + Web3.keccak(text=signature).hex()


def _hex_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    hex_value = value.hex()
    return hex_value if hex_value.startswith("0x") else "0x" + hex_value


def _hex_int(value: int | str) -> int:
    return int(value, 16) if isinstance(value, str) else value


def _common_payload(log: dict[str, Any]) -> dict[str, Any]:
    return {
        "pool_address": log["address"].lower(),
        "raw_data": _hex_string(log["data"]),
        "raw_topics": [_hex_string(topic) for topic in log["topics"]],
    }


class UniswapV3EventHandler(ABC):
    """Decode one Uniswap V3 event into the shared Kafka envelope."""

    event_type: ClassVar[str]
    topic: ClassVar[str]
    event_abi: ClassVar[dict[str, Any]]

    @abstractmethod
    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        """Decode handler-specific event arguments."""

    def build_event(
        self,
        log: dict[str, Any],
        block_timestamp: str | None,
        *,
        chain: str = CHAIN,
        protocol: str = PROTOCOL,
        ingested_at: str | None = None,
    ) -> dict[str, Any]:
        """Build the common event envelope around a decoded payload."""
        return {
            "protocol": protocol,
            "chain": chain,
            "event_type": self.event_type,
            "block_number": _hex_int(log["blockNumber"]),
            "transaction_hash": _hex_string(log["transactionHash"]),
            "log_index": _hex_int(log["logIndex"]),
            "block_timestamp": block_timestamp,
            "ingested_at": ingested_at or datetime.now(UTC).isoformat(),
            "payload": self.decode_payload(log),
        }


class SwapEventHandler(UniswapV3EventHandler):
    event_type = "swap"
    topic = _event_topic(
        "Swap(address,address,int256,int256,uint160,uint128,int24)"
    )
    event_abi = SWAP_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **_common_payload(log),
            "sender": args["sender"].lower(),
            "recipient": args["recipient"].lower(),
            "amount0": str(args["amount0"]),
            "amount1": str(args["amount1"]),
            "sqrt_price_x96": str(args["sqrtPriceX96"]),
            "liquidity": str(args["liquidity"]),
            "tick": args["tick"],
        }


class MintEventHandler(UniswapV3EventHandler):
    event_type = "mint"
    topic = _event_topic(
        "Mint(address,address,int24,int24,uint128,uint256,uint256)"
    )
    event_abi = MINT_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **_common_payload(log),
            "sender": args["sender"].lower(),
            "owner": args["owner"].lower(),
            "tick_lower": args["tickLower"],
            "tick_upper": args["tickUpper"],
            "amount": str(args["amount"]),
            "amount0": str(args["amount0"]),
            "amount1": str(args["amount1"]),
        }


class BurnEventHandler(UniswapV3EventHandler):
    event_type = "burn"
    topic = _event_topic(
        "Burn(address,int24,int24,uint128,uint256,uint256)"
    )
    event_abi = BURN_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **_common_payload(log),
            "owner": args["owner"].lower(),
            "tick_lower": args["tickLower"],
            "tick_upper": args["tickUpper"],
            "amount": str(args["amount"]),
            "amount0": str(args["amount0"]),
            "amount1": str(args["amount1"]),
        }


EVENT_HANDLERS = (
    SwapEventHandler(),
    MintEventHandler(),
    BurnEventHandler(),
)
EVENT_HANDLERS_BY_TOPIC = {
    handler.topic.lower(): handler for handler in EVENT_HANDLERS
}
SUPPORTED_EVENT_TOPICS = list(EVENT_HANDLERS_BY_TOPIC)


def get_event_handler(log: dict[str, Any]) -> UniswapV3EventHandler:
    """Select an event handler using the signature in topic zero."""
    topics = log.get("topics") or []
    if not topics:
        raise ValueError("Cannot select an event handler: log has no topics")

    topic = _hex_string(topics[0]).lower()
    try:
        return EVENT_HANDLERS_BY_TOPIC[topic]
    except KeyError as error:
        raise ValueError(f"Unsupported Uniswap V3 event topic: {topic}") from error
