"""Event decoders for supported Uniswap V3 pool logs."""

from typing import Any

from web3 import Web3
from web3._utils.events import get_event_data

from producer.protocols.evm import (
    EvmEventHandler,
    event_topic,
    first_log_topic,
    raw_log_payload,
)
from producer.protocols.uniswap_v3.abi import (
    BURN_EVENT_ABI,
    MINT_EVENT_ABI,
    SWAP_EVENT_ABI,
)
from producer.protocols.uniswap_v3.constants import PROTOCOL_NAME

_CODEC = Web3().codec


class UniswapV3EventHandler(EvmEventHandler):
    """Decode one Uniswap V3 event into the shared event model."""

    protocol = PROTOCOL_NAME


class SwapEventHandler(UniswapV3EventHandler):
    event_type = "swap"
    topic = event_topic(
        "Swap(address,address,int256,int256,uint160,uint128,int24)"
    )
    event_abi = SWAP_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **raw_log_payload(log, address_field="pool_address"),
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
    topic = event_topic(
        "Mint(address,address,int24,int24,uint128,uint256,uint256)"
    )
    event_abi = MINT_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **raw_log_payload(log, address_field="pool_address"),
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
    topic = event_topic(
        "Burn(address,int24,int24,uint128,uint256,uint256)"
    )
    event_abi = BURN_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **raw_log_payload(log, address_field="pool_address"),
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
SUPPORTED_EVENT_TOPICS = tuple(EVENT_HANDLERS_BY_TOPIC)


def get_event_handler(log: dict[str, Any]) -> UniswapV3EventHandler:
    """Select an event handler using the signature in topic zero."""
    topics = log.get("topics") or []
    if not topics:
        raise ValueError("Cannot select an event handler: log has no topics")

    topic = first_log_topic(log)
    if topic is None:
        raise ValueError("Cannot select an event handler: invalid topic zero")
    try:
        return EVENT_HANDLERS_BY_TOPIC[topic]
    except KeyError as error:
        raise ValueError(f"Unsupported Uniswap V3 event topic: {topic}") from error
