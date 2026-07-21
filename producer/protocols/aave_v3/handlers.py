"""Event decoders for supported Aave V3 Pool logs."""

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from web3 import Web3
from web3._utils.events import get_event_data

from config.settings import CHAIN
from producer.models import EventEnvelope
from producer.protocols.aave_v3.abi import (
    BORROW_EVENT_ABI,
    LIQUIDATION_CALL_EVENT_ABI,
    REPAY_EVENT_ABI,
)
from producer.protocols.aave_v3.constants import PROTOCOL_NAME

_CODEC = Web3().codec


def _event_topic(signature: str) -> str:
    return "0x" + Web3.keccak(text=signature).hex()


def _hex_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    hex_value = value.hex()
    return hex_value if hex_value.startswith("0x") else f"0x{hex_value}"


def _hex_int(value: int | str) -> int:
    return int(value, 16) if isinstance(value, str) else value


def _common_payload(log: dict[str, Any]) -> dict[str, Any]:
    return {
        "contract_address": log["address"].lower(),
        "raw_data": _hex_string(log["data"]),
        "raw_topics": [_hex_string(topic) for topic in log["topics"]],
    }


class AaveV3EventHandler(ABC):
    """Decode one Aave V3 event into the shared event model."""

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
        ingested_at: str | None = None,
    ) -> EventEnvelope:
        """Build the common event envelope around a decoded payload."""
        return EventEnvelope.create(
            protocol=PROTOCOL_NAME,
            chain=chain,
            event_type=self.event_type,
            block_number=_hex_int(log["blockNumber"]),
            transaction_hash=_hex_string(log["transactionHash"]),
            log_index=_hex_int(log["logIndex"]),
            block_timestamp=block_timestamp,
            ingested_at=ingested_at,
            payload=self.decode_payload(log),
        )


class BorrowEventHandler(AaveV3EventHandler):
    event_type = "borrow"
    topic = _event_topic(
        "Borrow(address,address,address,uint256,uint8,uint256,uint16)"
    )
    event_abi = BORROW_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **_common_payload(log),
            "reserve": args["reserve"].lower(),
            "user": args["user"].lower(),
            "on_behalf_of": args["onBehalfOf"].lower(),
            "amount_raw": str(args["amount"]),
            "interest_rate_mode": args["interestRateMode"],
            "borrow_rate_raw": str(args["borrowRate"]),
            "referral_code": args["referralCode"],
        }


class RepayEventHandler(AaveV3EventHandler):
    event_type = "repay"
    topic = _event_topic("Repay(address,address,address,uint256,bool)")
    event_abi = REPAY_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **_common_payload(log),
            "reserve": args["reserve"].lower(),
            "user": args["user"].lower(),
            "repayer": args["repayer"].lower(),
            "amount_raw": str(args["amount"]),
            "use_atokens": args["useATokens"],
        }


class LiquidationCallEventHandler(AaveV3EventHandler):
    event_type = "liquidation"
    topic = _event_topic(
        "LiquidationCall(address,address,address,uint256,uint256,address,bool)"
    )
    event_abi = LIQUIDATION_CALL_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **_common_payload(log),
            "collateral_asset": args["collateralAsset"].lower(),
            "debt_asset": args["debtAsset"].lower(),
            "user": args["user"].lower(),
            "debt_to_cover_raw": str(args["debtToCover"]),
            "liquidated_collateral_amount_raw": str(
                args["liquidatedCollateralAmount"]
            ),
            "liquidator": args["liquidator"].lower(),
            "receive_atoken": args["receiveAToken"],
        }


EVENT_HANDLERS = (
    BorrowEventHandler(),
    RepayEventHandler(),
    LiquidationCallEventHandler(),
)
EVENT_HANDLERS_BY_TOPIC = {
    handler.topic.lower(): handler for handler in EVENT_HANDLERS
}
SUPPORTED_EVENT_TOPICS = tuple(EVENT_HANDLERS_BY_TOPIC)


def get_event_handler(log: dict[str, Any]) -> AaveV3EventHandler:
    """Select an Aave handler using the signature in topic zero."""
    topics = log.get("topics") or []
    if not topics:
        raise ValueError("Cannot select an Aave V3 handler: log has no topics")

    topic = _hex_string(topics[0]).lower()
    try:
        return EVENT_HANDLERS_BY_TOPIC[topic]
    except KeyError as error:
        raise ValueError(f"Unsupported Aave V3 event topic: {topic}") from error
