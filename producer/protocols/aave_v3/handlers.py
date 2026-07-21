"""Event decoders for supported Aave V3 Pool logs."""

from typing import Any

from web3 import Web3
from web3._utils.events import get_event_data

from producer.protocols.evm import (
    EvmEventHandler,
    event_topic,
    first_log_topic,
    raw_log_payload,
)
from producer.protocols.aave_v3.abi import (
    BORROW_EVENT_ABI,
    LIQUIDATION_CALL_EVENT_ABI,
    REPAY_EVENT_ABI,
)
from producer.protocols.aave_v3.constants import PROTOCOL_NAME

_CODEC = Web3().codec


class AaveV3EventHandler(EvmEventHandler):
    """Decode one Aave V3 event into the shared event model."""

    protocol = PROTOCOL_NAME


class BorrowEventHandler(AaveV3EventHandler):
    event_type = "borrow"
    topic = event_topic(
        "Borrow(address,address,address,uint256,uint8,uint256,uint16)"
    )
    event_abi = BORROW_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **raw_log_payload(log, address_field="contract_address"),
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
    topic = event_topic("Repay(address,address,address,uint256,bool)")
    event_abi = REPAY_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **raw_log_payload(log, address_field="contract_address"),
            "reserve": args["reserve"].lower(),
            "user": args["user"].lower(),
            "repayer": args["repayer"].lower(),
            "amount_raw": str(args["amount"]),
            "use_atokens": args["useATokens"],
        }


class LiquidationCallEventHandler(AaveV3EventHandler):
    event_type = "liquidation"
    topic = event_topic(
        "LiquidationCall(address,address,address,uint256,uint256,address,bool)"
    )
    event_abi = LIQUIDATION_CALL_EVENT_ABI

    def decode_payload(self, log: dict[str, Any]) -> dict[str, Any]:
        args = get_event_data(_CODEC, self.event_abi, log)["args"]
        return {
            **raw_log_payload(log, address_field="contract_address"),
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

    topic = first_log_topic(log)
    if topic is None:
        raise ValueError("Cannot select an Aave V3 handler: invalid topic zero")
    try:
        return EVENT_HANDLERS_BY_TOPIC[topic]
    except KeyError as error:
        raise ValueError(f"Unsupported Aave V3 event topic: {topic}") from error
