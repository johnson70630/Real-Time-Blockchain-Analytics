"""Shared validation rules for raw market-data observation messages."""

import json
import re
from datetime import datetime
from typing import Any

REQUIRED_MARKET_DATA_FIELDS = (
    "observation_id",
    "protocol",
    "event_type",
    "chain",
    "feed_address",
    "base_asset",
    "quote_asset",
    "round_id",
    "answer_raw",
    "feed_decimals",
    "feed_updated_at",
    "observed_at",
    "block_number",
    "block_timestamp",
    "ingested_at",
    "producer_version",
    "schema_version",
)

ADDRESS_PATTERN = re.compile(r"^0x[0-9a-fA-F]{40}$")
UNSIGNED_INTEGER_PATTERN = re.compile(r"^[0-9]+$")
SIGNED_INTEGER_PATTERN = re.compile(r"^-?[0-9]+$")


def _is_non_empty(value: Any) -> bool:
    return value is not None and (
        not isinstance(value, str) or bool(value.strip())
    )


def _is_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def validate_observation_message(
    raw_value: str,
) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
    """Parse one Kafka value and return deterministic quarantine reasons."""
    try:
        observation = json.loads(raw_value)
    except json.JSONDecodeError:
        return None, ("invalid_json",)
    if not isinstance(observation, dict):
        return None, ("invalid_json_object",)

    errors = [
        f"missing_or_empty:{field}"
        for field in REQUIRED_MARKET_DATA_FIELDS
        if not _is_non_empty(observation.get(field))
    ]
    if observation.get("protocol") != "chainlink":
        errors.append("invalid:protocol")
    if observation.get("event_type") != "price_update":
        errors.append("invalid:event_type")

    address = observation.get("feed_address")
    if isinstance(address, str) and not ADDRESS_PATTERN.fullmatch(address):
        errors.append("invalid:feed_address")

    round_id = observation.get("round_id")
    if isinstance(round_id, str) and not UNSIGNED_INTEGER_PATTERN.fullmatch(
        round_id
    ):
        errors.append("invalid:round_id")

    answer_raw = observation.get("answer_raw")
    if isinstance(answer_raw, str) and not SIGNED_INTEGER_PATTERN.fullmatch(
        answer_raw
    ):
        errors.append("invalid:answer_raw")

    decimals = observation.get("feed_decimals")
    if (
        isinstance(decimals, bool)
        or not isinstance(decimals, int)
        or not 0 <= decimals <= 255
    ):
        errors.append("invalid:feed_decimals")

    block_number = observation.get("block_number")
    if (
        isinstance(block_number, bool)
        or not isinstance(block_number, int)
        or block_number < 0
    ):
        errors.append("invalid:block_number")

    for field in (
        "feed_updated_at",
        "observed_at",
        "block_timestamp",
        "ingested_at",
    ):
        if _is_non_empty(observation.get(field)) and not _is_timestamp(
            observation[field]
        ):
            errors.append(f"invalid:{field}")

    expected_id = (
        f"{observation.get('chain')}:{str(address).lower()}:{round_id}"
    )
    if observation.get("observation_id") != expected_id:
        errors.append("invalid:observation_id")

    return observation, tuple(dict.fromkeys(errors))
