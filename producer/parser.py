from typing import Any

from web3 import Web3

UNISWAP_V3_SWAP_TOPIC = "0x" + Web3.keccak(
    text="Swap(address,address,int256,int256,uint160,uint128,int24)"
).hex()


def parse_swap_log(
    log: dict[str, Any],
    chain: str = "arbitrum",
    protocol: str = "uniswap_v3",
) -> dict[str, Any]:
    """Parse a raw Uniswap V3 Swap log into a normalized event dictionary."""
    return {
        "protocol": protocol,
        "chain": chain,
        "event_type": "swap",
        "block_number": int(log["blockNumber"], 16),
        "transaction_hash": log["transactionHash"],
        "pool_address": log["address"].lower(),
        "log_index": int(log["logIndex"], 16),
        "raw_data": log["data"],
        "raw_topics": log["topics"],
    }