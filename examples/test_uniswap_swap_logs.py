import json
import os

from dotenv import load_dotenv
from websocket import create_connection
from web3 import Web3

load_dotenv()

ALCHEMY_WEBSOCKET_URL = os.getenv("ALCHEMY_WEBSOCKET_URL")

if not ALCHEMY_WEBSOCKET_URL:
    raise ValueError("ALCHEMY_WEBSOCKET_URL is missing from .env")

UNISWAP_V3_SWAP_TOPIC = "0x" + Web3.keccak(
    text="Swap(address,address,int256,int256,uint160,uint128,int24)"
).hex()

UNISWAP_V3_WETH_USDC_POOL = "0xC6962004f452bE9203591991D15f6b388e09E8D0".lower()

print(f"Using swap topic: {UNISWAP_V3_SWAP_TOPIC}")
print(f"Using pool address: {UNISWAP_V3_WETH_USDC_POOL}")

subscribe_request = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "eth_subscribe",
    "params": [
        "logs",
        {
            "topics": [UNISWAP_V3_SWAP_TOPIC],
        },
    ],
}

ws = create_connection(ALCHEMY_WEBSOCKET_URL)

ws.send(json.dumps(subscribe_request))
print(json.dumps(subscribe_request, indent=2))
print("Subscribed to Uniswap V3 WETH/USDC Swap logs on Arbitrum...")

while True:
    message = json.loads(ws.recv())

    if "error" in message:
        raise RuntimeError(f"Alchemy subscription error: {message['error']}")

    if "result" in message and message.get("id") == 1:
        print(f"Subscription confirmed: {message['result']}")
        continue

    if message.get("method") == "eth_subscription":
        log = message["params"]["result"]
        pool_address = log["address"].lower()
        is_target_pool = pool_address == UNISWAP_V3_WETH_USDC_POOL

        print(
            "swap_log "
            f"block_number={int(log['blockNumber'], 16)} "
            f"tx_hash={log['transactionHash']} "
            f"pool_address={pool_address} "
            f"is_target_pool={is_target_pool} "
            f"log_index={int(log['logIndex'], 16)}"
        )
        continue

    print(json.dumps(message, indent=2))