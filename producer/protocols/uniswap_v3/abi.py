"""ABI definitions for supported Uniswap V3 pool events."""

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

POOL_CREATED_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "token0", "type": "address"},
        {"indexed": True, "name": "token1", "type": "address"},
        {"indexed": True, "name": "fee", "type": "uint24"},
        {"indexed": False, "name": "tickSpacing", "type": "int24"},
        {"indexed": False, "name": "pool", "type": "address"},
    ],
    "name": "PoolCreated",
    "type": "event",
}

POOL_CREATED_EVENT_SIGNATURE = (
    "PoolCreated(address,address,uint24,int24,address)"
)

# Minimal read-only interface used to verify observed pool contracts.
POOL_REFERENCE_ABI = [
    {
        "inputs": [],
        "name": name,
        "outputs": [{"name": "", "type": output_type}],
        "stateMutability": "view",
        "type": "function",
    }
    for name, output_type in (
        ("token0", "address"),
        ("token1", "address"),
        ("fee", "uint24"),
        ("tickSpacing", "int24"),
        ("factory", "address"),
    )
]
