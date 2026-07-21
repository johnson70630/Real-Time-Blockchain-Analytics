"""ABI definitions for supported Aave V3 Pool events."""

BORROW_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "reserve", "type": "address"},
        {"indexed": False, "name": "user", "type": "address"},
        {"indexed": True, "name": "onBehalfOf", "type": "address"},
        {"indexed": False, "name": "amount", "type": "uint256"},
        {"indexed": False, "name": "interestRateMode", "type": "uint8"},
        {"indexed": False, "name": "borrowRate", "type": "uint256"},
        {"indexed": True, "name": "referralCode", "type": "uint16"},
    ],
    "name": "Borrow",
    "type": "event",
}

REPAY_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "reserve", "type": "address"},
        {"indexed": True, "name": "user", "type": "address"},
        {"indexed": True, "name": "repayer", "type": "address"},
        {"indexed": False, "name": "amount", "type": "uint256"},
        {"indexed": False, "name": "useATokens", "type": "bool"},
    ],
    "name": "Repay",
    "type": "event",
}

LIQUIDATION_CALL_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "collateralAsset", "type": "address"},
        {"indexed": True, "name": "debtAsset", "type": "address"},
        {"indexed": True, "name": "user", "type": "address"},
        {"indexed": False, "name": "debtToCover", "type": "uint256"},
        {
            "indexed": False,
            "name": "liquidatedCollateralAmount",
            "type": "uint256",
        },
        {"indexed": False, "name": "liquidator", "type": "address"},
        {"indexed": False, "name": "receiveAToken", "type": "bool"},
    ],
    "name": "LiquidationCall",
    "type": "event",
}
