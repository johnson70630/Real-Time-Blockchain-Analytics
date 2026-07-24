# Real-Time-Blockchain-Analytics

## Chainlink market-data poller

Chainlink prices are sampled by a dedicated RPC polling service. The existing
Uniswap and Aave WebSocket producer remains independent and unchanged.

The poller dynamically loads the version-controlled
`config/chainlink_feeds.json` catalog. It currently contains the official
standard Arbitrum proxy addresses for ETH/USD, USDC/USD, USDT/USD, and WBTC/USD,
with a Chainlink source URL recorded beside each address.

```shell
export ALCHEMY_RPC_URL="https://arb-mainnet.g.alchemy.com/v2/..."
export CHAINLINK_POLL_INTERVAL_SECONDS=30
export CHAINLINK_FEEDS_CONFIG="config/chainlink_feeds.json"
```

Only runtime settings belong in `.env`. `CHAINLINK_POLL_INTERVAL_SECONDS`
defaults to 30, and `CHAINLINK_FEEDS_CONFIG` defaults to the checked-in catalog.
Each catalog entry contains `name`, `address`, `base_asset`, `quote_asset`, and
`chain`. Adding a feed requires only another catalog entry.

Run the WebSocket producer only:

```shell
make producer
```

Run the market-data poller only:

```shell
make market-data
```

Run both Docker services with Kafka:

```shell
docker compose up -d producer market-data-poller
```

Each cycle pins all `latestRoundData()` calls to one block and publishes only
new rounds. `round_id` and `answer_raw` are serialized as decimal strings.
Polled messages use a deterministic
`chain:feed_address:round_id` observation ID and do not fabricate transaction
hashes or log indexes.

## Market-data Bronze

Market observations use the dedicated `market_data_observations` Kafka topic.
Start its Bronze consumer separately:

```shell
make market-data-bronze
```

Valid observations are retained exactly, including the original Kafka JSON, at:

```text
data/bronze/market_data/
  chain=arbitrum/
    observation_date=YYYY-MM-DD/
```

Malformed observations are preserved with validation reasons under
`data/quarantine/market_data/`. Bronze performs no deduplication, decimal
application, price normalization, or downstream enrichment.
