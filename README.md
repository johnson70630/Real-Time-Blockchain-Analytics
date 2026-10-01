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

## Market-data Silver

Build the deduplicated Chainlink price dataset from Bronze:

```shell
make market-data-silver
```

Silver calculates exact `DECIMAL(38, 18)` prices and partitions them by the
Chainlink feed update date:

```text
data/silver/market_prices/
  price_date=YYYY-MM-DD/
    part-....parquet
```

Invalid business records are retained with validation reasons under
`data/quarantine/market_data_silver/`. The job reads Bronze Parquet rather than
Kafka and deterministically keeps the newest Bronze record for each
`observation_id`.

## Silver price enrichment

Build point-in-time Chainlink price enrichment after the event and market-price
Silver jobs have completed:

```shell
make price-enrichment
```

The job uses `feed_updated_at` as the price-validity timestamp and never selects
a price after an event's `block_timestamp`. The latest prior price must be no
older than `MAX_PRICE_AGE_SECONDS` (300 by default). Missing mappings, missing
prices, stale prices, and invalid timestamps retain the source event with an
explicit enrichment status.

The reusable transformation is implemented with native Spark DataFrame joins
and window functions. It joins on chain, mapped base asset, and quote asset,
then orders eligible observations by `feed_updated_at`, block number, numeric
round ID, Silver processing time, observation ID, and feed address. Raw price
values provide final deterministic tie-breakers. Only the small
version-controlled mapping tables are broadcast; the price dataset is not
collected or blindly broadcast.

Asset aliases, token decimals, and Aave reserve addresses live in
`config/asset_price_mapping.json`. Because Uniswap logs identify a pool rather
than its token contracts, add a verified pool entry before expecting a pool's
token0/token1 sides to enrich:

```json
{
  "chain": "arbitrum",
  "pool_address": "0x...",
  "token0_asset": "WETH",
  "token1_asset": "USDC"
}
```

Enriched datasets are atomically replaced under:

```text
data/silver/enriched/
  uniswap_v3/swaps_enriched.parquet
  aave_v3/borrow_events_enriched.parquet
  aave_v3/repay_events_enriched.parquet
  aave_v3/liquidation_events_enriched.parquet
```

Raw amounts remain strings. Normalized amounts and prices use
`DECIMAL(38, 18)`; derived USD values use `DECIMAL(38, 8)`. USD calculations
use fixed-precision Spark decimal operands, round half-up at the eighth decimal
place, and never use `FloatType` or `DoubleType`.

## Durable data lake and Snowflake RAW landing

Local Parquet remains the default. Setting `STORAGE_MODE=s3` routes Spark-managed
Bronze, Chainlink Silver, enrichment, quarantine, and checkpoint locations
through the canonical `s3a://<bucket>/<prefix>/...` data-lake layout. Spark uses
Hadoop's standard AWS credential provider chain; credentials are never placed
in project configuration.

The current Snowflake milestone generates only the reviewed Uniswap swap
landing flow:

```shell
make snowflake-sql
```

The database, integration, stage, and Parquet file format are already
provisioned. The generated SQL creates persistent `UNISWAP_SWAPS_LANDING` and
`UNISWAP_SWAPS` tables, then emits an idempotent `COPY INTO` plus `MERGE` flow.
See
[`docs/cloud_data_platform.md`](docs/cloud_data_platform.md) for architecture,
security, and operating instructions. dbt business models are intentionally a
later milestone.
