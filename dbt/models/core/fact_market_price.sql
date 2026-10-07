select
    prices.observation_id,
    dates.date_key,
    chains.chain_key,
    protocols.protocol_key,
    prices.feed_address,
    prices.base_asset,
    prices.quote_asset,
    prices.round_id,
    prices.answer_raw,
    prices.feed_decimals,
    prices.price::number(38, 18) as price,
    prices.feed_updated_at,
    prices.observed_at,
    prices.block_number,
    prices.block_timestamp,
    prices.ingested_at,
    prices.silver_processed_at,
    prices.producer_version,
    prices.schema_version,
    prices.silver_job_version
from {{ ref('stg_chainlink_prices') }} as prices
left join {{ ref('dim_date') }} as dates
    on prices.feed_updated_at::date = dates.date
left join {{ ref('dim_chain') }} as chains
    on prices.chain = chains.chain
left join {{ ref('dim_protocol') }} as protocols
    on prices.protocol = protocols.protocol
