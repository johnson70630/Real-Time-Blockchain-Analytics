select
    swaps.event_id,
    dates.date_key,
    chains.chain_key,
    protocols.protocol_key,
    pools.pool_key,
    pools.token0_key,
    pools.token1_key,
    swaps.block_timestamp,
    swaps.block_number,
    swaps.transaction_hash,
    swaps.log_index,
    swaps.pool_address,
    swaps.amount0_raw,
    {{ normalize_token_amount('swaps.amount0_raw', 'token0.decimals') }} as amount0,
    swaps.amount1_raw,
    {{ normalize_token_amount('swaps.amount1_raw', 'token1.decimals') }} as amount1,
    swaps.ingested_at,
    swaps.silver_processed_at,
    swaps.producer_version,
    swaps.schema_version,
    swaps.silver_job_version
from {{ ref('int_uniswap_swap_classification') }} as swaps
left join {{ ref('dim_date') }} as dates
    on swaps.block_timestamp::date = dates.date
left join {{ ref('dim_chain') }} as chains
    on swaps.chain = chains.chain
left join {{ ref('dim_protocol') }} as protocols
    on swaps.protocol = protocols.protocol
left join {{ ref('dim_pool') }} as pools
    on swaps.chain = pools.chain
    and swaps.protocol = pools.protocol
    and swaps.pool_address = pools.pool_identifier
left join {{ ref('dim_token') }} as token0
    on pools.token0_key = token0.token_key
left join {{ ref('dim_token') }} as token1
    on pools.token1_key = token1.token_key
where swaps.swap_classification = 'CANONICAL_UNISWAP_V3'
