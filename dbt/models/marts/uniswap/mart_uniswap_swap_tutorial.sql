with valued_swaps as (
    select *
    from {{ ref('fact_uniswap_swap_valued') }}
),

denormalized as (
    select
        swaps.event_id,
        swaps.block_timestamp,
        swaps.block_number,
        swaps.transaction_hash,
        swaps.log_index,
        swaps.pool_key,
        pools.pool_address,
        pools.pool_identifier,
        pools.fee_tier,
        swaps.token0_key,
        token0.token_address as token0_address,
        token0.symbol as token0_symbol,
        token0.name as token0_name,
        token0.decimals as token0_decimals,
        swaps.amount0_raw,
        swaps.amount0,
        swaps.token0_price_usd,
        swaps.amount0_usd,
        swaps.token0_price_feed_updated_at,
        swaps.token0_price_age_seconds,
        swaps.token0_price_status,
        swaps.token0_price_status_reason,
        swaps.token0_mapped_feed_address,
        swaps.token0_matched_observation_id,
        swaps.token0_matched_feed_address,
        swaps.token1_key,
        token1.token_address as token1_address,
        token1.symbol as token1_symbol,
        token1.name as token1_name,
        token1.decimals as token1_decimals,
        swaps.amount1_raw,
        swaps.amount1,
        swaps.token1_price_usd,
        swaps.amount1_usd,
        swaps.token1_price_feed_updated_at,
        swaps.token1_price_age_seconds,
        swaps.token1_price_status,
        swaps.token1_price_status_reason,
        swaps.token1_mapped_feed_address,
        swaps.token1_matched_observation_id,
        swaps.token1_matched_feed_address
    from valued_swaps as swaps
    inner join {{ ref('dim_pool') }} as pools
        on swaps.pool_key = pools.pool_key
    inner join {{ ref('dim_token') }} as token0
        on swaps.token0_key = token0.token_key
    inner join {{ ref('dim_token') }} as token1
        on swaps.token1_key = token1.token_key
)

select
    *,
    concat(token0_symbol, ' / ', token1_symbol) as pool_pair,
    token0_price_status = 'priced'
        and token1_price_status = 'priced' as has_complete_usd_valuation,
    case
        when token0_price_status = 'priced'
            and token1_price_status = 'priced' then 'FULLY_PRICED'
        when token0_price_status = 'priced'
            or token1_price_status = 'priced' then 'PARTIALLY_PRICED'
        else 'UNPRICED'
    end as pricing_coverage_status
from denormalized
