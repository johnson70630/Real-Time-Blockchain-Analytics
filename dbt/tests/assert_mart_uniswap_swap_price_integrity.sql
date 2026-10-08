select event_id
from {{ ref('mart_uniswap_swap_tutorial') }}
where token0_price_feed_updated_at > block_timestamp
    or token1_price_feed_updated_at > block_timestamp
    or coalesce(token0_price_age_seconds, 0) < 0
    or coalesce(token1_price_age_seconds, 0) < 0
    or (
        token0_price_status = 'priced'
        and (token0_price_usd is null or amount0_usd is null)
    )
    or (
        token1_price_status = 'priced'
        and (token1_price_usd is null or amount1_usd is null)
    )
    or (
        token0_price_status != 'priced'
        and (token0_price_usd is not null or amount0_usd is not null)
    )
    or (
        token1_price_status != 'priced'
        and (token1_price_usd is not null or amount1_usd is not null)
    )
    or has_complete_usd_valuation
        != (token0_price_status = 'priced' and token1_price_status = 'priced')
    or pricing_coverage_status != case
        when token0_price_status = 'priced'
            and token1_price_status = 'priced' then 'FULLY_PRICED'
        when token0_price_status = 'priced'
            or token1_price_status = 'priced' then 'PARTIALLY_PRICED'
        else 'UNPRICED'
    end
