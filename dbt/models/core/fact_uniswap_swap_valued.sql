select
    swaps.*,
    token0.price_status as token0_price_status,
    token0.price_status_reason as token0_price_status_reason,
    token0.mapped_feed_address as token0_mapped_feed_address,
    token0.matched_observation_id as token0_matched_observation_id,
    token0.matched_feed_address as token0_matched_feed_address,
    token0.price_usd as token0_price_usd,
    token0.price_feed_updated_at as token0_price_feed_updated_at,
    token0.price_age_seconds as token0_price_age_seconds,
    token0.amount_usd as amount0_usd,
    token1.price_status as token1_price_status,
    token1.price_status_reason as token1_price_status_reason,
    token1.mapped_feed_address as token1_mapped_feed_address,
    token1.matched_observation_id as token1_matched_observation_id,
    token1.matched_feed_address as token1_matched_feed_address,
    token1.price_usd as token1_price_usd,
    token1.price_feed_updated_at as token1_price_feed_updated_at,
    token1.price_age_seconds as token1_price_age_seconds,
    token1.amount_usd as amount1_usd
from {{ ref('fact_uniswap_swap') }} as swaps
left join {{ ref('int_defi_event_token_prices') }} as token0
    on swaps.event_id = token0.event_id
    and token0.event_domain = 'uniswap_swap'
    and token0.token_side = 'token0'
left join {{ ref('int_defi_event_token_prices') }} as token1
    on swaps.event_id = token1.event_id
    and token1.event_domain = 'uniswap_swap'
    and token1.token_side = 'token1'
