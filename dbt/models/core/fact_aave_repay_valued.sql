select
    repays.*,
    prices.price_status,
    prices.price_status_reason,
    prices.mapped_feed_address,
    prices.matched_observation_id,
    prices.matched_feed_address,
    prices.price_usd,
    prices.price_feed_updated_at,
    prices.price_age_seconds,
    prices.amount_usd
from {{ ref('fact_aave_repay') }} as repays
left join {{ ref('int_defi_event_token_prices') }} as prices
    on repays.event_id = prices.event_id
    and prices.event_domain = 'aave_repay'
    and prices.token_side = 'reserve'
