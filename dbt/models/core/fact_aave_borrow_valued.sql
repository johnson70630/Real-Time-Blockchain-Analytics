select
    borrows.*,
    prices.price_status,
    prices.price_status_reason,
    prices.mapped_feed_address,
    prices.matched_observation_id,
    prices.matched_feed_address,
    prices.price_usd,
    prices.price_feed_updated_at,
    prices.price_age_seconds,
    prices.amount_usd
from {{ ref('fact_aave_borrow') }} as borrows
left join {{ ref('int_defi_event_token_prices') }} as prices
    on borrows.event_id = prices.event_id
    and prices.event_domain = 'aave_borrow'
    and prices.token_side = 'reserve'
