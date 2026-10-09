select
    coalesce(source.event_domain, mart.event_domain) as event_domain,
    coalesce(source.event_id, mart.event_id) as event_id,
    coalesce(source.token_side, mart.token_side) as token_side
from {{ ref('int_defi_event_token_prices') }} as source
full outer join {{ ref('mart_point_in_time_pricing_tutorial') }} as mart
    on source.event_domain = mart.event_domain
    and source.event_id = mart.event_id
    and source.token_side = mart.token_side
where source.event_id is null
    or mart.event_id is null
    or not equal_null(source.token_key, mart.token_key)
    or not equal_null(source.token_address, mart.token_address)
    or not equal_null(source.token_symbol, mart.token_symbol)
    or not equal_null(source.normalized_amount, mart.normalized_amount)
    or not equal_null(source.mapped_feed_address, mart.feed_address)
    or not equal_null(source.mapped_base_asset, mart.base_asset)
    or not equal_null(source.mapped_quote_asset, mart.quote_asset)
    or not equal_null(source.matched_observation_id, mart.observation_id)
    or not equal_null(source.price_usd, mart.valuation_price_usd)
    or not equal_null(source.price_feed_updated_at, mart.feed_updated_at)
    or not equal_null(source.price_age_seconds, mart.price_age_seconds)
    or not equal_null(source.price_status, mart.price_status)
    or not equal_null(source.amount_usd, mart.amount_usd)
