select event_id
from {{ ref('mart_aave_lending_tutorial') }}
where price_feed_updated_at > block_timestamp
    or coalesce(price_age_seconds, 0) < 0
    or (
        price_status = 'priced'
        and (price_usd is null or amount_usd is null)
    )
    or (
        price_status != 'priced'
        and (price_usd is not null or amount_usd is not null)
    )
    or has_valid_usd_valuation != (price_status = 'priced')
