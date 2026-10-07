{% set max_price_age_seconds = var('max_price_age_seconds', 300) | int %}

select *
from {{ ref('int_defi_event_token_prices') }}
where price_feed_updated_at > event_timestamp
    or price_age_seconds < 0
    or (
        price_status = 'priced'
        and (
            matched_observation_id is null
            or matched_feed_address is null
            or price_feed_updated_at is null
            or price_age_seconds is null
            or price_age_seconds > {{ max_price_age_seconds }}
            or price_usd is null
            or amount_usd is null
        )
    )
    or (
        price_status = 'stale'
        and (
            matched_observation_id is null
            or matched_feed_address is null
            or price_feed_updated_at is null
            or price_age_seconds <= {{ max_price_age_seconds }}
            or price_usd is not null
            or amount_usd is not null
        )
    )
    or (
        price_status = 'no_prior_price'
        and (
            mapped_feed_address is null
            or matched_observation_id is not null
            or price_usd is not null
            or amount_usd is not null
        )
    )
    or (
        price_status = 'unmapped'
        and (
            mapped_feed_address is not null
            or matched_observation_id is not null
            or price_usd is not null
            or amount_usd is not null
        )
    )
