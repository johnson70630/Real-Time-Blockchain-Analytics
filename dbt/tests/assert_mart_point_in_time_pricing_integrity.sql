{% set max_price_age_seconds = var('max_price_age_seconds', 300) | int %}

select event_domain, event_id, token_side
from {{ ref('mart_point_in_time_pricing_tutorial') }}
where not selected_price_is_non_future
    or feed_updated_at > event_timestamp
    or coalesce(price_age_seconds, 0) < 0
    or (
        price_status = 'priced'
        and (
            observation_id is null
            or price is null
            or valuation_price_usd is null
            or amount_usd is null
            or price != valuation_price_usd
            or price_age_seconds > {{ max_price_age_seconds }}
        )
    )
    or (
        price_status = 'stale'
        and (
            observation_id is null
            or price is null
            or valuation_price_usd is not null
            or amount_usd is not null
            or price_age_seconds <= {{ max_price_age_seconds }}
        )
    )
    or (
        price_status = 'unmapped'
        and (
            feed_address is not null
            or observation_id is not null
            or price is not null
            or valuation_price_usd is not null
            or amount_usd is not null
        )
    )
    or (
        price_status = 'no_prior_price'
        and (
            feed_address is null
            or observation_id is not null
            or price is not null
            or valuation_price_usd is not null
            or amount_usd is not null
        )
    )
    or (
        next_observation_id is null
        and (
            next_round_id is not null
            or next_price is not null
            or next_feed_updated_at is not null
            or seconds_until_next_round is not null
            or next_price_occurs_after_event
        )
    )
    or (
        next_observation_id is not null
        and (
            next_round_id is null
            or next_price is null
            or next_feed_updated_at <= event_timestamp
            or seconds_until_next_round <= 0
            or not next_price_occurs_after_event
        )
    )
