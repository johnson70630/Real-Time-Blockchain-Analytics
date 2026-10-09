select observation_id
from {{ ref('mart_chainlink_oracle_tutorial') }}
where (
        previous_observation_id is null
        and (
            previous_price is not null
            or previous_feed_updated_at is not null
            or seconds_since_previous_round is not null
            or price_change is not null
            or price_change_pct is not null
        )
    )
    or (
        previous_observation_id is not null
        and (
            previous_price is null
            or previous_feed_updated_at >= feed_updated_at
            or seconds_since_previous_round <= 0
        )
    )
