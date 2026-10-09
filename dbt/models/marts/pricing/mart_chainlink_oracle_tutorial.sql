with observations as (
    select
        prices.observation_id,
        chains.chain,
        protocols.protocol,
        prices.feed_address,
        prices.base_asset,
        prices.quote_asset,
        prices.round_id,
        prices.answer_raw,
        prices.feed_decimals,
        prices.price,
        prices.feed_updated_at,
        prices.observed_at,
        prices.block_number,
        prices.block_timestamp
    from {{ ref('fact_market_price') }} as prices
    inner join {{ ref('dim_chain') }} as chains
        on prices.chain_key = chains.chain_key
    inner join {{ ref('dim_protocol') }} as protocols
        on prices.protocol_key = protocols.protocol_key
),

feed_times as (
    select
        chain,
        feed_address,
        feed_updated_at,
        lag(feed_updated_at) over (
            partition by lower(chain), lower(feed_address)
            order by feed_updated_at
        ) as previous_feed_updated_at
    from observations
    group by chain, feed_address, feed_updated_at
),

sequenced as (
    select
        current_observation.*,
        previous_observation.observation_id as previous_observation_id,
        previous_observation.price as previous_price,
        feed_times.previous_feed_updated_at
    from observations as current_observation
    inner join feed_times
        on lower(current_observation.chain) = lower(feed_times.chain)
        and lower(current_observation.feed_address)
            = lower(feed_times.feed_address)
        and current_observation.feed_updated_at = feed_times.feed_updated_at
    left join observations as previous_observation
        on lower(current_observation.chain) = lower(previous_observation.chain)
        and lower(current_observation.feed_address)
            = lower(previous_observation.feed_address)
        and feed_times.previous_feed_updated_at
            = previous_observation.feed_updated_at
    qualify row_number() over (
        partition by current_observation.observation_id
        order by
            previous_observation.block_number desc nulls last,
            length(ltrim(previous_observation.round_id, '0')) desc nulls last,
            ltrim(previous_observation.round_id, '0') desc nulls last,
            previous_observation.observation_id desc nulls last
    ) = 1
)

select
    *,
    datediff(
        'second', previous_feed_updated_at, feed_updated_at
    ) as seconds_since_previous_round,
    (price - previous_price)::number(38, 18) as price_change,
    (
        (price - previous_price) / nullif(previous_price, 0) * 100
    )::number(38, 18) as price_change_pct
from sequenced
