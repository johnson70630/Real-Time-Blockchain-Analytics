with selected_prices as (
    select
        pit.*,
        observations.round_id,
        observations.price as observation_price
    from {{ ref('int_defi_event_token_prices') }} as pit
    left join {{ ref('fact_market_price') }} as observations
        on pit.matched_observation_id = observations.observation_id
),

next_observations as (
    select
        pit.event_domain,
        pit.event_id,
        pit.token_side,
        observations.observation_id as next_observation_id,
        observations.round_id as next_round_id,
        observations.price as next_price,
        observations.feed_updated_at as next_feed_updated_at
    from {{ ref('int_defi_event_token_prices') }} as pit
    inner join {{ ref('fact_market_price') }} as observations
        on pit.chain_key = observations.chain_key
        and lower(pit.mapped_feed_address) = lower(observations.feed_address)
        and upper(pit.mapped_base_asset) = upper(observations.base_asset)
        and upper(pit.mapped_quote_asset) = upper(observations.quote_asset)
        and observations.price > 0
        and observations.feed_updated_at > pit.event_timestamp
    qualify row_number() over (
        partition by pit.event_domain, pit.event_id, pit.token_side
        order by
            observations.feed_updated_at,
            observations.block_number,
            length(ltrim(observations.round_id, '0')),
            ltrim(observations.round_id, '0'),
            observations.observation_id
    ) = 1
)

select
    pit.event_domain,
    pit.event_id,
    pit.token_side,
    case
        when pit.event_domain = 'uniswap_swap' then 'uniswap_v3'
        when pit.event_domain like 'aave_%' then 'aave_v3'
    end as protocol,
    case
        when pit.event_domain = 'uniswap_swap' then 'swap'
        when pit.event_domain = 'aave_borrow' then 'borrow'
        when pit.event_domain = 'aave_repay' then 'repay'
        when pit.event_domain = 'aave_liquidation' then 'liquidation'
    end as event_type,
    case
        when pit.event_domain = 'aave_borrow' then 'BORROW'
        when pit.event_domain = 'aave_repay' then 'REPAY'
        when pit.event_domain = 'aave_liquidation' then 'LIQUIDATION'
    end as activity_type,
    pit.event_timestamp,
    pit.token_key,
    pit.token_address,
    pit.token_symbol,
    pit.normalized_amount,
    pit.mapped_feed_address as feed_address,
    pit.mapped_base_asset as base_asset,
    pit.mapped_quote_asset as quote_asset,
    pit.matched_observation_id as observation_id,
    pit.round_id,
    pit.observation_price as price,
    pit.price_usd as valuation_price_usd,
    pit.price_feed_updated_at as feed_updated_at,
    pit.price_age_seconds,
    pit.price_status,
    pit.price_status_reason,
    pit.amount_usd,
    pit.matched_observation_id is null
        or pit.price_feed_updated_at <= pit.event_timestamp
        as selected_price_is_non_future,
    future.next_observation_id,
    future.next_round_id,
    future.next_price,
    future.next_feed_updated_at,
    datediff(
        'second', pit.event_timestamp, future.next_feed_updated_at
    ) as seconds_until_next_round,
    future.next_observation_id is not null
        and future.next_feed_updated_at > pit.event_timestamp
        as next_price_occurs_after_event
from selected_prices as pit
left join next_observations as future
    on pit.event_domain = future.event_domain
    and pit.event_id = future.event_id
    and pit.token_side = future.token_side
