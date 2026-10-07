{% set max_price_age_seconds = var('max_price_age_seconds', 300) | int %}

with candidates as (
    select
        event_tokens.event_domain,
        event_tokens.event_id,
        event_tokens.token_side,
        event_tokens.event_timestamp,
        event_tokens.chain_key,
        event_tokens.token_key,
        tokens.chain,
        tokens.token_address,
        tokens.symbol as token_symbol,
        event_tokens.normalized_amount,
        mappings.feed_address as mapped_feed_address,
        mappings.base_asset as mapped_base_asset,
        mappings.quote_asset as mapped_quote_asset,
        prices.observation_id,
        prices.feed_address as observed_feed_address,
        prices.price as candidate_price_usd,
        prices.feed_updated_at,
        prices.block_number as price_block_number,
        prices.round_id as price_round_id,
        prices.silver_processed_at as price_silver_processed_at
    from {{ ref('int_defi_event_token_amounts') }} as event_tokens
    inner join {{ ref('dim_token') }} as tokens
        on event_tokens.token_key = tokens.token_key
    left join {{ ref('token_price_feed_mapping') }} as mappings
        on lower(tokens.chain) = lower(mappings.chain)
        and lower(tokens.token_address) = lower(mappings.token_address)
    left join {{ ref('fact_market_price') }} as prices
        on event_tokens.chain_key = prices.chain_key
        and lower(mappings.feed_address) = lower(prices.feed_address)
        and upper(mappings.base_asset) = upper(prices.base_asset)
        and upper(mappings.quote_asset) = upper(prices.quote_asset)
        and prices.price > 0
        and prices.feed_updated_at <= event_tokens.event_timestamp
    qualify row_number() over (
        partition by
            event_tokens.event_domain,
            event_tokens.event_id,
            event_tokens.token_side
        order by
            prices.feed_updated_at desc nulls last,
            prices.block_number desc nulls last,
            length(ltrim(prices.round_id, '0')) desc nulls last,
            ltrim(prices.round_id, '0') desc nulls last,
            prices.silver_processed_at desc nulls last,
            prices.observation_id desc nulls last,
            prices.feed_address desc nulls last,
            prices.price desc nulls last
    ) = 1
),

classified as (
    select
        *,
        datediff('second', feed_updated_at, event_timestamp) as price_age_seconds,
        case
            when mapped_feed_address is null then 'unmapped'
            when observation_id is null then 'no_prior_price'
            when datediff('second', feed_updated_at, event_timestamp)
                > {{ max_price_age_seconds }} then 'stale'
            else 'priced'
        end as price_status
    from candidates
)

select
    event_domain,
    event_id,
    token_side,
    event_timestamp,
    chain_key,
    token_key,
    chain,
    token_address,
    token_symbol,
    normalized_amount,
    mapped_feed_address,
    mapped_base_asset,
    mapped_quote_asset,
    observation_id as matched_observation_id,
    observed_feed_address as matched_feed_address,
    feed_updated_at as price_feed_updated_at,
    price_age_seconds,
    price_status,
    case
        when price_status = 'unmapped'
            then 'token address is absent from the authoritative price mapping'
        when price_status = 'no_prior_price'
            then 'no matching price exists at or before the event'
        when price_status = 'stale'
            then 'latest prior price exceeds the maximum allowed age'
    end as price_status_reason,
    iff(
        price_status = 'priced',
        candidate_price_usd,
        null
    )::number(38, 18) as price_usd,
    iff(
        price_status = 'priced',
        {{ calculate_usd_value('normalized_amount', 'candidate_price_usd') }},
        null
    )::number(38, 8) as amount_usd
from classified
