{% set max_price_age_seconds = var('max_price_age_seconds', 300) | int %}

with canonical_price_times as (
    select
        *
    from {{ ref('fact_market_price') }}
    where price > 0
    qualify row_number() over (
        partition by
            chain_key,
            lower(feed_address),
            upper(base_asset),
            upper(quote_asset),
            feed_updated_at
        order by
            block_number desc nulls last,
            length(ltrim(round_id, '0')) desc nulls last,
            ltrim(round_id, '0') desc nulls last,
            silver_processed_at desc nulls last,
            observation_id desc nulls last,
            feed_address desc nulls last,
            price desc nulls last
    ) = 1
),

price_intervals as (
    select
        *,
        lead(feed_updated_at) over (
            partition by
                chain_key,
                lower(feed_address),
                upper(base_asset),
                upper(quote_asset)
            order by feed_updated_at
        ) as next_feed_updated_at
    from canonical_price_times
),

candidates as (
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
    left join price_intervals as prices
        on event_tokens.chain_key = prices.chain_key
        and lower(mappings.feed_address) = lower(prices.feed_address)
        and upper(mappings.base_asset) = upper(prices.base_asset)
        and upper(mappings.quote_asset) = upper(prices.quote_asset)
        and prices.feed_updated_at <= event_tokens.event_timestamp
        and (
            prices.next_feed_updated_at > event_tokens.event_timestamp
            or prices.next_feed_updated_at is null
        )
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
