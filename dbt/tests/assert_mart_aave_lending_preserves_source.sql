with sources as (
    select
        event_id,
        'BORROW' as activity_type,
        reserve_token_key as token_key,
        amount_raw,
        borrow_amount as amount,
        price_status,
        price_usd,
        amount_usd,
        user,
        on_behalf_of,
        interest_rate_mode,
        borrow_rate_raw,
        referral_code,
        cast(null as varchar) as repayer,
        cast(null as boolean) as use_atokens
    from {{ ref('fact_aave_borrow_valued') }}

    union all

    select
        event_id,
        'REPAY' as activity_type,
        reserve_token_key as token_key,
        amount_raw,
        repay_amount as amount,
        price_status,
        price_usd,
        amount_usd,
        user,
        cast(null as varchar) as on_behalf_of,
        cast(null as number(38, 0)) as interest_rate_mode,
        cast(null as varchar) as borrow_rate_raw,
        cast(null as number(38, 0)) as referral_code,
        repayer,
        use_atokens
    from {{ ref('fact_aave_repay_valued') }}
)

select coalesce(sources.event_id, mart.event_id) as event_id
from sources
full outer join {{ ref('mart_aave_lending_tutorial') }} as mart
    on sources.event_id = mart.event_id
where sources.event_id is null
    or mart.event_id is null
    or not equal_null(sources.activity_type, mart.activity_type)
    or not equal_null(sources.token_key, mart.token_key)
    or not equal_null(sources.amount_raw, mart.amount_raw)
    or not equal_null(sources.amount, mart.amount)
    or not equal_null(sources.price_status, mart.price_status)
    or not equal_null(sources.price_usd, mart.price_usd)
    or not equal_null(sources.amount_usd, mart.amount_usd)
    or not equal_null(sources.user, mart.user)
    or not equal_null(sources.on_behalf_of, mart.on_behalf_of)
    or not equal_null(sources.interest_rate_mode, mart.interest_rate_mode)
    or not equal_null(sources.borrow_rate_raw, mart.borrow_rate_raw)
    or not equal_null(sources.referral_code, mart.referral_code)
    or not equal_null(sources.repayer, mart.repayer)
    or not equal_null(sources.use_atokens, mart.use_atokens)
