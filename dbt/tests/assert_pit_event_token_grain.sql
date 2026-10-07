with expected as (
    select
        (select count(*) * 2 from {{ ref('fact_uniswap_swap') }})
        + (select count(*) from {{ ref('fact_aave_borrow') }})
        + (select count(*) from {{ ref('fact_aave_repay') }})
        + (select count(*) * 2 from {{ ref('fact_aave_liquidation') }})
            as expected_rows
),
actual as (
    select
        count(*) as amount_rows,
        count(distinct event_domain || '|' || event_id || '|' || token_side)
            as distinct_amount_rows
    from {{ ref('int_defi_event_token_amounts') }}
),
matched as (
    select
        count(*) as matched_rows,
        count(distinct event_domain || '|' || event_id || '|' || token_side)
            as distinct_matched_rows
    from {{ ref('int_defi_event_token_prices') }}
)
select *
from expected, actual, matched
where expected_rows != amount_rows
    or amount_rows != distinct_amount_rows
    or amount_rows != matched_rows
    or matched_rows != distinct_matched_rows
