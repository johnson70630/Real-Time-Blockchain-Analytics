with comparisons as (
    select
        'uniswap_swap' as fact_name,
        (select count(*) from {{ ref('fact_uniswap_swap') }}) as base_rows,
        (select count(*) from {{ ref('fact_uniswap_swap_valued') }}) as valued_rows
    union all
    select
        'aave_borrow',
        (select count(*) from {{ ref('fact_aave_borrow') }}),
        (select count(*) from {{ ref('fact_aave_borrow_valued') }})
    union all
    select
        'aave_repay',
        (select count(*) from {{ ref('fact_aave_repay') }}),
        (select count(*) from {{ ref('fact_aave_repay_valued') }})
    union all
    select
        'aave_liquidation',
        (select count(*) from {{ ref('fact_aave_liquidation') }}),
        (select count(*) from {{ ref('fact_aave_liquidation_valued') }})
)
select *
from comparisons
where base_rows != valued_rows
