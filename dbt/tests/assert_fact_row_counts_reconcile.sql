with reconciliation as (
    select 'uniswap_swap' as fact_name,
           (
               select count(*)
               from {{ ref('int_uniswap_swap_classification') }}
               where swap_classification = 'CANONICAL_UNISWAP_V3'
           ) as source_rows,
           (select count(*) from {{ ref('fact_uniswap_swap') }}) as fact_rows
    union all
    select 'aave_borrow',
           (select count(*) from {{ ref('stg_aave_borrows') }}),
           (select count(*) from {{ ref('fact_aave_borrow') }})
    union all
    select 'aave_repay',
           (select count(*) from {{ ref('stg_aave_repays') }}),
           (select count(*) from {{ ref('fact_aave_repay') }})
    union all
    select 'aave_liquidation',
           (select count(*) from {{ ref('stg_aave_liquidations') }}),
           (select count(*) from {{ ref('fact_aave_liquidation') }})
    union all
    select 'market_price',
           (select count(*) from {{ ref('stg_chainlink_prices') }}),
           (select count(*) from {{ ref('fact_market_price') }})
)

select * from reconciliation where source_rows != fact_rows
