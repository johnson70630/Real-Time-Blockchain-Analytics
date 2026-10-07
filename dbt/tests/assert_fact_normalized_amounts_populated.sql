select 'uniswap_amount0' as field_name, event_id
from {{ ref('fact_uniswap_swap') }}
where regexp_like(amount0_raw, '^-?[0-9]+$') and amount0 is null

union all

select 'uniswap_amount1', event_id from {{ ref('fact_uniswap_swap') }}
where regexp_like(amount1_raw, '^-?[0-9]+$') and amount1 is null

union all

select 'aave_borrow', event_id from {{ ref('fact_aave_borrow') }}
where regexp_like(amount_raw, '^[0-9]+$') and borrow_amount is null

union all

select 'aave_repay', event_id from {{ ref('fact_aave_repay') }}
where regexp_like(amount_raw, '^[0-9]+$') and repay_amount is null

union all

select 'liquidation_debt', event_id from {{ ref('fact_aave_liquidation') }}
where regexp_like(debt_to_cover_raw, '^[0-9]+$') and debt_to_cover is null

union all

select 'liquidation_collateral', event_id from {{ ref('fact_aave_liquidation') }}
where regexp_like(liquidated_collateral_amount_raw, '^[0-9]+$')
    and liquidated_collateral_amount is null
