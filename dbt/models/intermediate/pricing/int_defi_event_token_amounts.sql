select
    'uniswap_swap' as event_domain,
    event_id,
    'token0' as token_side,
    block_timestamp as event_timestamp,
    chain_key,
    token0_key as token_key,
    amount0::number(38, 18) as normalized_amount
from {{ ref('fact_uniswap_swap') }}

union all

select
    'uniswap_swap',
    event_id,
    'token1',
    block_timestamp,
    chain_key,
    token1_key,
    amount1::number(38, 18)
from {{ ref('fact_uniswap_swap') }}

union all

select
    'aave_borrow',
    event_id,
    'reserve',
    block_timestamp,
    chain_key,
    reserve_token_key,
    borrow_amount::number(38, 18)
from {{ ref('fact_aave_borrow') }}

union all

select
    'aave_repay',
    event_id,
    'reserve',
    block_timestamp,
    chain_key,
    reserve_token_key,
    repay_amount::number(38, 18)
from {{ ref('fact_aave_repay') }}

union all

select
    'aave_liquidation',
    event_id,
    'debt',
    block_timestamp,
    chain_key,
    debt_token_key,
    debt_to_cover::number(38, 18)
from {{ ref('fact_aave_liquidation') }}

union all

select
    'aave_liquidation',
    event_id,
    'collateral',
    block_timestamp,
    chain_key,
    collateral_token_key,
    liquidated_collateral_amount::number(38, 18)
from {{ ref('fact_aave_liquidation') }}
