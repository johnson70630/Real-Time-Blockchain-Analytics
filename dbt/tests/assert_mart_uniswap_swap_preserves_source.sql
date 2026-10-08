select
    coalesce(swaps.event_id, mart.event_id) as event_id
from {{ ref('fact_uniswap_swap_valued') }} as swaps
full outer join {{ ref('mart_uniswap_swap_tutorial') }} as mart
    on swaps.event_id = mart.event_id
where swaps.event_id is null
    or mart.event_id is null
    or not equal_null(swaps.pool_key, mart.pool_key)
    or not equal_null(swaps.token0_key, mart.token0_key)
    or not equal_null(swaps.token1_key, mart.token1_key)
    or not equal_null(swaps.amount0_raw, mart.amount0_raw)
    or not equal_null(swaps.amount1_raw, mart.amount1_raw)
    or not equal_null(swaps.amount0, mart.amount0)
    or not equal_null(swaps.amount1, mart.amount1)
    or not equal_null(swaps.token0_price_status, mart.token0_price_status)
    or not equal_null(swaps.token1_price_status, mart.token1_price_status)
    or not equal_null(swaps.token0_price_usd, mart.token0_price_usd)
    or not equal_null(swaps.token1_price_usd, mart.token1_price_usd)
    or not equal_null(swaps.amount0_usd, mart.amount0_usd)
    or not equal_null(swaps.amount1_usd, mart.amount1_usd)
