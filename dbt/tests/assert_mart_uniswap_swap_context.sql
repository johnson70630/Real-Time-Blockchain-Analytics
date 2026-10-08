select mart.event_id
from {{ ref('mart_uniswap_swap_tutorial') }} as mart
left join {{ ref('dim_pool') }} as pools
    on mart.pool_key = pools.pool_key
left join {{ ref('dim_token') }} as token0
    on mart.token0_key = token0.token_key
left join {{ ref('dim_token') }} as token1
    on mart.token1_key = token1.token_key
where pools.pool_key is null
    or token0.token_key is null
    or token1.token_key is null
    or mart.pool_address != pools.pool_address
    or mart.token0_address != token0.token_address
    or mart.token1_address != token1.token_address
    or mart.pool_pair != concat(token0.symbol, ' / ', token1.symbol)
