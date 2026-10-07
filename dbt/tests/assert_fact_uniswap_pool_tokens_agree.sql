select facts.event_id
from {{ ref('fact_uniswap_swap') }} as facts
join {{ ref('dim_pool') }} as pools
    on facts.pool_key = pools.pool_key
where facts.token0_key != pools.token0_key
    or facts.token1_key != pools.token1_key
    or facts.token0_key = facts.token1_key
