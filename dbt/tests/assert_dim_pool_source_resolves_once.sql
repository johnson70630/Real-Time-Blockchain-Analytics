select
    pools.chain,
    pools.protocol,
    pools.pool_address,
    count(dimensions.pool_key) as dimension_matches
from {{ ref('stg_uniswap_v3_pools') }} as pools
left join {{ ref('dim_pool') }} as dimensions
    on pools.chain = dimensions.chain
    and pools.protocol = dimensions.protocol
    and pools.pool_address = dimensions.pool_identifier
group by pools.chain, pools.protocol, pools.pool_address
having count(dimensions.pool_key) != 1
