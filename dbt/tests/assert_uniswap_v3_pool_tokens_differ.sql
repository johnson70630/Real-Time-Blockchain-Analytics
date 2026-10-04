select chain, protocol, pool_address, token0_address, token1_address
from {{ ref('stg_uniswap_v3_pools') }}
where token0_address = token1_address
