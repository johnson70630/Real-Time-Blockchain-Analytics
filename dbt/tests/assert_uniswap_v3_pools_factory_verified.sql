select chain, protocol, pool_address
from {{ ref('stg_uniswap_v3_pools') }}
where factory_verified is distinct from true
