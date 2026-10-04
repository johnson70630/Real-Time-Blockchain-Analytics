select
    chain,
    protocol,
    pool_address,
    count(*) as row_count
from {{ ref('stg_uniswap_v3_pools') }}
group by chain, protocol, pool_address
having count(*) > 1
