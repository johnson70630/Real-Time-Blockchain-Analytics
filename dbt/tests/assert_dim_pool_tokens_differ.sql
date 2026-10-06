select pool_key, token0_key, token1_key
from {{ ref('dim_pool') }}
where token0_key = token1_key
