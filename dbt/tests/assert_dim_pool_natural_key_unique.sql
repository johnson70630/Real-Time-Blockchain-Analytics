select chain_key, protocol_key, pool_identifier, count(*) as row_count
from {{ ref('dim_pool') }}
group by chain_key, protocol_key, pool_identifier
having count(*) > 1
