select chain, token_address, count(*) as row_count
from {{ ref('dim_token') }}
group by chain, token_address
having count(*) > 1
