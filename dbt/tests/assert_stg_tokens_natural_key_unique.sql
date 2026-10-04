select
    chain,
    token_address,
    count(*) as row_count
from {{ ref('stg_tokens') }}
group by chain, token_address
having count(*) > 1
