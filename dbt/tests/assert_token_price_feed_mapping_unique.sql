select chain, token_address, count(*) as row_count
from {{ ref('token_price_feed_mapping') }}
group by chain, token_address
having count(*) != 1
