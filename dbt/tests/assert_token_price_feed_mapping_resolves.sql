select mappings.chain, mappings.token_address, mappings.feed_address
from {{ ref('token_price_feed_mapping') }} as mappings
left join {{ ref('dim_token') }} as tokens
    on lower(mappings.chain) = lower(tokens.chain)
    and lower(mappings.token_address) = lower(tokens.token_address)
where tokens.token_key is null
