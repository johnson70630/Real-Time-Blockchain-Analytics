with pool_tokens as (
    select chain, token0_address as token_address
    from {{ ref('stg_uniswap_v3_pools') }}

    union

    select chain, token1_address as token_address
    from {{ ref('stg_uniswap_v3_pools') }}
)

select pool_tokens.chain, pool_tokens.token_address
from pool_tokens
left join {{ ref('stg_tokens') }} as tokens
    on pool_tokens.chain = tokens.chain
    and pool_tokens.token_address = tokens.token_address
where tokens.token_address is null
