with aave_assets as (
    select chain, reserve as token_address
    from {{ ref('stg_aave_borrows') }}

    union

    select chain, reserve as token_address
    from {{ ref('stg_aave_repays') }}

    union

    select chain, collateral_asset as token_address
    from {{ ref('stg_aave_liquidations') }}

    union

    select chain, debt_asset as token_address
    from {{ ref('stg_aave_liquidations') }}
)

select aave_assets.chain, aave_assets.token_address
from aave_assets
left join {{ ref('stg_tokens') }} as tokens
    on aave_assets.chain = tokens.chain
    and aave_assets.token_address = tokens.token_address
where tokens.token_address is null
