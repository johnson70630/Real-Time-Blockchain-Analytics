select
    sha2(
        concat_ws('|', lower(tokens.chain), lower(tokens.token_address)),
        256
    ) as token_key,
    chains.chain_key,
    tokens.chain,
    tokens.token_address,
    tokens.symbol,
    tokens.name,
    tokens.decimals,
    tokens.metadata_source,
    tokens.metadata_block_number
from {{ ref('stg_tokens') }} as tokens
left join {{ ref('dim_chain') }} as chains
    on tokens.chain = chains.chain
