select
    sha2(
        concat_ws(
            '|',
            lower(pools.chain),
            lower(pools.protocol),
            lower(pools.pool_address)
        ),
        256
    ) as pool_key,
    chains.chain_key,
    pools.chain,
    protocols.protocol_key,
    pools.protocol,
    protocols.protocol_version,
    pools.pool_address as pool_identifier,
    pools.pool_address,
    cast(null as varchar) as pool_id,
    token0.token_key as token0_key,
    token1.token_key as token1_key,
    pools.token0_address,
    pools.token1_address,
    pools.fee_tier,
    pools.tick_spacing,
    pools.factory_address,
    pools.metadata_source,
    pools.factory_verified
from {{ ref('stg_uniswap_v3_pools') }} as pools
left join {{ ref('dim_chain') }} as chains
    on pools.chain = chains.chain
left join {{ ref('dim_protocol') }} as protocols
    on pools.protocol = protocols.protocol
left join {{ ref('dim_token') }} as token0
    on pools.chain = token0.chain
    and pools.token0_address = token0.token_address
left join {{ ref('dim_token') }} as token1
    on pools.chain = token1.chain
    and pools.token1_address = token1.token_address
