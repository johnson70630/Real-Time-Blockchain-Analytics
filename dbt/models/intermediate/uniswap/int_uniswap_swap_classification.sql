select
    swaps.*,
    case
        when pools.pool_key is not null then 'CANONICAL_UNISWAP_V3'
        else 'NONCANONICAL_OR_INCOMPATIBLE'
    end as swap_classification,
    case
        when pools.pool_key is not null then 'verified_official_factory_pool'
        else 'not_in_verified_pool_registry'
    end as classification_reason
from {{ ref('stg_uniswap_swaps') }} as swaps
left join {{ ref('dim_pool') }} as pools
    on swaps.chain = pools.chain
    and swaps.protocol = pools.protocol
    and swaps.pool_address = pools.pool_identifier
