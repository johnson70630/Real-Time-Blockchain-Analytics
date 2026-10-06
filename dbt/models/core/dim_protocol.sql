with observed_protocols as (
    select protocol from {{ ref('stg_uniswap_v3_pools') }}
    union
    select protocol from {{ ref('stg_uniswap_swaps') }}
    union
    select protocol from {{ ref('stg_aave_borrows') }}
    union
    select protocol from {{ ref('stg_aave_repays') }}
    union
    select protocol from {{ ref('stg_aave_liquidations') }}
    union
    select protocol from {{ ref('stg_chainlink_prices') }}
)

select
    sha2(lower(trim(protocol)), 256) as protocol_key,
    lower(trim(protocol)) as protocol,
    case lower(trim(protocol))
        when 'uniswap_v3' then 'Uniswap'
        when 'aave_v3' then 'Aave'
        when 'chainlink' then 'Chainlink'
        else trim(protocol)
    end as protocol_name,
    case lower(trim(protocol))
        when 'uniswap_v3' then 'v3'
        when 'aave_v3' then 'v3'
        when 'chainlink' then null
    end as protocol_version
from observed_protocols
where protocol is not null
