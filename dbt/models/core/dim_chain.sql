with observed_chains as (
    select chain from {{ ref('stg_tokens') }}
    union
    select chain from {{ ref('stg_uniswap_v3_pools') }}
    union
    select chain from {{ ref('stg_uniswap_swaps') }}
    union
    select chain from {{ ref('stg_aave_borrows') }}
    union
    select chain from {{ ref('stg_aave_repays') }}
    union
    select chain from {{ ref('stg_aave_liquidations') }}
    union
    select chain from {{ ref('stg_chainlink_prices') }}
)

select
    sha2(lower(trim(chain)), 256) as chain_key,
    lower(trim(chain)) as chain,
    case lower(trim(chain))
        when 'arbitrum' then 'Arbitrum'
        else trim(chain)
    end as chain_name
from observed_chains
where chain is not null
