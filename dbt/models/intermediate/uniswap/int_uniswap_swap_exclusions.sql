select *
from {{ ref('int_uniswap_swap_classification') }}
where swap_classification = 'NONCANONICAL_OR_INCOMPATIBLE'
