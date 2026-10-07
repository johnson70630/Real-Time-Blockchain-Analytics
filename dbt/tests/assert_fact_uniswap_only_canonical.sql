select facts.event_id
from {{ ref('fact_uniswap_swap') }} as facts
left join {{ ref('int_uniswap_swap_classification') }} as classified
    on facts.event_id = classified.event_id
where classified.swap_classification != 'CANONICAL_UNISWAP_V3'
    or classified.event_id is null
