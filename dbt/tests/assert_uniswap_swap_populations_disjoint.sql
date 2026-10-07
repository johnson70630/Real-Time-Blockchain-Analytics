select canonical.event_id
from {{ ref('fact_uniswap_swap') }} as canonical
join {{ ref('int_uniswap_swap_exclusions') }} as excluded
    on canonical.event_id = excluded.event_id
