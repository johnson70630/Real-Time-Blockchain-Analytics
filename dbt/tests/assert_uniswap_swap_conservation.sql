with counts as (
    select
        (select count(*) from {{ ref('stg_uniswap_swaps') }}) as staged_rows,
        (select count(*) from {{ ref('fact_uniswap_swap') }}) as canonical_rows,
        (select count(*) from {{ ref('int_uniswap_swap_exclusions') }}) as excluded_rows
)

select *
from counts
where staged_rows != canonical_rows + excluded_rows
