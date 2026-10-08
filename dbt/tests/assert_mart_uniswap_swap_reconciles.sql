with counts as (
    select
        (select count(*) from {{ ref('fact_uniswap_swap') }}) as fact_rows,
        (select count(*) from {{ ref('mart_uniswap_swap_tutorial') }}) as mart_rows,
        (
            select count(distinct event_id)
            from {{ ref('mart_uniswap_swap_tutorial') }}
        ) as distinct_mart_events
)
select *
from counts
where fact_rows != mart_rows
    or mart_rows != distinct_mart_events
