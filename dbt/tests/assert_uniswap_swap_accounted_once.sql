with memberships as (
    select event_id, 'canonical' as population
    from {{ ref('fact_uniswap_swap') }}

    union all

    select event_id, 'excluded' as population
    from {{ ref('int_uniswap_swap_exclusions') }}
),
accounting as (
    select
        staged.event_id,
        count(memberships.population) as population_count
    from {{ ref('stg_uniswap_swaps') }} as staged
    left join memberships on staged.event_id = memberships.event_id
    group by staged.event_id
)

select *
from accounting
where population_count != 1
