with counts as (
    select
        (select count(*) from {{ ref('fact_market_price') }}) as fact_rows,
        (
            select count(*)
            from {{ ref('mart_chainlink_oracle_tutorial') }}
        ) as mart_rows,
        (
            select count(distinct observation_id)
            from {{ ref('mart_chainlink_oracle_tutorial') }}
        ) as distinct_observations
)
select *
from counts
where fact_rows != mart_rows
    or mart_rows != distinct_observations
