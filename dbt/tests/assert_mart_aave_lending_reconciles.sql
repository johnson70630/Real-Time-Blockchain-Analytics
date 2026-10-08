with counts as (
    select
        (select count(*) from {{ ref('fact_aave_borrow') }}) as borrow_rows,
        (select count(*) from {{ ref('fact_aave_repay') }}) as repay_rows,
        (select count(*) from {{ ref('mart_aave_lending_tutorial') }}) as mart_rows,
        (
            select count(distinct event_id)
            from {{ ref('mart_aave_lending_tutorial') }}
        ) as distinct_mart_events
)
select *
from counts
where borrow_rows + repay_rows != mart_rows
    or mart_rows != distinct_mart_events
