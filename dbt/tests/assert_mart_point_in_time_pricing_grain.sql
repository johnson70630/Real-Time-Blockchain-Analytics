with counts as (
    select
        (
            select count(*)
            from {{ ref('int_defi_event_token_prices') }}
        ) as source_rows,
        (
            select count(*)
            from {{ ref('mart_point_in_time_pricing_tutorial') }}
        ) as mart_rows
),

duplicate_grain as (
    select
        event_domain,
        event_id,
        token_side
    from {{ ref('mart_point_in_time_pricing_tutorial') }}
    group by event_domain, event_id, token_side
    having count(*) != 1
)

select
    'row_count' as defect,
    cast(null as varchar) as event_domain,
    cast(null as varchar) as event_id,
    cast(null as varchar) as token_side
from counts
where source_rows != mart_rows

union all

select 'duplicate_grain', event_domain, event_id, token_side
from duplicate_grain
