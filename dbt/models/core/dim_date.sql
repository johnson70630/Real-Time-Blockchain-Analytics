with source_dates as (
    select block_timestamp::date as activity_date from {{ ref('stg_uniswap_swaps') }}
    union all
    select block_timestamp::date from {{ ref('stg_aave_borrows') }}
    union all
    select block_timestamp::date from {{ ref('stg_aave_repays') }}
    union all
    select block_timestamp::date from {{ ref('stg_aave_liquidations') }}
    union all
    select price_date from {{ ref('stg_chainlink_prices') }}
),

bounds as (
    select
        dateadd(year, -1, coalesce(min(activity_date), current_date())) as start_date,
        dateadd(
            year,
            2,
            greatest(coalesce(max(activity_date), current_date()), current_date())
        ) as end_date
    from source_dates
),

date_candidates as (
    select
        dateadd(day, generated_days.day_offset, bounds.start_date)::date as calendar_date,
        bounds.end_date
    from bounds
    cross join (
        select row_number() over (order by seq4()) - 1 as day_offset
        from table(generator(rowcount => 20000))
    ) as generated_days
)

select
    to_number(to_char(calendar_date, 'YYYYMMDD')) as date_key,
    calendar_date as date,
    year(calendar_date) as year,
    quarter(calendar_date) as quarter,
    month(calendar_date) as month,
    monthname(calendar_date) as month_name,
    day(calendar_date) as day,
    dayofweekiso(calendar_date) as day_of_week,
    dayname(calendar_date) as day_name,
    weekiso(calendar_date) as week_of_year
from date_candidates
where calendar_date <= end_date
