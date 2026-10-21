with events as (
    select 'event-1005' as event_id,
        '2026-01-01 10:05:00 +00:00'::timestamp_tz as event_timestamp
),

old_prices as (
    select column1::timestamp_tz as feed_updated_at
    from values
        ('2026-01-01 09:58:00 +00:00'),
        ('2026-01-01 10:10:00 +00:00')
),

recovered_prices as (
    select column1::timestamp_tz as feed_updated_at
    from values
        ('2026-01-01 09:58:00 +00:00'),
        ('2026-01-01 10:03:00 +00:00'),
        ('2026-01-01 10:10:00 +00:00')
),

assignments as (
    select
        events.event_id,
        (
            select max(feed_updated_at)
            from old_prices
            where feed_updated_at <= events.event_timestamp
        ) as old_assignment,
        (
            select max(feed_updated_at)
            from recovered_prices
            where feed_updated_at <= events.event_timestamp
        ) as recovered_assignment
    from events
)

select *
from assignments
where old_assignment != '2026-01-01 09:58:00 +00:00'::timestamp_tz
    or recovered_assignment != '2026-01-01 10:03:00 +00:00'::timestamp_tz
    or recovered_assignment > '2026-01-01 10:05:00 +00:00'::timestamp_tz
