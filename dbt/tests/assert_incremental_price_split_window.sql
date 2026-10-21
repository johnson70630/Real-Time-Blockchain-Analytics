with events as (
    select column1::varchar as event_id,
        column2::timestamp_tz as event_timestamp
    from values
        ('before', '2026-01-01 09:57:00 +00:00'),
        ('prior_interval', '2026-01-01 10:00:00 +00:00'),
        ('selected_interval', '2026-01-01 10:05:00 +00:00'),
        ('after', '2026-01-01 10:10:00 +00:00')
),

changed_window as (
    select
        '2026-01-01 09:58:00 +00:00'::timestamp_tz
            as previous_feed_updated_at,
        '2026-01-01 10:03:00 +00:00'::timestamp_tz as feed_updated_at,
        '2026-01-01 10:10:00 +00:00'::timestamp_tz as next_feed_updated_at
),

actual as (
    select events.event_id
    from events
    cross join changed_window
    where events.event_timestamp >= changed_window.previous_feed_updated_at
        and events.event_timestamp < changed_window.next_feed_updated_at
),

expected as (
    select column1::varchar as event_id
    from values ('prior_interval'), ('selected_interval')
),

differences as (
    (select * from actual minus select * from expected)
    union all
    (select * from expected minus select * from actual)
)

select * from differences
