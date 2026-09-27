-- The mart must have a row for EVERY minute staging spans, silent ones
-- included -- a silent minute is the entire point of the table, and it has no
-- rows in staging to group. Fails (returns one row) when the counts differ.
--
-- THE SPAN IS MEASURED INSIDE THE MART'S OWN WINDOW, not over the whole of
-- staging. The mart is a table and staging is a view over a live feed, so the
-- two are comparable only where the mart has stopped moving. Measured over the
-- whole of staging: 4,511 expected minutes against 4,510 in the mart, one
-- minute of positions having arrived between building the mart and running this
-- test. That gap was always reachable; adding the 4B marts made the build slow
-- enough to hit it on every run.
--
-- COUNTED INSIDE THE SPAN, not over the whole mart, since the mart became
-- incremental (2026-09-26). It now keeps minutes whose positions have aged
-- out of the raw table's 90-day retention, so a total row count would exceed
-- the span staging can still see. Counting mart minutes between the span's
-- first and last minute asks exactly the original question.
--
-- WINDOWED IN THE HOURLY RUN (var recent_hours; macros/recent_window.sql):
-- the span is measured over recent positions only. Unset, it is the full
-- history, which is what CI and `make dbt-full-check` run.
with mart_window as (

    select min(minute_utc) as lo,
           max(minute_utc) + interval '1 minute' as stop
    from {{ ref('mart_feed_health') }}
    where {{ recent_predicate('minute_utc') }}

),

span as (

    select date_trunc('minute', min(v.position_at)) as lo,
           date_trunc('minute', max(v.position_at)) as hi
    from {{ ref('stg_vehicle_positions') }} v
    cross join mart_window w
    where not v.is_stale_timestamp
      and v.position_at >= w.lo
      and v.position_at <  w.stop
      and {{ recent_predicate('v.position_at') }}

),

counted as (

    select count(*) as n
    from {{ ref('mart_feed_health') }} m
    cross join span s
    where m.minute_utc between s.lo and s.hi

)

select
    (extract(epoch from s.hi - s.lo) / 60)::integer + 1 as expected_minutes,
    c.n as mart_minutes
from span s
cross join counted c
where (extract(epoch from s.hi - s.lo) / 60)::integer + 1 <> c.n
