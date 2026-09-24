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
with mart_window as (

    select min(minute_utc) as lo,
           max(minute_utc) + interval '1 minute' as stop
    from {{ ref('mart_feed_health') }}

),

span as (

    select date_trunc('minute', min(v.position_at)) as lo,
           date_trunc('minute', max(v.position_at)) as hi
    from {{ ref('stg_vehicle_positions') }} v
    cross join mart_window w
    where not v.is_stale_timestamp
      and v.position_at >= w.lo
      and v.position_at <  w.stop

)

select
    (extract(epoch from hi - lo) / 60)::integer + 1 as expected_minutes,
    (select count(*) from {{ ref('mart_feed_health') }}) as mart_minutes
from span
where (extract(epoch from hi - lo) / 60)::integer + 1
      <> (select count(*) from {{ ref('mart_feed_health') }})

