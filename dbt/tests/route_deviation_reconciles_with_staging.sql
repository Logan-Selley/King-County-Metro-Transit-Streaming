-- Every countable position lands in exactly one route-hour. "Countable" is the
-- contract from the model: not stale, has a route, has a deviation. Returns a
-- row when the totals disagree, which is how a dropped or doubled group shows.
--
-- SETTLED HOURS ARE CHOSEN BY position_at, NOT BY THE (service_date, local_hour)
-- KEY. The key is not ordered by time: service day 2026-09-23's hour 0 is
-- 00:00-01:00 on the 24th, because the owl service that began on the 23rd runs
-- past midnight, so a LOW local_hour can be the most recent hour in the table. A
-- first version excluded the two newest keys by key order, which left the
-- still-filling hour in and failed a scheduled run with a 1,500-row gap on
-- exactly that key. The model's own header warns about this trap; the test
-- walked into it anyway.
--
-- Two hours of margin, against a build that happens hourly. The mart is a table
-- and staging is a view over a live feed, so their numbers only agree over
-- positions old enough that no more can arrive: measured, the gap on the
-- still-filling hour was 1,500 rows in four minutes.
with settled_hours as (

    select distinct
        service_date || ':' || lpad(
            extract(hour from position_local)::integer::text, 2, '0') as hour_key
    from {{ ref('stg_vehicle_positions') }}
    where not is_stale_timestamp
      and route_short_name is not null
      and schedule_deviation_seconds is not null
      and position_at < date_trunc('hour', now()) - interval '2 hours'

),

expected as (

    select count(*) as n
    from {{ ref('stg_vehicle_positions') }}
    where not is_stale_timestamp
      and route_short_name is not null
      and schedule_deviation_seconds is not null
      and position_at < date_trunc('hour', now()) - interval '2 hours'

),

actual as (

    select coalesce(sum(positions), 0) as n
    from {{ ref('mart_route_deviation_hourly') }} m
    where m.service_date || ':' || lpad(m.local_hour::text, 2, '0')
          in (select hour_key from settled_hours)

)

select expected.n as expected, actual.n as actual
from expected, actual
where expected.n <> actual.n


