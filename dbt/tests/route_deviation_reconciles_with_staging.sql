-- Every countable position lands in exactly one route-hour. "Countable" is the
-- contract from the model: not stale, has a route, has a deviation. Returns one
-- row per (service_date, local_hour) whose totals disagree, which is how a
-- dropped or doubled group shows, and names the key that did it.
--
-- A KEY IS NOT ONE CLOCK HOUR. (service_date, local_hour) is not ordered by
-- time, and it is not even contiguous: service date 2026-09-24's hour 3 holds
-- positions from 03:xx on the 24th (the start of the service day) AND from 03:xx
-- on the 25th, from late trips still carrying start_date 20260924. Measured on
-- 2026-09-26: key (09-24, 5) held 187 positions on the 25th and 35,441 on the
-- 24th. Two earlier versions of this test fell into that gap:
--   * The first excluded the two newest keys by key order, which left the
--     still-filling hour in: a 1,500-row gap on a scheduled run.
--   * The second chose settled positions by `position_at < now() - 2h`. A key
--     whose morning half was settled and whose next-day half was still arriving
--     counted as settled, with the mart holding both halves and staging only
--     the old one. That failed the 13:15 UTC run on 2026-09-26 (06:15 local,
--     when late owl trips are running), both attempts.
--   A 48-hour window by position_at then cut keys at the OTHER edge the same
--   way, and failed the 20:15 run on three keys of 09-24.
--
-- So each key is judged whole. SETTLED means the key's NEWEST countable
-- position is more than two hours old: the mart is a table and staging a view
-- over a live feed, so they only agree once nothing more can arrive (measured:
-- 1,500 rows in four minutes on the still-filling hour). And the window, when
-- there is one, is in whole service days, so a key is entirely in or entirely
-- out of it.
--
-- WINDOWED IN THE HOURLY RUN (var recent_hours; macros/recent_window.sql),
-- rounded up to whole service days: 6 hours becomes yesterday and today.
-- Unset, it reconciles the full history, which is what CI and
-- `make dbt-full-check` run.
{%- set hours = recent_hours() %}
{%- if hours is not none %}
    {%- set window_days = ((hours | int) + 23) // 24 %}
    {%- set from_date -%}
        ((now() at time zone 'America/Los_Angeles')::date - {{ window_days }})
    {%- endset %}
{%- endif %}

with staged as (

    select
        service_date,
        extract(hour from position_local)::integer as local_hour,
        count(*)                                   as positions,
        max(position_at)                           as newest_at
    from {{ ref('stg_vehicle_positions') }}
    where not is_stale_timestamp
      and route_short_name is not null
      and schedule_deviation_seconds is not null
      {%- if hours is not none %}
      and service_date >= {{ from_date }}
      -- Only for partition pruning; the service_date bound above is the
      -- window. A trip starting on day D reports no earlier than D's local
      -- midnight, and a day of slack covers the mart model's own restart
      -- filter, which uses the same bound.
      and position_at >= ({{ from_date }}::timestamp at time zone 'America/Los_Angeles')
                         - interval '1 day'
      {%- endif %}
    group by 1, 2

),

settled as (

    select *
    from staged
    where newest_at < date_trunc('hour', now()) - interval '2 hours'

),

marted as (

    select service_date, local_hour, sum(positions) as positions
    from {{ ref('mart_route_deviation_hourly') }}
    group by 1, 2

)

select
    s.service_date,
    s.local_hour,
    s.positions as expected,
    m.positions as actual
from settled s
left join marted m using (service_date, local_hour)
where m.positions is distinct from s.positions
