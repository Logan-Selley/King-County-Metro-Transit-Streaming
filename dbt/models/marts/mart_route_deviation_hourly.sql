{#
  Schedule deviation per route, per service day, per local hour.

  WHY NOT route_id IN THE GRAIN: grouping by route_id AND route_short_name
  opens two rows for one route when a route changes id across a service
  change, which the grain test then fails on.

  The question it answers: which routes run late, and when? Phase 2 measured
  a network-wide median of +105s; this breaks that number down to where it
  comes from.

  SHAPE (enforced by the contract in _marts.yml):
    route_short_name      text
    service_date          date     from stg_vehicle_positions, NOT position_at
    local_hour            integer  0-23, hour of position_local
    positions             integer
    median_deviation_s    integer
    pct_late_over_5min    numeric(5,2)  0-100, share of positions > +300s

  THE ONE TRAP NOT COVERED ABOVE: schedule_deviation_seconds is NULL when it
  could not be computed, and that is different from 0, which is a bus exactly on
  time (ADR 0005). The medians and the percentage are over non-NULL deviations,
  and `positions` counts those same rows, or pct_late gets diluted by every row
  that has no deviation to be late by.
#}

with staging as (

    -- The four filters here are also what tests/route_deviation_reconciles_
    -- with_staging.sql recomputes, so `positions` summing to its `expected`
    -- count is a contract rather than a coincidence. Every one of them has to
    -- match: a row dropped here and counted there is a reconciliation failure.
    select
        route_short_name,
        service_date,
        extract(hour from position_local)::integer as local_hour,
        schedule_deviation_seconds
    from {{ ref('stg_vehicle_positions') }}
    where not is_stale_timestamp
      and route_short_name is not null
      and schedule_deviation_seconds is not null

)

select
    route_short_name,
    service_date,
    local_hour,
    count(*)::integer as positions,
    -- Over non-NULL deviations only, which the WHERE clause already
    -- guarantees. A NULL here means "not computable" and a 0 means "exactly on
    -- time" (ADR 0005); averaging them together would report a bus that could
    -- not be scored as a bus that was on time.
    percentile_cont(0.5) within group (order by schedule_deviation_seconds)::integer
                      as median_deviation_s,
    -- Denominator is `positions`, i.e. the same non-NULL population. Using
    -- all rows in the hour would dilute the share with unscorable ones.
    (100.0 * count(*) filter (where schedule_deviation_seconds > 300)
        / count(*))::numeric(5, 2) as pct_late_over_5min
from staging
group by 1, 2, 3
