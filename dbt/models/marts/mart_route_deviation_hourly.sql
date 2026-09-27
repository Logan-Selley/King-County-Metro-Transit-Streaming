{#
  Schedule deviation per route, per service day, per local hour.

  WHY NOT route_id IN THE GRAIN: grouping by route_id AND route_short_name
  opens two rows for one route when a route changes id across a service
  change, which the grain test then fails on.

  The question it answers: which routes run late, and when? The network-wide
  median deviation is +105s; this breaks that number down to where it
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

{#
  INCREMENTAL BY WHOLE SERVICE DAY. Measured 2026-09-26: 536 s of every hourly
  build as a full rebuild over 6.2M positions, growing ~730 MB a day.

  THE UNIT IS THE SERVICE DAY, NOT THE HOUR, because the grain is
  (route_short_name, service_date, local_hour) and an hour is not a clean unit
  of it. At the November fall-back, local hour 1 happens twice in one service
  day, so two UTC hours feed one row; recomputing by UTC hour would write that
  row twice. Recomputing a whole service_date and replacing it
  (delete+insert on service_date) cannot split a row, because every row lives
  inside exactly one service day.

  WHICH DAYS: from the day before this table's newest service_date onward.
  Yesterday is included because trips cross midnight (4.5% of them, findings
  section 10): a 00:40 fix belongs to the previous service day, so that day is
  still receiving rows after midnight. Relative to the table's own newest day
  rather than now(), so a build that has been failing for days catches up on
  its next success instead of skipping the gap.

  WHY THE position_at FILTER as well as service_date. Only position_at reaches
  raw.enriched_vehicle_positions' daily partitions (service_date is derived
  from start_date), so it is the filter that keeps the scan to a couple of
  days. A service day's positions all fall on or after that date's local
  midnight; the extra day is margin, not a requirement. service_date then picks
  exactly the recomputed days, so no partial day is written.

  Cost is now ~2 service days per build whatever the retention, which is the
  point.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='service_date',
    on_schema_change='fail',
) }}

with
{% if is_incremental() %}
restart as (

    select coalesce(max(service_date) - 1, '-infinity'::date) as from_date
    from {{ this }}

),
{% endif %}

staging as (

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
    {% if is_incremental() %}
      and position_at >= ((select from_date from restart)::timestamp
                          at time zone 'America/Los_Angeles') - interval '1 day'
      and service_date >= (select from_date from restart)
    {% endif %}

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
