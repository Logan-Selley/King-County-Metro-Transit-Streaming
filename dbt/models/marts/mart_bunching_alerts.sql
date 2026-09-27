{#
  One row per bunching alert, placed on the map. Feeds the findings site: the
  map, the by-hour chart and the route ranking all cut from this one table, so
  they cannot disagree about how many alerts there were.

  GRAIN: the alert, keyed as the sink keys it (vehicle_id_a, vehicle_id_b,
  window_at). mart_bunching_by_route_hour stays as it is; it aggregates all
  history by route and hour and answers a different question.

  SHAPE (enforced by the contract in _marts.yml):

    window_at            the alert's event time (stg_bunching_alerts.window_at)
    local_date           Pacific calendar date of window_at
    local_hour           Pacific hour, 0-23
    day_type             'weekday' | 'saturday' | 'sunday', from local_date.
                         Calendar days, not GTFS service days: an alert at
                         00:40 Saturday is 'saturday' even though its trip
                         belongs to Friday's service. Named for what it is,
                         the same call stg_bunching_alerts makes for local_date.
    route_short_name, direction_id, vehicle_id_a, vehicle_id_b, gap_ft
                         straight from staging
    latitude, longitude  the MIDPOINT of the two vehicles' positions: each
                         vehicle's LAST position on its alerted trip
                         (trip_id_a / trip_id_b) inside the alert's window,
                         [window_at - 60 s, window_at). The window is the
                         detector's tumbling window (config.window_s = 60), so
                         these are positions the detector actually saw.
    min_stop_sequence    the lower current_stop_sequence of those two positions
    stop_id, stop_name   the stop of the vehicle with the lower sequence (ties:
                         vehicle a). stop_name from static.stops, latest
                         version_id per stop_id.
    neighborhood_name    that same vehicle's neighborhood

  AN ALERT IS NEVER DROPPED. The positions are found with LEFT joins, so an
  alert that cannot be placed keeps its row with null locations. Dropping it
  would make the map's total disagree with every other count of alerts, and
  tests/bunching_alerts_located_when_the_feed_was_live.sql is what states which
  alerts are expected to have a location.

  THE SHAPE OF THE LOOKUP DECIDES ITS COST. raw.enriched_vehicle_positions'
  partitions are keyed by a b-tree on (vehicle_id, position_timestamp), so
  "this vehicle's LAST position before window_at" (order by position_timestamp
  desc limit 1, in a lateral join) is an index probe per vehicle. Counting the
  window's positions instead, in a plain join, is not served from that index
  and costs about seven times as much per alert. The raw table is the probe's
  source rather than stg_vehicle_positions because the probe needs nothing
  staging adds and does need that index.

  INCREMENTAL ON window_at, with a lookback covering the detector's lateness
  (allowed_lateness_s = 360, and the sink lands an alert seconds after its
  window closes). A lower bound self-heals: a build that has been failing for
  days picks up everything after its own newest row on the next success,
  because the bound is derived from this table rather than from now().
#}

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='window_at',
    on_schema_change='fail',
) }}

with

{% if is_incremental() %}
restart as (

    -- 15 minutes, against a lateness bound of 6: wide enough that no alert can
    -- land behind it, narrow enough that an hourly run touches tens of alerts.
    select coalesce(max(window_at) - interval '15 minutes',
                    '-infinity'::timestamptz) as from_at
    from {{ this }}

),
{% endif %}

{% if is_incremental() %}
unplaced as (

    -- AN ALERT BUILT BEFORE ITS POSITIONS LANDED IS RETRIED. Positions and
    -- alerts arrive through separate connectors, so if the positions sink
    -- falls behind, a build can place an alert with nothing to place it by,
    -- and the lookback above would never revisit it. These rows are rebuilt
    -- until they are placed. Two days, because an alert raised while the feed
    -- was silent can never be placed, and unbounded those rows would be
    -- retried every hour forever; a sink down longer than that wants a
    -- --full-refresh anyway. Normally this is empty: 2,719 of 2,719 live
    -- alerts were placed on 2026-09-27.
    select distinct window_at
    from {{ this }}
    where latitude is null
      and window_at >= (select max(window_at) from {{ this }}) - interval '2 days'

),
{% endif %}

alerts as (

    select * from {{ ref('stg_bunching_alerts') }}
    {% if is_incremental() %}
    -- delete+insert on window_at replaces every alert sharing a retried
    -- window_at, and this selects all of them again, so no neighbour is lost.
    where window_at >= (select from_at from restart)
       or window_at in (select window_at from unplaced)
    {% endif %}

),

placed as (

    select
        a.window_at,
        a.local_date,
        a.local_hour,
        a.route_short_name,
        a.direction_id,
        a.vehicle_id_a,
        a.vehicle_id_b,
        a.gap_ft,

        a_pos.latitude               as lat_a,
        a_pos.longitude              as lon_a,
        a_pos.current_stop_sequence  as seq_a,
        a_pos.stop_id                as stop_a,
        a_pos.neighborhood_name      as hood_a,

        b_pos.latitude               as lat_b,
        b_pos.longitude              as lon_b,
        b_pos.current_stop_sequence  as seq_b,
        b_pos.stop_id                as stop_b,
        b_pos.neighborhood_name      as hood_b,

        -- Which vehicle is nearer its trip's start, and therefore whose stop
        -- the alert is reported at. Ties go to vehicle a, and a vehicle whose
        -- position was not found never wins.
        case
            when a_pos.current_stop_sequence is null then false
            when b_pos.current_stop_sequence is null then true
            else a_pos.current_stop_sequence <= b_pos.current_stop_sequence
        end as nearer_is_a

    from alerts a

    left join lateral (

        select p.latitude, p.longitude, p.current_stop_sequence, p.stop_id,
               p.neighborhood_name
        from {{ source('raw', 'enriched_vehicle_positions') }} p
        where p.vehicle_id = a.vehicle_id_a
          and p.trip_id = a.trip_id_a
          and p.position_timestamp >= a.window_at - interval '60 seconds'
          and p.position_timestamp < a.window_at
        order by p.position_timestamp desc
        limit 1

    ) a_pos on true

    left join lateral (

        select p.latitude, p.longitude, p.current_stop_sequence, p.stop_id,
               p.neighborhood_name
        from {{ source('raw', 'enriched_vehicle_positions') }} p
        where p.vehicle_id = a.vehicle_id_b
          and p.trip_id = a.trip_id_b
          and p.position_timestamp >= a.window_at - interval '60 seconds'
          and p.position_timestamp < a.window_at
        order by p.position_timestamp desc
        limit 1

    ) b_pos on true

),

stops as (

    select distinct on (stop_id) stop_id, stop_name
    from {{ source('static', 'stops') }}
    order by stop_id, version_id desc

)

select
    p.window_at,
    p.local_date,
    p.local_hour,
    case extract(dow from p.local_date)::integer
        when 6 then 'saturday'
        when 0 then 'sunday'
        else 'weekday'
    end::text as day_type,

    p.route_short_name,
    p.direction_id,
    p.vehicle_id_a,
    p.vehicle_id_b,
    p.gap_ft,

    -- A midpoint needs both endpoints; one position is not half a midpoint.
    case when p.lat_a is not null and p.lat_b is not null
         then (p.lat_a + p.lat_b) / 2 end::double precision as latitude,
    case when p.lon_a is not null and p.lon_b is not null
         then (p.lon_a + p.lon_b) / 2 end::double precision as longitude,

    case when p.nearer_is_a then p.seq_a else p.seq_b end::integer
                      as min_stop_sequence,
    case when p.nearer_is_a then p.stop_a else p.stop_b end::text
                      as stop_id,
    s.stop_name,
    case when p.nearer_is_a then p.hood_a else p.hood_b end::text
                      as neighborhood_name

from placed p
left join stops s
  on s.stop_id = case when p.nearer_is_a then p.stop_a else p.stop_b end
