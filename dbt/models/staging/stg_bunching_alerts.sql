{#
  One row per bunched pair per event-time window, from raw.bunching_alerts.
  Names, types and units, and nothing a question-specific mart would disagree
  about. Follows stg_vehicle_positions.

  Three things here are not cosmetic:

  1. gap_ft IS FEET, and the column name carries the unit because of it. The
     detector compares along-route distance in FEED units, which is feet for
     423 of 424 shapes (consumers/bunching/config.py, and the unit note in
     detect.py). A model that renamed this gap_m would be wrong by 3.28x on
     every row and would still pass a not_null test.

  2. LOCAL TIME IS AMERICA/LOS_ANGELES, converted once, here, for the reason
     stg_vehicle_positions spells out: "hour of day" means Seattle's hour.

  3. window_at IS THE EVENT TIME, NOT THE ARRIVAL TIME. The alert is emitted
     when its window closes, so in live operation the two are seconds apart --
     but the topic was re-ingested once, which backfilled roughly a day of
     alerts in a few minutes. A mart grouping by ingest time would pile a day
     of history into one hour and still look plausible.

  There is no service_date column because the alert carries none: Metro's
  service day comes from GTFS start_date, and a bunched pair does not say which
  trip's day it belongs to. local_date is the LOCAL CALENDAR DATE of the
  window, which is not the same thing for an alert at 00:40, and it is named
  for what it is.
#}

with source as (

    select * from {{ source('raw', 'bunching_alerts') }}

),

typed as (

    select
        route_id,
        route_short_name,
        direction_id,
        vehicle_id_a,
        vehicle_id_b,
        trip_id_a,
        trip_id_b,

        gap_ft,

        window_end                                                   as window_at,
        (window_end at time zone 'America/Los_Angeles')               as window_local,
        (window_end at time zone 'America/Los_Angeles')::date         as local_date,
        extract(hour from (window_end at time zone 'America/Los_Angeles'))::integer
                                                                     as local_hour,

        -- Seconds late per Metro's own estimate, NULL when it could not be
        -- computed, which is not zero (ADR 0005). Both are kept: one bus eight
        -- minutes late is a different rider experience from two both on time.
        deviation_a,
        deviation_b

    from source

)

select * from typed

