{#
  One row per (vehicle, GPS fix), typed and named for analysis. This is the
  worked example the other staging models follow.

  Staging does units, types and names, and nothing a question-specific mart
  would disagree about. Three decisions in here are not cosmetic, and each
  one is a bug this project already paid for once:

  1. LOCAL TIME IS AMERICA/LOS_ANGELES, converted once, here. Phase 2's
     service-date anchor used UTC midnight and was seven hours wrong on every
     record, not just across DST (findings, Phase 2). Every mart that says
     "hour of day" means Seattle's hour, and it gets it from this column
     instead of re-deriving it with a timezone literal of its own.

  2. SERVICE DATE COMES FROM start_date, NOT FROM THE TIMESTAMP. Metro's
     service day starts around 04:00-06:00 and 4.5% of trips cross midnight,
     so a 00:40 fix on a 23:50 trip belongs to the previous service day.
     start_date is GTFS YYYYMMDD on this table (enrichment strips the dashes;
     raw.trip_updates carries ISO -- see findings, "The two topics disagree
     about start_date"). Parsed with an explicit format so a format change
     fails here instead of silently producing NULLs.

  3. STALE TIMESTAMPS ARE FLAGGED, NOT DROPPED. The feed occasionally
     publishes a block of vehicles with timestamps days old (10 records
     stamped 2026-09-04, enriched on the 21st). ingest_lag_s is ~50s normally;
     anything over an hour is flagged. Flagging keeps them countable, which
     is what feed-health wants, while analytic marts filter them out.
#}

with source as (

    select * from {{ source('raw', 'enriched_vehicle_positions') }}

),

typed as (

    select
        vehicle_id,
        trip_id,
        route_id,
        route_short_name,
        direction_id,
        shape_id,
        block_id,
        stop_id,
        current_stop_sequence,
        current_status,
        occupancy_status,

        position_timestamp                                   as position_at,
        (position_timestamp at time zone 'America/Los_Angeles')
                                                             as position_local,
        to_date(start_date, 'YYYYMMDD')                      as service_date,

        enriched_at,
        extract(epoch from enriched_at - position_timestamp)::integer
                                                             as ingest_lag_s,

        latitude,
        longitude,
        shape_dist_traveled,
        schedule_deviation_seconds,
        neighborhood_name,

        static_feed_version,
        gtfs_feed_version

    from source

)

select
    *,
    ingest_lag_s > 3600 as is_stale_timestamp
from typed
