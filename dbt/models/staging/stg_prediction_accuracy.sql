{#
  One row per (prediction, observed arrival) pair, from
  raw.prediction_accuracy. The grain the error curve is drawn at. Names, types
  and units only. Follows stg_vehicle_positions.

  Three things here are not cosmetic:

  1. THE SIGN OF error_s. Positive means the prediction said LATER than the bus
     actually arrived, so a rider who trusted the sign was still waiting when
     the bus left. consumers/prediction/accuracy.py states this convention and
     the JSON schema's description repeats it, because a flip inverts every
     conclusion the curve supports.

  2. abs_error_s IS STORED, NOT RECOMPUTED. It comes from the job, which
     computes it next to the raw error, so the two cannot come to disagree by
     one of them being regenerated here with a different NULL policy.

  3. start_date IS ISO, and it is parsed with an explicit format. This topic's
     start_date is YYYY-MM-DD while the enriched topic's is GTFS YYYYMMDD
     (findings: "The two topics disagree about start_date"). The explicit
     format makes a change fail here instead of producing NULLs, and the parsed
     column is named service_date to match stg_vehicle_positions.

  observed_at is the EVENT time and the partition key: when the bus actually
  arrived, which is the moment a prediction becomes checkable. The local-hour
  columns come from it, not from issued_at, so "hour of day" means the hour the
  bus was seen rather than the hour a sign was right or wrong about it.
#}

with source as (

    select * from {{ source('raw', 'prediction_accuracy') }}

),

typed as (

    select
        trip_id,
        stop_id,
        route_id,
        route_short_name,

        start_date,
        to_date(start_date, 'YYYY-MM-DD')                            as service_date,

        issued_at,
        predicted_arrival,
        observed_at,
        (observed_at at time zone 'America/Los_Angeles')             as observed_local,
        extract(hour from (observed_at at time zone 'America/Los_Angeles'))::integer
                                                                     as local_hour,

        -- lead_time_s is predicted_arrival minus issued_at, both from the
        -- PREDICTION: how far ahead the sign was talking. Negative for the
        -- restatements of an arrival already in the past, which the job keeps
        -- in their own bucket rather than clamping into "0-2m".
        lead_time_s,
        error_s,
        abs_error_s,
        lead_bucket

    from source

)

select * from typed

