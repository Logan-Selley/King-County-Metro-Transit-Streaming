{{ config(severity='error') }}

{#
  The two claims the error curve exists to support, asserted instead of
  eyeballed. The job's output shows them; if the warehouse cannot
  reproduce them, either the sink or the mart is wrong, and both are worse than
  a failing build.

    1. The typical mistake grows with lead time. Asserted between the two
       endpoint buckets rather than bucket to bucket: median |error| is not
       guaranteed to rise at every step (10-15m versus 15-20m can invert on a
       day's data), but 45-60m being worse than 0-2m is the whole shape of the
       finding.
    2. The sign runs optimistic at every FORWARD-looking lead. A single such
       bucket with a non-positive mean error would mean the sign flips
       somewhere, which inverts the reading of the chart.

  THE `past` BUCKET IS EXCLUDED from claim 2, and that is not a fudge. It holds
  negative lead time -- predictions restating an arrival that already happened
  -- so its mean error is negative by construction: measured, -283s against +11s
  for 0-2m. Including it would assert that the sign is optimistic about
  predictions of the past, which is a category error rather than a finding.

  An empty mart FAILS this on purpose. NULL endpoints would otherwise satisfy
  "not greater than" and pass vacuously, which is how a broken pipe looks like a
  green build.
#}

with mart as (

    select * from {{ ref('mart_prediction_error_by_lead') }}

),

endpoints as (

    select
        max(case when lead_bucket = '0-2m'   then median_abs_error_s end)  as near_median,
        max(case when lead_bucket = '45-60m' then median_abs_error_s end)  as far_median,
        count(*) filter (
            where lead_bucket <> 'past' and mean_error_s <= 0
        )                                                                  as pessimistic_buckets
    from mart

)

select * from endpoints
where near_median is null
   or far_median is null
   or far_median <= near_median
   or pessimistic_buckets > 0
