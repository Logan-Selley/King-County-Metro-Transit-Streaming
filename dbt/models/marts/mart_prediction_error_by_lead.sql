{#
  THE CHART: how wrong is Metro's arrival estimate, by how far ahead it was
  issued. One row per lead bucket.

  SHAPE: lead_bucket, bucket_order, predictions, median_abs_error_s,
  p90_abs_error_s, mean_error_s.

  The answer is already known from Phase 3 (findings, "Phase 3 close-out"):
  median |error| rises 43s -> 199s from 0-2m to 45-60m, and the mean is positive
  everywhere, meaning buses arrive EARLIER than the sign said. This mart
  reproduces that from the warehouse instead of from the job's stdout.

  bucket_order IS min(lead_time_s), AND THAT IS THE POINT. The obvious
  implementation is a CASE over the nine labels, which would be a second copy
  of consumers/prediction/config.py's LEAD_BUCKET_LABELS -- a copy that a
  relabelled bucket would silently break (the labels would still sort, just in
  the wrong order). The minimum lead time in each bucket is monotonic with the
  bucket order by construction, so the ordering cannot drift from the Python
  definitions at all. A bucket label added there shows up here sorted correctly
  without an edit.

  INTERVAL BOUNDS COME FROM THE JOB, never from here. lead_bucket is computed
  in consumers/prediction/accuracy.py, where the offsets against
  LEAD_BUCKETS_S are unit-tested, so the warehouse does not re-derive them.

  MEAN AND MEDIAN ARE BOTH HERE because they disagree. The mean of error_s says
  which way the sign errs (positive: the sign is optimistic), while the median
  of |error| says how big a typical mistake is. A chart with only one of them
  can say "the sign is 40 seconds optimistic" about a distribution whose median
  mistake is 90 seconds, and both are true.
#}

with pairs as (

    select * from {{ ref('stg_prediction_accuracy') }}

),

by_bucket as (

    select
        lead_bucket,

        min(lead_time_s)::integer                                        as bucket_order,
        count(*)::integer                                                as predictions,

        round(percentile_cont(0.5) within group (order by abs_error_s))::integer
                                                                         as median_abs_error_s,
        round(percentile_cont(0.9) within group (order by abs_error_s))::integer
                                                                         as p90_abs_error_s,
        round(avg(error_s))::integer                                     as mean_error_s

    from pairs
    group by 1

)

select * from by_bucket
order by bucket_order

