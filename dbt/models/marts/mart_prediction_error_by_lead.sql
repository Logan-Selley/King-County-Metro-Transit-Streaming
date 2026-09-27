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

{#
  FROM THE HISTOGRAM, NOT THE ROWS. Measured 2026-09-26: 1,090 s of every
  hourly build, the single most expensive node, because percentile_cont had
  to sort all 8.8M resolved predictions each time. Now it reads
  int_prediction_error_histogram (value counts) and int_prediction_bucket_daily
  (additive totals), a few thousand rows per bucket.

  THE PERCENTILE IS percentile_cont's OWN DEFINITION, rebuilt from counts. For
  n sorted values v[0..n-1] and fraction p, percentile_cont returns
  v[lo] + (r - lo) * (v[hi] - v[lo]) with r = p * (n - 1), lo = floor(r),
  hi = ceil(r). In a histogram sorted by value, with `upto` the running count,
  the value at 0-based position k is the one whose [upto - count, upto) range
  contains k. Same arithmetic in float8 as the aggregate uses, so the result
  matches it exactly, which the before/after comparison checked on the live
  warehouse (see the incremental change's notes).
#}
with hist as (

    select lead_bucket, abs_error_s, sum(predictions) as n
    from {{ ref('int_prediction_error_histogram') }}
    group by 1, 2

),

ranked as (

    select
        lead_bucket,
        abs_error_s,
        sum(n) over (partition by lead_bucket order by abs_error_s) as upto,
        n
    from hist

),

positions as (

    select
        lead_bucket,
        sum(n)                          as total,
        0.5::float8 * (sum(n) - 1)      as r50,
        0.9::float8 * (sum(n) - 1)      as r90
    from hist
    group by 1

),

percentiles as (

    select
        p.lead_bucket,
        {% for name, r in [('median', 'r50'), ('p90', 'r90')] %}
        (select v.abs_error_s from ranked v
          where v.lead_bucket = p.lead_bucket
            and floor(p.{{ r }}) >= v.upto - v.n and floor(p.{{ r }}) < v.upto)::float8
        + (p.{{ r }} - floor(p.{{ r }}))
        * ((select v.abs_error_s from ranked v
             where v.lead_bucket = p.lead_bucket
               and ceil(p.{{ r }}) >= v.upto - v.n and ceil(p.{{ r }}) < v.upto)
           - (select v.abs_error_s from ranked v
               where v.lead_bucket = p.lead_bucket
                 and floor(p.{{ r }}) >= v.upto - v.n and floor(p.{{ r }}) < v.upto))
                                                        as {{ name }}_abs_error{{ ',' if not loop.last }}
        {% endfor %}
    from positions p

),

totals as (

    select
        lead_bucket,
        min(min_lead_time_s)                as bucket_order,
        sum(predictions)                    as predictions,
        sum(sum_error_s) / sum(predictions) as mean_error
    from {{ ref('int_prediction_bucket_daily') }}
    group by 1

),

by_bucket as (

    select
        t.lead_bucket,
        t.bucket_order::integer                   as bucket_order,
        t.predictions::integer                    as predictions,
        round(p.median_abs_error)::integer        as median_abs_error_s,
        round(p.p90_abs_error)::integer           as p90_abs_error_s,
        round(t.mean_error)::integer              as mean_error_s
    from totals t
    join percentiles p using (lead_bucket)

)

select * from by_bucket
order by bucket_order
