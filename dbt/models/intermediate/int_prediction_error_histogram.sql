{#
  How many predictions per (observed day, lead bucket, whole seconds of
  |error|). What mart_prediction_error_by_lead computes its median and p90
  from, instead of re-sorting every prediction ever resolved.

  WHY A HISTOGRAM. A median cannot be updated incrementally from partial
  medians, so the mart's percentile_cont had to sort every row of
  raw.prediction_accuracy on every build: 1,090 s of each hourly build on
  2026-09-26, over 8.8M rows (2.2 GB) growing ~3M rows a day, with
  work_mem at 4 MB so the sort spilled to the spinning disk. A count per
  distinct value CAN be added up across days, and percentile_cont over a
  multiset is a function of its value counts alone.

  EXACT, NOT BINNED, and that rests on a measurement: abs_error_s, error_s
  and lead_time_s are whole seconds on every row (checked over 09-25: zero
  non-integer values), because each is a difference of two epoch-second
  timestamps. So the histogram keys on the exact value and loses nothing; the
  mart's reconstruction is the same number percentile_cont gives. The widest
  bucket-day held 1,643 distinct values, so this adds ~15k small rows a day
  against ~3M source rows.

  INCREMENTAL BY OBSERVED UTC DAY, recomputing from the day before this
  table's newest onward (delete+insert on observed_day). raw.prediction_accuracy
  is partitioned by observed_at, so the filter prunes to about two days. A
  resolved prediction is written once, when the arrival is observed, and the
  sink upserts on a key that contains observed_at, so older days do not
  change. Rows restated into an old day by a connector replay would be missed
  until `dbt build --full-refresh -s +mart_prediction_error_by_lead`.
#}
{{ config(
    incremental_strategy='delete+insert',
    unique_key='observed_day',
    on_schema_change='fail',
) }}

with
{% if is_incremental() %}
restart as (

    select coalesce(max(observed_day) - 1, '-infinity'::date) as from_day
    from {{ this }}

),
{% endif %}

pairs as (

    select
        (observed_at at time zone 'UTC')::date as observed_day,
        lead_bucket,
        abs_error_s
    from {{ ref('stg_prediction_accuracy') }}
    {% if is_incremental() %}
    where observed_at >= (select from_day from restart)::timestamp at time zone 'UTC'
    {% endif %}

)

select
    observed_day,
    lead_bucket,
    -- integer, because every value is whole seconds (see the header).
    abs_error_s::integer                          as abs_error_s,
    count(*)::bigint                              as predictions,
    -- The cast above would silently round if the job ever emitted fractions,
    -- and the mart's percentiles would stop being exact without anything
    -- failing. This counts them; tests/prediction_histogram_is_exact.sql
    -- fails the build on any.
    count(*) filter (where abs_error_s <> round(abs_error_s))::bigint
                                                  as fractional_values
from pairs
group by 1, 2, 3
