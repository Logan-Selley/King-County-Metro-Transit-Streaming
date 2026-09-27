{#
  Per (observed UTC day, lead bucket): the count, the sum of signed error and
  the smallest lead time. The three things mart_prediction_error_by_lead needs
  that ARE additive across days: predictions (a sum), mean_error_s (sum over
  count) and bucket_order (a min of mins). The histogram next to this carries
  the one that is not, the percentiles.

  Same incremental unit and restart rule as int_prediction_error_histogram,
  for the same reasons; see its header.
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
        lead_time_s,
        error_s
    from {{ ref('stg_prediction_accuracy') }}
    {% if is_incremental() %}
    where observed_at >= (select from_day from restart)::timestamp at time zone 'UTC'
    {% endif %}

)

select
    observed_day,
    lead_bucket,
    count(*)::bigint      as predictions,
    -- Whole seconds on every row, so this sum is exact in float8 far past any
    -- volume this table will see (2^53 seconds).
    sum(error_s)          as sum_error_s,
    min(lead_time_s)      as min_lead_time_s
from pairs
group by 1, 2
