{{ config(severity='error') }}
{#
  The error-curve mart reconstructs percentile_cont exactly from
  int_prediction_error_histogram, which keys on WHOLE seconds of |error|. That
  is exact only while every value is a whole second, measured true on
  2026-09-25 because each error is a difference of epoch-second timestamps. If
  the prediction job ever emits fractions, the histogram's integer cast rounds
  them and the published medians drift without anything else failing. This
  fails first. Scoped to the histogram, so it costs nothing to run hourly.
#}
select observed_day, lead_bucket, sum(fractional_values) as fractional_values
from {{ ref('int_prediction_error_histogram') }}
group by 1, 2
having sum(fractional_values) > 0
