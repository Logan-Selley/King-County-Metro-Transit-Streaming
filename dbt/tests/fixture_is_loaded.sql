{{ config(enabled=var('require_fixture', false), severity='error') }}
{#
  CI ONLY (`--vars '{require_fixture: true}'`).

  Most of this project's spec tests return failing rows, so on an EMPTY
  warehouse they have nothing to return and pass. Measured by reading them:
  the owl-service, minute-coverage, grain and reconciliation tests all pass on
  zero rows, and feed_health_detects_overnight_stall is conditional on the
  night being loaded by design, because the production warehouse drops it
  after 90 days. Only assert_prediction_error_grows_with_lead fails when empty.

  So a CI build that loaded nothing, or loaded the wrong slice, would be green.
  This fails it instead, by checking that tests/fixtures/warehouse/ actually
  arrived with the properties the other tests depend on. It is a test about
  the TEST DATA, which is why it is off everywhere except CI: in production a
  missing 2026-09-23 night is retention working, not a broken load.
#}
with checks as (
    select
        'the 2026-09-23 night (owl service + stall) is not loaded' as failure,
        not exists (
            select 1 from {{ ref('stg_vehicle_positions') }}
            where position_local between '2026-09-23 02:00' and '2026-09-23 05:00'
        ) as failed
    union all
    select
        'no stale-timestamp rows, so the stale filter is never exercised',
        not exists (
            select 1 from {{ ref('stg_vehicle_positions') }} where is_stale_timestamp
        )
    union all
    select
        'the stall minute 2026-09-23 03:30 has positions, so the fixture is the wrong night',
        exists (
            select 1 from {{ ref('stg_vehicle_positions') }}
            where date_trunc('minute', position_local) = '2026-09-23 03:30'
        )
    union all
    select
        'no prediction-accuracy rows',
        not exists (select 1 from {{ ref('stg_prediction_accuracy') }})
    union all
    select
        'no bunching alerts',
        not exists (select 1 from {{ ref('stg_bunching_alerts') }})
)
select failure from checks where failed
