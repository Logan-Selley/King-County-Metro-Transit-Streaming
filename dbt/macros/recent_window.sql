{#
  THE HOURLY RUN TESTS WHAT CHANGED, not the whole history. Measured on
  2026-09-26 from target/run_results.json: of 5,102 node-seconds in one hourly
  build (1,628 s wall clock on 4 threads), about 2,250 went to seven tests that
  each scan a staging VIEW end to end, which means all of
  raw.enriched_vehicle_positions (6.2M rows) or raw.prediction_accuracy
  (8.8M), on a spinning disk: not_null_stg_vehicle_positions_service_date alone
  took 532 s. At 90 days of retention that is ~36 GB and ~65 GB per test, every
  hour.

  So the hourly DAG runs `dbt build --vars '{recent_hours: 6}'`, and the tests
  over the big tables look only at event times inside that window. Rows older
  than 6 hours were already tested by the builds that saw them arrive, and
  nothing rewrites them: the sinks upsert on the event-time key, and the marts
  only recompute a recent tail (see the incremental marts).

  UNSET MEANS FULL HISTORY, and that default is deliberate. CI builds against
  a committed fixture from 2026-09-23, so a window relative to now() would
  filter every fixture row out and the tests would pass on nothing, which is
  the failure dbt/tests/fixture_is_loaded.sql exists to prevent. `make dbt`,
  `make ci-dbt` and `make dbt-full-check` all run without the var.
#}

{% macro recent_hours() -%}
    {{ return(var('recent_hours', none)) }}
{%- endmacro %}


{#
  A predicate on `column` for the hourly window, or `true` when running over
  the full history. For singular tests, which write their own SQL.
#}
{% macro recent_predicate(column) -%}
    {%- set hours = recent_hours() -%}
    {%- if hours is none -%}
        true
    {%- else -%}
        {{ column }} >= now() - interval '{{ hours | int }} hours'
    {%- endif -%}
{%- endmacro %}


{#
  The event-time column each large relation is scoped by. Only the relations
  whose tests are expensive: the bunching tables hold a few thousand rows and
  are tested in full every hour for free.
#}
{% macro recent_scope_column(identifier) -%}
    {%- set columns = {
        'stg_vehicle_positions': 'position_at',
        'stg_prediction_accuracy': 'observed_at',
        'mart_feed_health': 'minute_utc',
    } -%}
    {{ return(columns.get(identifier)) }}
{%- endmacro %}


{#
  Overrides dbt's own default__get_where_subquery (dbt-core 1.12.5,
  macros/materializations/tests/where_subquery.sql), which every generic test
  (not_null, unique, accepted_values) calls to get the relation it selects
  from. Scoping here covers every generic test on these models at once, and a
  test added later is scoped without anyone remembering to.

  A test's own `where` config still applies, ANDed with the window.
#}
{% macro default__get_where_subquery(relation) -%}
    {%- set where = config.get('where', '') -%}
    {%- set column = recent_scope_column(relation.identifier) -%}
    {%- set clauses = [] -%}
    {%- if where -%}{%- do clauses.append('(' ~ where ~ ')') -%}{%- endif -%}
    {%- if column and recent_hours() is not none -%}
        {%- do clauses.append(recent_predicate(column)) -%}
    {%- endif -%}
    {%- if clauses -%}
        {%- set filtered -%}
            (select * from {{ relation }} where {{ clauses | join(' and ') }}) dbt_subquery
        {%- endset -%}
        {%- do return(filtered) -%}
    {%- else -%}
        {%- do return(relation) -%}
    {%- endif -%}
{%- endmacro %}
