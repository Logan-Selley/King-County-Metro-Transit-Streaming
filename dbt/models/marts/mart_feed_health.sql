{#
  One row per minute: is Metro's feed alive, and how late is it?

  THREE DECISIONS, each measured on the loaded warehouse rather than chosen:

  0. THE STALL CHECK'S GRANT IS DECLARED HERE, NOT IN TERRAFORM. A table
     materialization builds a NEW table and swaps it in, so the new relation
     inherits none of the old ACL: a postgresql_grant in terraform/core/access.tf
     is revoked out from under the mart by the next dbt build, every hour at :15.
     Measured 2026-09-25: mart_feed_health's relacl was empty, airflow_ops could
     not read it, and tf-drift reported the missing SELECT. dbt owns the table, so
     dbt owns the grant and re-applies it on every build. Schema USAGE stays in
     Terraform, where a schema-level fact belongs.

  1. THE SPINE IS GENERATED, NOT GROUPED. A silent minute has no rows in
     staging, so grouping staging cannot produce it. generate_series covers
     [min(position_at), max(position_at)] and the aggregates left join onto
     it. tests/feed_health_covers_every_minute.sql recomputes those bounds
     from staging and compares the row count, which is what makes the range a
     contract rather than a coincidence.

  2. STALE ROWS ARE EXCLUDED FROM THE SPINE AND FROM THE COUNTS. This table
     holds 855,805 rows with an ingest lag over an hour. Ten are the
     2026-09-04 block, which would stretch the spine seventeen days back and
     report seventeen days of silence. The other 855,795 are older fixes the
     enrichment consumer re-published days later -- positions from 09-04 to
     09-22, enriched 09-21 to 09-23 -- and they sit INSIDE the range. Counting
     them as liveness changes the answer: 740 silent minutes unfiltered
     against 1,789 filtered, so a replay would paper over a real outage. A fix
     hours old is also not a usable answer to "how late is the feed", which is
     what median_ingest_lag_s reports.

  3. stall_minutes IS A GAPS-AND-ISLANDS COUNT. Number the minutes, subtract a
     row_number PARTITIONED BY is_silent, and the difference is constant across
     a run of consecutive minutes carrying the same flag. Partitioning by the
     flag is load-bearing: without it a silent run and a later non-silent run
     can land on the same difference and be counted as one run.

  WHY THIS EXISTS. On 2026-09-23 from ~03:00 to 04:21 Pacific, Metro's
  upstream objects stopped changing. The producer handled it correctly and
  logged stale=3998s at the peak, and nothing alerted, because the signal
  lived in a log line. This mart makes it a table, so Airflow can alert on it
  (dags/transit_health.py) and so the history of outages is queryable.

  SHAPE (enforced by the contract in _marts.yml):
    minute_utc          timestamptz  the minute, truncated, UTC
    minute_local        timestamp    same minute in America/Los_Angeles
    positions           integer      rows with position_at in the minute
    vehicles            integer      distinct vehicle_id in the minute
    median_ingest_lag_s integer      NULL when positions = 0
    is_silent           boolean      positions = 0
    stall_minutes       integer      length of the run of consecutive silent
                                     minutes this minute belongs to, 0 if not
                                     silent

  MEASURED: QUIET IS NOT SILENT, 2026-09-23 Pacific:

        00:00-02:59  owl service     never silent; thinnest minute 30 positions
        03:00-04:21  UPSTREAM stall  82 silent minutes
        04:22-04:37  recovery        flaps: silent runs of 1, 3 and 4 minutes
        04:38-       normal

  In that window owl service never went silent: its thinnest minute still
  carried 30 positions, so a silent minute is worth reporting. What sets the
  alert threshold in transit_health at 5 consecutive silent minutes is
  the other end of the table, the recovery flaps in runs of 1, 3 and 4 minutes:
  5 clears the longest of them, so the stall trips it and the flapping does not.
  This mart reports every silent run; the threshold belongs to the alert, not to
  the table.

  Silence has two causes this table cannot tell apart. The stall above was
  Metro's upstream objects not changing. The ~10 hours of silence before
  midnight on 2026-09-22 were OURS: the producer and enrichment consumer had
  died with the session that started them, and 2026-09-21 has 180 silent owl
  minutes for the same reason. Both look like "no data arrived", which is what
  this mart measures. Telling them apart needs the producer's own view (its
  stale= counter, or a heartbeat topic) rather than positions, so read a row
  here as "nobody received positions", not as Metro being down.
#}

{# WHY A dbt var rather than a literal role name. CI builds dbt against a throwaway
   warehouse that has no roles at all, because nothing there runs Terraform, so a
   hard-coded GRANT ... TO airflow_ops fails the build with "role does not exist".
   make ci-dbt passes stall_mart_readers: [] and the grant is skipped. #}
{% set stall_mart_readers = var('stall_mart_readers', ['airflow_ops']) %}

{#
  INCREMENTAL, recomputing only a recent tail. Measured 2026-09-26: as a full
  rebuild this model took 775 s of every hourly build, re-deriving every minute
  since the first position from a 6.2M-row table that grows ~730 MB a day.

  THE RECOMPUTE STARTS AT A NON-SILENT MINUTE, and that is the whole of the
  correctness argument. stall_minutes is the length of the WHOLE silent run a
  minute belongs to, so a recompute that began inside a run would count only
  its tail and write a shorter stall over the true one. The restart point is
  therefore the last non-silent minute at or before (newest minute - lookback).
  No silent run can contain a non-silent minute, so every run that touches the
  recomputed range lies entirely inside it, and every run before it is
  already final.

  THE LOOKBACK is 6 hours. A position can only move a minute's counts if it
  arrives late, and a position more than an hour late is is_stale_timestamp
  and excluded here anyway, so one hour would be enough while every hourly
  build succeeds. The margin covers late sinks and a skipped run; a longer
  outage of the build is covered by the restart being relative to this
  table's own newest minute rather than to now(): after a day of failed
  builds, the next one recomputes from where the table stopped.

  delete+insert on minute_utc replaces exactly the recomputed minutes. The
  table also keeps minutes older than the raw retention window once
  drop_partitions_before removes their positions, which a full rebuild would
  have lost; a stall history is cheap (1,440 rows a day) and worth keeping.

  on_schema_change='fail' because the contract is enforced: a column change
  here must be a deliberate --full-refresh, not a silent append.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='minute_utc',
    on_schema_change='fail',
    grants={'select': stall_mart_readers},
) }}

{% set lookback = '6 hours' %}

with
{% if is_incremental() %}
restart as (

    -- -infinity when no minute is old enough, which recomputes everything:
    -- correct for a table that is still young, and cheap because it is.
    select coalesce(max(minute_utc), '-infinity'::timestamptz) as from_minute
    from {{ this }}
    where not is_silent
      and minute_utc <= (select max(minute_utc) from {{ this }}) - interval '{{ lookback }}'

),
{% endif %}

staging as (

    select
        position_at,
        vehicle_id,
        ingest_lag_s
    from {{ ref('stg_vehicle_positions') }}
    where not is_stale_timestamp
    {% if is_incremental() %}
      -- Through the staging view to raw.enriched_vehicle_positions' daily
      -- partitions: Postgres prunes them at executor start from this
      -- init-plan value, so the scan is the tail, not the table.
      and position_at >= (select from_minute from restart)
    {% endif %}

),

bounds as (

    select
        date_trunc('minute', min(position_at)) as first_minute,
        date_trunc('minute', max(position_at)) as last_minute
    from staging

),

spine as (

    select
        generate_series(first_minute, last_minute, interval '1 minute') as minute_utc
    from bounds

),

observed as (

    select
        date_trunc('minute', position_at)     as minute_utc,
        count(*)::integer                     as positions,
        count(distinct vehicle_id)::integer   as vehicles,
        percentile_cont(0.5) within group (order by ingest_lag_s)::integer
                                              as median_ingest_lag_s
    from staging
    group by 1

),

joined as (

    select
        spine.minute_utc,
        coalesce(observed.positions, 0)     as positions,
        coalesce(observed.vehicles, 0)      as vehicles,
        -- NULL on a silent minute, as the shape specifies: there is no lag to
        -- report, and 0 would read as "arrived instantly".
        observed.median_ingest_lag_s,
        coalesce(observed.positions, 0) = 0 as is_silent
    from spine
    left join observed on observed.minute_utc = spine.minute_utc

),

numbered as (

    select
        minute_utc,
        positions,
        vehicles,
        median_ingest_lag_s,
        is_silent,
        row_number() over (order by minute_utc)
            - row_number() over (partition by is_silent order by minute_utc)
            as run_group
    from joined

),

sized as (

    -- The columns are carried through both CTEs because a window function
    -- cannot read another's result in the same select: the run length needs a
    -- second pass, and the columns it does not use must still be in scope.
    select
        minute_utc,
        positions,
        vehicles,
        median_ingest_lag_s,
        is_silent,
        count(*) over (partition by is_silent, run_group) as run_length
    from numbered

)

select
    minute_utc,
    -- Derived from the truncated UTC minute rather than from position_local,
    -- so it is provably the same minute instead of a second conversion that
    -- could disagree with the spine.
    (minute_utc at time zone 'America/Los_Angeles') as minute_local,
    positions,
    vehicles,
    median_ingest_lag_s,
    is_silent,
    -- 0 on a minute that has positions: stall_minutes describes the run this
    -- minute belongs to, and a minute with data belongs to no stall.
    (case when is_silent then run_length else 0 end)::integer as stall_minutes
from sized
