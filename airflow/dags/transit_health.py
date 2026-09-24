"""Feed-health and DLQ alerting. Build step 4D.

This is the DAG the 2026-09-23 03:00 stall asked for. Metro's feed stopped
changing for ~80 minutes, the producer logged stale=3998s, and nothing
alerted, because the only signal was a log line. transit_dbt's
`source_freshness` task already fails loudly on a stale feed. This DAG adds
the two things freshness cannot say:

  check_feed_stall   A stall that has ENDED, with its start, end and measured
                     length. Why ended: mart_feed_health's minute spine is
                     bounded by the data (generate_series over
                     [min(position_at), max(position_at)]), so a stall still in
                     progress does not appear as trailing silent rows at all,
                     the table simply stops. That is the case source_freshness
                     catches. This one is the post-mortem, and it can put a
                     number on the outage, which a freshness failure cannot.

  dlq_report         Count records in the dlq.* topics over a TIME WINDOW and
                     fail on growth above a threshold. Airflow has no Kafka
                     client and should not get one (it would join Airflow's
                     pinned environment), so this is a DockerOperator running
                     `rpk` in the Redpanda image against redpanda:9092.

THRESHOLDS, both measured rather than chosen.

  Stall: 5 consecutive silent minutes. The mart's silent runs on the loaded
  data are 1062, 623, 225, 82, 14, 4, 3, 2 and 1 minutes, so 5 is the value
  that clears every recovery flap -- the longest measured is 4 -- and trips
  everything else. The 82-minute stall trips it; the 04:22-04:37 flapping does
  not.

  DLQ: two thresholds, because the measurements say the volume is noise and
  the REASON is the signal.

  Every one of the 14,839 records in the topic's last 12 hours is
  `position_out_of_bounds` (lat=0.0 lon=0.0, ~199 vehicles with chronic GPS
  failures), which is what Phase 1 found at 18,956 in 24h. So volume alone
  says nothing: the hourly count runs from 0 to 2,259 depending on how many
  of those vehicles are out and whether the producer was up, while the topic
  already holds 74,000 records, so "any DLQ record" would be permanently red.

  What is NOT noise is the reason. producer/errors.py's DlqReason has three
  values, and the other two -- `malformed_payload` and
  `missing_required_field` -- mean the FEED changed shape or was corrupted.
  Both currently sit at zero, and one of them is exactly the 2026-09-14
  service-change failure this project already paid for once. So:

      8000 chronic out-of-bounds records in the window -> alert on volume
      any record with another reason, at all          -> alert on reason

  8000 is ~3.5x the highest hour measured (2,259) and ~6.5x the 12-hour
  average (1,237/hour), which leaves room for the chronic population to drift
  without shouting. A fleet-wide bbox or decoder failure puts ~100,000 in an
  hour, so it trips either way.

WHY A TIME WINDOW AND NOT A WATERMARK. "Since the last run" would need the
previous count stored somewhere, which means another table in a warehouse this
project deliberately keeps as a derived view. rpk's bounded consume
(`--offset=@-1h:end`) answers the same question with no state at all, and the
question stays "is it growing faster than normal" rather than "is it growing".

Schedule: hourly at :45, after transit_dbt's :15 build has refreshed the mart.
That ordering is by schedule, not by a sensor: a sensor would make this DAG
wait on dbt, and a dbt failure would then hide the feed health it exists to
report.

THE WINDOW IS ANCHORED TO THE MART'S LATEST MINUTE, NOT TO THE CLOCK, and that
is what makes the ordering above safe to rely on. A stall only reaches the mart
once data RESUMES, so a run whose last silent minute falls between :15 and :44
is invisible to the :45 check (not yet in the mart) and, if the window is
measured back from now(), also outside the next check's window: at the following
:45 that window starts at :45 minus an hour, and a run that ended at :21 the
previous hour falls before it. Measuring back from the mart's own latest minute
instead makes consecutive builds cover back-to-back windows with no seam.
Checked against every real silent run of 5 or more minutes in the warehouse:
the 2026-09-23 03:00-04:21 upstream stall is the one a clock-anchored window
misses, and this version reports it.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.docker.operators.docker import DockerOperator

REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v25.2.1"
NETWORK = "transit-stream_default"

# Measured; see the module docstring.
STALL_THRESHOLD_MINUTES = 5
DLQ_CHRONIC_THRESHOLD = 8000

# How far back into the mart the stall check looks, and how much of that it
# re-covers on the next run. The overlap is the price of a failed dbt build: with
# no new build, one window gets measured twice rather than skipped, so the
# failure mode is an occasional duplicate alert instead of a silent outage.
# Ten minutes is well over the gap between the :15 build and the :45 check.
STALL_WINDOW_MINUTES = 60
STALL_WINDOW_OVERLAP_MINUTES = 10

# The reason string that is chronic, from producer.errors.DlqReason. Anything
# else in the topic is a shape change or corruption rather than a bad GPS fix,
# and any occurrence at all is an alert.
CHRONIC_DLQ_REASON = "position_out_of_bounds"

# Reconstructs the silent runs. The mart stores stall_minutes per minute but no
# run id, so this repeats the gaps-and-islands arithmetic the model uses:
# subtract a row_number PARTITIONED BY is_silent from a global one, and the
# difference is constant across a run. Partitioning by the flag is the part
# that matters, without it a silent run and a later non-silent one can land on
# the same difference and be counted as one.
#
# Always returns exactly ONE row, because SQLCheckOperator fails on a falsy
# value: `ok` is the assertion "no long stall ended inside the mart's most
# recent window", so a healthy hour is TRUE rather than "no rows came back".
STALL_SQL = """
with numbered as (
    select
        minute_utc,
        minute_local,
        is_silent,
        row_number() over (order by minute_utc)
            - row_number() over (partition by is_silent order by minute_utc)
            as run_group
    from marts.mart_feed_health
),
runs as (
    select
        min(minute_local)  as started_local,
        max(minute_local)  as ended_local,
        count(*)::integer  as silent_minutes
    from numbered
    where is_silent
    group by run_group
),
recent as (
    select *
    from runs
    -- Anchored to the mart's latest minute, NOT to now(). See the module
    -- docstring for the stall this difference loses. The overlap means a failed
    -- build re-measures a window instead of skipping it.
    where ended_local > (
        (select max(minute_local) from marts.mart_feed_health)
            - interval '{window} minutes'
            - interval '{overlap} minutes'
    )
    order by silent_minutes desc
    limit 1
)
select
    not coalesce((select silent_minutes >= {threshold} from recent), false) as ok,
    coalesce(
        (select format('silent for %s minutes, %s to %s Pacific (threshold %s)',
                       silent_minutes,
                       to_char(started_local, 'MM-DD HH24:MI'),
                       to_char(ended_local, 'HH24:MI'),
                       {threshold})
         from recent),
        format('no silent run of {threshold}+ minutes ended in the mart''s last %s minutes',
               {window} + {overlap})
    ) as detail
""".format(threshold=STALL_THRESHOLD_MINUTES,
           window=STALL_WINDOW_MINUTES,
           overlap=STALL_WINDOW_OVERLAP_MINUTES)

# $DLQ_TOPICS unquoted so it splits into three arguments. The values live in the
# container's environment rather than inside this string, so the numbers and the
# reason string are readable from the DAG file rather than buried in quoting.
#
# ONE consume, and awk makes the verdict, so the pipeline's exit status is the
# task's: awk exits 1 and `pipefail` propagates it. Doing the counting in the
# shell would need a second consume or a `grep -c` that returns 1 when it finds
# nothing, which is the wrong kind of failure.
DLQ_SCRIPT = r"""
set -euo pipefail
rpk topic consume $DLQ_TOPICS -X brokers=redpanda:9092 \
    --offset=@-$DLQ_WINDOW:end -f '%v\n' \
| awk -v window="$DLQ_WINDOW" -v chronic_reason="$CHRONIC_REASON" \
      -v max="$DLQ_CHRONIC_THRESHOLD" '
    { if (index($0, chronic_reason)) chronic++; else other++ }
    END {
        printf "dlq records in the last %s: %d chronic, %d other (chronic threshold %s)\n",
               window, chronic + 0, other + 0, max
        if (other > 0) {
            printf "DLQ REASON ALERT: %d record(s) failed for a reason other than %s\n",
                   other, chronic_reason
            exit 1
        }
        if (chronic > max) {
            printf "DLQ GROWTH: %d chronic records, over the %s threshold\n", chronic, max
            exit 1
        }
    }'
""".strip()


with DAG(
    dag_id="transit_health",
    description="Alert on feed stalls and DLQ growth",
    schedule="45 * * * *",
    start_date=datetime(2026, 9, 23),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["transit", "health"],
) as dag:
    SQLCheckOperator(
        task_id="check_feed_stall",
        # Defined by AIRFLOW_CONN_WAREHOUSE in docker-compose.airflow.yml, so it
        # comes from the environment rather than from the UI and a clean clone
        # works.
        conn_id="warehouse",
        sql=STALL_SQL,
    )

    DockerOperator(
        task_id="dlq_report",
        image=REDPANDA_IMAGE,
        # THE ENTRYPOINT MUST BE OVERRIDDEN. This image's is /entrypoint.sh,
        # which expects redpanda arguments, so without this the command is
        # handed to it and the task runs `rpk --help` instead: measured, it
        # printed rpk's usage and exited 0. A green task doing nothing is the
        # failure mode this whole DAG exists to prevent, so it is worth the two
        # lines. Verified as `--entrypoint bash <image> -c <script>`.
        entrypoint=["bash"],
        command=["-c", DLQ_SCRIPT],
        network_mode=NETWORK,
        environment={
            "DLQ_TOPICS":
                "dlq.vehicle_positions dlq.trip_updates dlq.service_alerts",
            "DLQ_WINDOW": "1h",
            "DLQ_CHRONIC_THRESHOLD": str(DLQ_CHRONIC_THRESHOLD),
            "CHRONIC_REASON": CHRONIC_DLQ_REASON,
        },
        auto_remove="success",
        mount_tmp_dir=False,
    )
