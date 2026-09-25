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

# Consumer lag, per group, in records. ONE NUMBER PER GROUP because the groups do
# not lag alike and a single threshold would be either blind or noisy. Measured
# on the live stack 2026-09-24, mid-catch-up after a job resubmit, so these are
# BUSY values rather than idle ones:
#
#     bunching                              1,000
#     prediction-accuracy-pred             23,045
#     prediction-accuracy-obs               1,500
#     enrichment                              430
#     connect-enriched_vehicle_positions      500
#     connect-bunching_alerts                   0
#     connect-prediction_accuracy               0
#
# The shape of those measurements is why the dict has this shape:
#
#   * -pred reads raw.trip_updates, ~30x the row volume of positions at ~18.6k
#     records per poll, so its lag is naturally in the tens of thousands and
#     100k is about four polls of backlog. Both Flink groups also commit only at
#     a checkpoint (every 30s), so a fully caught-up -pred still shows a few
#     thousand between checkpoints.
#   * -obs reads enriched.vehicle_positions, three partitions of a much smaller
#     feed, so a quarter of that is generous.
#   * the connect-* groups commit continuously, so 0 means caught up. Their
#     thresholds are "the sink has stopped draining", not "the sink is behind".
#
# WHAT THESE CATCH: a consumer that is alive and no longer keeping up, which is
# the failure freshness checks cannot see, because there the data IS arriving and
# nobody is reading it fast enough.
#
# WHAT THEY DELIBERATELY TOLERATE: a backfill. Restart a connector, or resubmit a
# job from `earliest`, and the lag exceeds these until the backlog drains, so a
# deliberate action fires this alert once.
#
# WHAT THEY MISS: a group whose offsets have expired after seven days without a
# member. "A connector was deleted by hand" is `make tf-drift`'s question, and it
# answers immediately rather than in a week.
LAG_THRESHOLDS = {
    "bunching": 10_000,
    "prediction-accuracy-pred": 100_000,
    "prediction-accuracy-obs": 25_000,
    "enrichment": 25_000,
    "connect-enriched_vehicle_positions": 25_000,
    "connect-bunching_alerts": 5_000,
    "connect-prediction_accuracy": 50_000,
}

# Handed to the container as "group:threshold" pairs, so the script holds no
# second copy of the numbers. tests/test_operations_contract.py pins this dict to
# exactly the groups the pipeline runs, from the code's own constants.
LAG_TARGETS = " ".join(f"{name}:{limit}" for name, limit in LAG_THRESHOLDS.items())

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

# The lag verdict, same shape as DLQ_SCRIPT: one container, rpk for the numbers,
# awk for the arithmetic, and the pipeline's exit status IS the task's.
#
# AN ABSENT GROUP IS NOT A FAILURE. `rpk group describe` errors for a group that
# does not exist, which happens on a stack where a job has never run (the
# prediction job is opt-in) and for any group whose offsets expired. A group that
# does not exist cannot lag, so it is reported and skipped. The other half of that
# question, "a consumer should exist and does not", belongs to `make tf-drift` for
# the connectors and to dbt source freshness for the two Flink outputs.
#
# TOTAL-LAG is the number rpk prints for a group as a whole, across partitions,
# which is what a threshold wants: one partition of six being behind is still a
# consumer that is behind.
LAG_SCRIPT = r"""
set -euo pipefail
fail=0
for target in $LAG_TARGETS; do
    group="${target%%:*}"
    limit="${target##*:}"
    desc=$(rpk group describe "$group" -X brokers=redpanda:9092 2>&1) || desc=""
    if [ -z "$desc" ]; then
        printf '  %-36s UNREADABLE: rpk returned nothing (broker down?)\n' "$group"
        fail=1
        continue
    fi
    # STATE Dead is what rpk reports for a group that does not exist as well as
    # for one that expired, and both mean no consumer is making progress. It
    # reports TOTAL-LAG 0 with it, so the lag comparison alone passes silently:
    # that is a never-submitted Flink job reading as healthy, which is what
    # happened on 2026-09-23.
    state=$(printf '%s\n' "$desc" | awk '/^STATE/ {print $2; exit}')
    # Rows AFTER the topic-partition header. Zero rows is a group with nothing
    # assigned, which cannot be behind on anything and cannot be making progress
    # either.
    assigned=$(printf '%s\n' "$desc" | awk \
        '/^TOPIC[[:space:]]+PARTITION/ {seen=1; next} seen && NF {n++} END {print n+0}')
    lag=$(printf '%s\n' "$desc" | awk '/^TOTAL-LAG/ {print $2; exit}')
    if [ -z "$state" ]; then
        printf '  %-36s UNREADABLE: %s\n' "$group" \
            "$(printf '%s' "$desc" | head -1 | cut -c1-100)"
        fail=1
    elif [ "$state" = "Dead" ]; then
        printf '  %-36s DEAD: no live members (job not submitted, or group expired)\n' "$group"
        fail=1
    elif [ "$assigned" -eq 0 ]; then
        printf '  %-36s EMPTY: no partitions assigned to the group\n' "$group"
        fail=1
    elif [ -z "$lag" ]; then
        printf '  %-36s UNREADABLE: no TOTAL-LAG in the describe output\n' "$group"
        fail=1
    elif [ "$lag" -gt "$limit" ]; then
        printf '  %-36s %8s OVER threshold %s\n' "$group" "$lag" "$limit"
        fail=1
    else
        printf '  %-36s %8s (threshold %s)\n' "$group" "$lag" "$limit"
    fi
done
if [ "$fail" -ne 0 ]; then
    echo "ALERT: a consumer group is missing, dead, unreadable or over its lag threshold"
    exit 1
fi
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

    DockerOperator(
        task_id="consumer_lag",
        image=REDPANDA_IMAGE,
        # Entrypoint overridden for the same measured reason dlq_report records:
        # this image's /entrypoint.sh expects redpanda arguments, so without it
        # the command is swallowed and the task runs `rpk --help` and exits 0. A
        # green task doing nothing is the failure this DAG exists to catch.
        entrypoint=["bash"],
        command=["-c", LAG_SCRIPT],
        network_mode=NETWORK,
        environment={"LAG_TARGETS": LAG_TARGETS},
        auto_remove="success",
        mount_tmp_dir=False,
    )


# --- consumer_lag, and the healthchecks (5D) -----------------------------------
#
# The third task, `consumer_lag`, fails when a production consumer group falls
# too far behind. tests/test_operations_contract.py is the spec; `make
# airflow-check` requires the task.
#
# WHAT IT ASSERTS
#
#   1. LAG_THRESHOLDS: a module-level dict literal, {group: max records behind},
#      for EXACTLY the seven production groups. The contract test derives them
#      from the code (bunching, prediction-accuracy-pred/-obs, enrichment, one
#      connect-<name> per connect/*.json) and fails on any difference.
#
#   2. NOT every group rpk lists. Measured 2026-09-24: `rpk group list` has
#      nineteen groups, twelve of them abandoned diagnostics from Phases 2-3
#      (scan-*, probe-era2, strcheck, flink-smoke, fmt-<uuid>, ...), each about
#      3.2M records behind because nothing will ever read them again. A check
#      over all groups would be red forever. Deleting them (`rpk group delete`)
#      is a reasonable cleanup, but the explicit list is what keeps a future
#      diagnostic group from paging anyone.
#
#   3. Thresholds per group, measured and written down like the DLQ ones above.
#      The groups do not lag alike. At steady state on 2026-09-24:
#      connect-* 0, bunching 0, enrichment 120, prediction-accuracy-pred 1,189.
#      The Flink groups commit offsets only when a checkpoint completes (every
#      30s), so their "lag" includes up to 30s of records they have already
#      processed. Measure across an evening peak before changing one.
#
#   4. Same shape as dlq_report: a DockerOperator running rpk in the Redpanda
#      image with the entrypoint overridden (see that task's comment for what
#      happens without it), one `rpk group describe` per group, and a verdict
#      that is the task's exit status.
#
#   5. A Flink group shows STATE Empty even while its job runs. Flink assigns
#      partitions itself instead of joining the group, and only commits
#      offsets. So state says nothing here; lag is the only signal. A job that
#      is gone shows up as lag that grows every hour.
#
# HEALTHCHECKS, the other half of 5D, are in docker-compose.yml:
#
#   * producer and enrichment: producer/heartbeat.py touches a file once per
#     loop iteration and the healthcheck stats its age (`-mmin -2`). What
#     "healthy" means for the producer is that it is LOOPING, not that data is
#     arriving. During the 2026-09-23 03:00 upstream stall it ticked normally
#     for 82 minutes while Metro published nothing: a healthy process and an
#     unhealthy FEED. The feed has its own two alarms (source freshness and
#     check_feed_stall) and this one deliberately does not duplicate them.
#   * flink-jobmanager: the REST API's /overview. flink-taskmanager: the
#     jobmanager must see it (`/taskmanagers` carries a dataPort), which is the
#     check that catches a TaskManager that is up but not attached.
#   * Docker does NOT restart an unhealthy container. `restart: unless-stopped`
#     acts on exit, not on health. What a healthcheck buys is an honest
#     `docker ps` and `depends_on: condition: service_healthy` ordering;
#     flink-submit uses the latter for the jobmanager. Restarting on unhealthy
#     is a supervisor's job, and saying so is part of the answer.
