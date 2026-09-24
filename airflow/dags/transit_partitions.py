"""Daily partition maintenance for the sink tables.

The whole DAG is one ordering rule, found while building the sink
(docker/initdb/03-sink.sql):

    Once the DEFAULT partition holds rows for a day, Postgres refuses to create
    that day's partition -- "updated partition constraint for default
    partition would be violated".

The connector writes continuously, so the day's partition must exist BEFORE
the first record for that day arrives. If it doesn't, those records go to the
default partition, and after that the partition can't be created until
someone moves them by hand. So this runs well ahead of midnight and creates
the next two days, not just tomorrow: one failed run then costs nothing.

UTC days, because raw.ensure_partition bounds partitions with date literals
compared against a timestamptz. That makes them UTC-midnight boundaries, which
is correct for storage. "Which service day is this" is a staging concern
(stg_vehicle_positions.service_date), not a partitioning one.

RETENTION IS HERE NOW, decided in build step 4E rather than inherited. 90 days,
against a measured 730 MB/day and 2.8 T free: the reasoning, the alternatives
and the irreversibility argument are in docker/initdb/04-retention.sql, and the
window is RETENTION_DAYS below.

The drop is a separate task from the ensure loop on purpose. Creating a
partition is idempotent and safe to retry; dropping one is not, so they are
different tasks with different meanings when they fail, and `drop_partitions_
before` returns what it removed rather than a count so the run log says which
days went.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Every table the sink writes that is partitioned by day. The two Flink sinks
# joined this list in build step 4B; before that there was one, which is why the
# loop reads as if it were built for three. A table missing from here does not
# fail loudly -- it silently grows a default partition and then cannot be split,
# which is the failure 03-sink.sql's comment records.
PARTITIONED = [
    "raw.enriched_vehicle_positions",
    "raw.bunching_alerts",
    "raw.prediction_accuracy",
]
DAYS_AHEAD = 2
RETENTION_DAYS = 90

with DAG(
    dag_id="transit_partitions",
    description="Create tomorrow's (and the next day's) partitions ahead of the sink",
    # 20:00 UTC is 13:00 Pacific: well clear of midnight in both, so a failed
    # run has half a day of retries before it matters.
    schedule="0 20 * * *",
    start_date=datetime(2026, 9, 23),
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=15)},
    tags=["transit", "maintenance"],
) as dag:
    for table in PARTITIONED:
        SQLExecuteQueryOperator(
            task_id=f"ensure_{table.split('.')[-1]}",
            conn_id="warehouse",
            # ensure_partition is idempotent ("exists" vs "created"), so a
            # retry or a manual re-trigger is always safe.
            sql=f"""
                select raw.ensure_partition('{table}', d::date)
                from generate_series(current_date + 1,
                                     current_date + {DAYS_AHEAD},
                                     interval '1 day') as d
            """,
            show_return_value_in_logs=True,
        )

        SQLExecuteQueryOperator(
            task_id=f"drop_old_partitions_{table.split('.')[-1]}",
            conn_id="warehouse",
            # current_date, not now(): ensure_partition bounds partitions with
            # date literals, so the day boundaries are UTC and the cutoff has to
            # be a date to match. A partition for the cutoff day itself is kept
            # (the function drops strictly older), so the window is 90 days plus
            # today.
            sql=f"""
                select raw.drop_partitions_before('{table}',
                                                  current_date - {RETENTION_DAYS})
            """,
            show_return_value_in_logs=True,
        )
