"""Hourly dbt: source freshness, then build.

WHY DockerOperator (the parcel project's reasoning, which still holds):
Airflow and dbt pin overlapping libraries differently, so they never share an
interpreter. This DAG decides WHAT runs and WHEN; the transit-dbt image
(docker/Dockerfile.dbt) owns HOW. Airflow could be swapped for cron without
touching a line of dbt.

SIBLING CONTAINERS: DockerOperator asks the HOST daemon to start the dbt
container, so the mount source must be a host path (TRANSIT_HOST_ROOT, set by
`make airflow-up`), and the container joins transit-stream_default to reach
the warehouse as warehouse:5432.

FRESHNESS AND BUILD ARE SIBLINGS, NOT A CHAIN. If freshness gated the build,
a feed stall would stop the build, and the build is what records the stall
in mart_feed_health. The stall would then be missing from the table that is
supposed to report it. So both run every hour: freshness fails loudly (the
alert), and build still runs (the record). That's the 2026-09-23 03:00 stall
answered twice, from two directions.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DBT_IMAGE = "transit-dbt:1.11.0"
NETWORK = "transit-stream_default"
HOST_ROOT = os.environ["TRANSIT_HOST_ROOT"]


def dbt(task_id: str, command: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=command,
        network_mode=NETWORK,
        mounts=[Mount(source=f"{HOST_ROOT}/dbt", target="/dbt", type="bind")],
        environment={
            "DBT_PROFILES_DIR": "/dbt",
            "DBT_HOST": "warehouse",
            "DBT_PORT": "5432",
            # dbt_transform, not the superuser (build step 5C). It reads raw.* and
            # static.*, owns staging.* and marts.*, and cannot drop a landing
            # table. The names match dbt/profiles.yml and the Makefile's DBT_RUN,
            # because all three start the same container.
            "DBT_USER": "dbt_transform",
            "DBT_PASSWORD": os.environ["DBT_TRANSFORM_PASSWORD"],
            "POSTGRES_DB": os.environ["POSTGRES_DB"],
        },
        # dbt writes target/ and logs/ into the mounted project. Not removing
        # the container on failure would keep them, but it also leaves one
        # dead container per failed hour; the logs Airflow captures are enough.
        auto_remove="success",
        mount_tmp_dir=False,
    )


with DAG(
    dag_id="transit_dbt",
    description="dbt source freshness + dbt build, hourly",
    schedule="15 * * * *",
    start_date=datetime(2026, 9, 23),
    catchup=False,
    # A run that is still going when the next hour starts means something is
    # wrong; queueing a second build behind it would only hide that.
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["transit", "dbt"],
) as dag:
    dbt("source_freshness", "source freshness")
    # recent_hours scopes the TESTS over the big tables to the last 6 hours
    # (dbt/macros/recent_window.sql); the incremental marts scope themselves.
    # Measured 2026-09-26: 1,628 s per build with every test over all of
    # history (~2,250 node-seconds of it tests), 491 s with a 48-hour window,
    # where four tests still took 90-157 s each re-reading two days that
    # earlier builds had already tested. 6 hours is the new hour plus five
    # hours of slack: a row escapes the hourly tests only if five runs in a
    # row fail, and `make dbt-full-check` covers that and everything older.
    dbt("build", "build --vars '{recent_hours: 6}'")
