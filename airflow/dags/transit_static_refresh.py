"""Daily static GTFS refresh. Build step 4D.

Reload static GTFS when Metro publishes a new zip. `make static-load`
(static/run.py --load) already skips when the ETag is unchanged; what this adds
is a schedule and a distinguishable outcome.

THE EXIT CODE IS THE INTERFACE. static/run.py returns 99 for "the ETag has not
moved" and 0 for "loaded a new version". `skip_on_exit_code` below turns the
first into SKIPPED, so a no-op day is visibly a no-op instead of a green run
that did nothing. tests/test_enrichment_contract.py asserts that this file and
static/run.py agree on the number, because this file cannot import the loader
to check: Airflow's image carries neither psycopg nor shapely.

WHY DockerOperator. The loader needs psycopg and shapely, which Airflow's
environment must not carry. That is the same separation the dbt DAG has, and
the reason docker/Dockerfile.pipeline exists. Airflow decides WHAT runs and
WHEN; the image owns HOW. (static/run.py's docstring used to claim a
BashOperator, which would have needed the project venv inside Airflow.)

NO MOUNTS, unlike transit_dbt's tasks. That DAG mounts the dbt project because
the project is its input; this image bakes the code in, so a task is one
container and nothing else.

ONE THING THIS DOES NOT DO, and it matters more now that the enrichment
consumer runs as a supervised container: the enrichment pins the static
version AT STARTUP, so loading a new version while it is running leaves it
joining against the old one. static/run.py prints a reminder to that effect.
Restarting it is a deliberate manual step for now:

    docker compose -f docker-compose.yml --profile stream restart enrichment

Worth automating only once there is a service-change procedure to hang it on.
The failure mode if it is forgotten is exactly the one that made this DAG
necessary: a service change that lands silently and shows up as a jump in the
unknown-trip rate.

Why daily, when Metro publishes a new zip every few weeks: the service change
of 2026-09-14 (findings section 9) landed mid-project and was only noticed
because the unknown-trip rate jumped. A daily ETag check costs one HEAD
request and turns that into a scheduled event.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

PIPELINE_IMAGE = "transit-pipeline:0.1.0"
NETWORK = "transit-stream_default"

# MUST EQUAL static.run.EXIT_UNCHANGED. Not imported, because importing it would
# drag psycopg into Airflow's environment; pinned by a contract test instead.
UNCHANGED_EXIT_CODE = 99

# Only forwarded when it is actually set. static/feed.py reads this variable
# with its own default, and os.environ.get() returns "" rather than the default
# when the variable exists but is empty -- so passing an empty string through
# would strip the good-citizen contact from requests to King County.
ENV = {
    "WAREHOUSE_HOST": "warehouse",
    "WAREHOUSE_PORT": "5432",
    # static_loader, not the superuser (build step 5C): it writes static.* and
    # reads nothing else, so a bad load cannot reach raw.*.
    "STATIC_LOADER_USER": "static_loader",
    "STATIC_LOADER_PASSWORD": os.environ["STATIC_LOADER_PASSWORD"],
    "POSTGRES_DB": os.environ["POSTGRES_DB"],
}
if os.environ.get("FEED_USER_AGENT"):
    ENV["FEED_USER_AGENT"] = os.environ["FEED_USER_AGENT"]


with DAG(
    dag_id="transit_static_refresh",
    description="Reload static GTFS when Metro publishes a new zip",
    schedule="0 11 * * *",   # 04:00 Pacific, before the AM peak
    start_date=datetime(2026, 9, 23),
    catchup=False,
    default_args={"retries": 1},
    tags=["transit", "static"],
) as dag:
    DockerOperator(
        task_id="load_static",
        image=PIPELINE_IMAGE,
        command="python -m static.run --load",
        network_mode=NETWORK,
        environment=ENV,
        # The whole point: an unchanged feed ends the task SKIPPED.
        skip_on_exit_code=UNCHANGED_EXIT_CODE,
        auto_remove="success",
        mount_tmp_dir=False,
    )

