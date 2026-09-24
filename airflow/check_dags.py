"""The DAG spec: import every DAG and check the tasks each must have.

    make airflow-check

Runs INSIDE the apache/airflow image (Airflow is not in the project venv and
must not be), so it is the Airflow counterpart of the contract suites. It
fails on any import error, which is the single most common way a DAG
disappears from the UI without a word, and on a DAG that is present but has
lost one of the tasks REQUIRED below.
"""

import sys

from airflow.models import DagBag

REQUIRED = {
    "transit_partitions": {"ensure_enriched_vehicle_positions"},
    "transit_dbt": {"source_freshness", "build"},
    "transit_health": {"check_feed_stall", "dlq_report"},
    "transit_static_refresh": {"load_static"},
}

bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
failed = False

for path, err in bag.import_errors.items():
    print(f"IMPORT ERROR {path}:\n{err}")
    failed = True

for dag_id, tasks in REQUIRED.items():
    dag = bag.dags.get(dag_id)
    if dag is None:
        print(f"MISSING DAG  {dag_id}")
        failed = True
        continue
    missing = tasks - set(dag.task_ids)
    status = "ok" if not missing else f"missing tasks: {sorted(missing)}"
    print(f"{dag_id:<24} {status}")
    failed |= bool(missing)

sys.exit(1 if failed else 0)
