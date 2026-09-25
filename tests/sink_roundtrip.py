#!/usr/bin/env python
"""Produce ONE framed record to alerts.bunching and wait for it in the warehouse.

NOT a pytest test, for the same reason tests/smoke_roundtrip.py is not one: it
needs a live stack. It is `make sink-roundtrip`, and it is the last step of the
`platform` CI job, where it is the only step that proves the whole sink path
works rather than that every individual piece is configured: registry framing,
JsonSchemaConverter, TimestampConverter, the upsert, and partition routing.

WHY THE RECORD IS PRODUCED DIRECTLY TO THE OUTPUT TOPIC. The Flink detector is
what normally writes alerts.bunching, and this job does not start Flink, so the
gap between "the connector is configured" and "a row lands" is closed here
without a cluster. What is under test is the connector and the table, not the
detector, which has its own contract suite.

ONE ROW, IDENTIFIABLE, AND ONLY HALF REMOVABLE. The pair ('sink-roundtrip-a',
'sink-roundtrip-b') is not a vehicle id any feed will produce, and --clean
deletes the warehouse row this script found. It cannot delete the RECORD:
alerts.bunching is a delete-policy topic with 30-day retention, and Kafka has
no way to remove one record from the middle of one. Measured on 2026-09-25
after two runs against the live collection: 0 rows in raw.bunching_alerts, 2
probe records still on the topic. Any replay of the connector before they age
out puts both rows back.

So this belongs on a stack that is thrown away afterwards, which is what the
`platform` CI job is. Against a real collection, --clean keeps the marts
honest until the next replay and no longer than that.

Exit status is the whole point: 0 only when a row matching the record this run
produced is visible in raw.bunching_alerts.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Producer  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from consumers.framing import frame, schema_id  # noqa: E402

TOPIC = "alerts.bunching"
SUBJECT = f"{TOPIC}-value"
TABLE = "raw.bunching_alerts"
VEHICLE_A = "sink-roundtrip-a"
VEHICLE_B = "sink-roundtrip-b"


def record(window_end: int) -> dict:
    """Every REQUIRED field of schemas/json/bunching_alert.json, and nothing else.

    window_end is epoch SECONDS as an integer: the connector's TimestampConverter
    refuses a float64, and unix.precision=seconds is what keeps the row out of
    1970. Both are recorded in that schema's own descriptions.
    """
    return {
        "route_id": "40",
        "direction_id": 0,
        "vehicle_id_a": VEHICLE_A,
        "vehicle_id_b": VEHICLE_B,
        "gap_ft": 250.0,
        "window_end": window_end,
    }


def dsn() -> str:
    """The warehouse, as the admin in .env. This is a probe, not a client role."""
    return (
        f"host=localhost port={os.environ.get('WAREHOUSE_PORT', '5434')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'transit')} "
        f"user={os.environ.get('POSTGRES_USER', 'transit')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', '')} connect_timeout=3"
    )


def produce(window_end: int) -> None:
    payload = record(window_end)
    sid = schema_id(os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081"),
                    SUBJECT)
    body = frame(json.dumps(payload).encode(), sid)
    producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"]})
    errors: list[str] = []

    def report(err, _msg):
        if err is not None:
            errors.append(str(err))

    producer.produce(TOPIC, value=bytes(body), on_delivery=report)
    remaining = producer.flush(30)
    if errors or remaining:
        raise SystemExit(f"produce failed: {errors or f'{remaining} still queued'}")
    print(f"produced 1 record to {TOPIC} (schema id {sid}, window_end {window_end})")


def wait_for_row(window_end: int, timeout_s: int) -> bool:
    import psycopg

    sql = (f"select count(*) from {TABLE} where vehicle_id_a = %s "
           f"and vehicle_id_b = %s and window_end = to_timestamp(%s)")
    deadline = time.monotonic() + timeout_s
    attempt = 0
    with psycopg.connect(dsn()) as conn:
        while True:
            attempt += 1
            with conn.cursor() as cur:
                cur.execute(sql, (VEHICLE_A, VEHICLE_B, window_end))
                found = cur.fetchone()[0]
            if found:
                print(f"row visible in {TABLE} after {attempt} probe(s)")
                return True
            if time.monotonic() >= deadline:
                print(f"TIMEOUT: no row in {TABLE} after {timeout_s}s "
                      f"({attempt} probes)")
                return False
            time.sleep(2)


def clean(window_end: int) -> None:
    import psycopg

    with psycopg.connect(dsn()) as conn, conn.cursor() as cur:
        cur.execute(f"delete from {TABLE} where vehicle_id_a = %s "
                    f"and vehicle_id_b = %s and window_end = to_timestamp(%s)",
                    (VEHICLE_A, VEHICLE_B, window_end))
        print(f"cleaned up {cur.rowcount} row(s)")
        conn.commit()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="tests/sink_roundtrip.py", description=__doc__)
    ap.add_argument("--timeout", type=int, default=90,
                    help="seconds to wait for the row (default 90)")
    ap.add_argument("--clean", action="store_true",
                    help="delete the warehouse row afterwards (the topic record stays)")
    ap.add_argument("--window-end", type=int, default=None,
                    help="epoch seconds; defaults to now, which is covered by the "
                         "partitions 03-sink.sql creates")
    args = ap.parse_args(argv)

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    window_end = args.window_end or int(time.time())

    produce(window_end)
    found = wait_for_row(window_end, args.timeout)
    if args.clean:
        clean(window_end)
    return 0 if found else 1


if __name__ == "__main__":
    raise SystemExit(main())