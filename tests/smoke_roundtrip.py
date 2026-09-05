#!/usr/bin/env python
"""Produce and consume a protobuf round trip against the running stack.

NOT a pytest test, deliberately: it needs a live broker, and a test suite
that fails when Docker is not running is a test suite people stop trusting.
`make test` stays runnable anywhere; this is `make smoke`.

What it actually proves is the dual-listener configuration, which is the most
common way a local Kafka stack appears to work and then hangs. Clients
bootstrap by connecting once and then RECONNECT to whatever address the broker
advertises; if `--advertise-kafka-addr` is wrong for the external listener,
this script connects, produces, and never sees the message come back.

It writes to a throwaway topic rather than raw.vehicle_positions, so running
it never contaminates a collection run.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient
from google.transit import gtfs_realtime_pb2 as rt

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
FIXTURE = Path(__file__).parent / "fixtures" / "vehicle_positions.pb"
N = 50


def drop_topic(topic: str) -> None:
    """Delete the throwaway topic. Repeated `make smoke` runs otherwise leave
    a trail of smoke.* topics cluttering the console and `make check`."""
    try:
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        for _, fut in admin.delete_topics([topic], operation_timeout=15).items():
            fut.result()
        print(f"cleaned up {topic}")
    except Exception as exc:  # cleanup failure must not fail the smoke test
        print(f"warning: could not delete {topic}: {exc}", file=sys.stderr)


def _run(topic: str) -> int:
    feed = rt.FeedMessage()
    feed.ParseFromString(FIXTURE.read_bytes())
    entities = list(feed.entity)[:N]
    if not entities:
        print("fixture is empty", file=sys.stderr)
        return 1

    print(f"bootstrap {BOOTSTRAP}\ntopic     {topic}")

    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    failures: list[str] = []

    def report(err, _msg):
        if err is not None:
            failures.append(str(err))

    for entity in entities:
        vehicle = entity.vehicle
        # Keyed on vehicle_id per ADR 0002, same keying the real producer uses.
        producer.produce(
            topic,
            key=vehicle.vehicle.id.encode(),
            value=vehicle.SerializeToString(),
            on_delivery=report,
        )
    remaining = producer.flush(30)

    if remaining:
        print(f"FAILED: {remaining} message(s) never delivered", file=sys.stderr)
        return 1
    if failures:
        print(f"FAILED: delivery errors: {failures[:3]}", file=sys.stderr)
        return 1
    print(f"produced  {len(entities)}")

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"smoke-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])

    seen: set[str] = set()
    consumed = 0
    try:
        while consumed < len(entities):
            msg = consumer.poll(15.0)
            if msg is None:
                print(
                    f"FAILED: timed out after {consumed}/{len(entities)}; "
                    "check --advertise-kafka-addr for the external listener",
                    file=sys.stderr,
                )
                return 1
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"FAILED: {msg.error()}", file=sys.stderr)
                return 1
            # Decoding proves the bytes survived the round trip intact, not
            # merely that a message of the right length came back.
            position = rt.VehiclePosition()
            position.ParseFromString(msg.value())
            assert position.vehicle.id == msg.key().decode()
            seen.add(msg.key().decode())
            consumed += 1
    finally:
        consumer.close()

    print(f"consumed  {consumed}  ({len(seen)} distinct vehicle keys)")
    print("round trip OK")
    return 0


def main() -> int:
    topic = f"smoke.{uuid.uuid4().hex[:8]}"
    try:
        return _run(topic)
    finally:
        drop_topic(topic)


if __name__ == "__main__":
    sys.exit(main())
