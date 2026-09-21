"""Enrichment consumer: raw.vehicle_positions -> enriched.vehicle_positions.

    python -m consumers.enrichment.run --status         # version + schema state
    python -m consumers.enrichment.run --dry-run        # consume, enrich, publish nothing
    python -m consumers.enrichment.run                  # run until stopped
    python -m consumers.enrichment.run --schema-version 2   # use enrich_v2

The enrichment logic lives in reference.py and enrich.py; this module is the wiring.

--- offsets and why auto-commit is off ---

`enable.auto.commit=False`, and offsets are committed only after the batch's
records have been produced AND flushed. Auto-commit would commit on a timer,
so a crash between commit and flush would silently drop records that Kafka
believes were processed -- the one failure mode at-least-once is supposed to
rule out. Committing after the flush means a crash reprocesses, which the
sink's primary key absorbs (ADR 0004).

--- the consumer group ---

One group, `enrichment`. Restarting resumes from the committed offset rather
than replaying the topic, which matters once raw.vehicle_positions holds days
of history. To reprocess deliberately, reset the group offset with rpk --
that is a decision, not something a restart should do by accident.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass

import psycopg
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

from consumers.enrichment import schema as schema_mod
from consumers.enrichment.enrich import STATS as ENRICH_STATS, enrich_v1, enrich_v2
from consumers.enrichment.reference import ReferenceData, resolve_version
from producer.errors import DlqReason
from producer.publish import TopicPublisher

log = logging.getLogger("enrichment")

SOURCE_TOPIC = "raw.vehicle_positions"
TARGET_TOPIC = "enriched.vehicle_positions"
DLQ_TOPIC = "dlq.vehicle_positions"
GROUP = "enrichment"


@dataclass
class Counters:
    consumed: int = 0
    enriched: int = 0
    # Separate from `enriched` on purpose. `enriched` counts records added to
    # the batch; `delivered` counts records the broker acknowledged after a
    # clean flush. They diverge exactly when delivery fails, which is the
    # case worth seeing -- collapsing them would hide it.
    delivered: int = 0
    dlq: int = 0
    errors: int = 0

    def __str__(self) -> str:
        return (f"consumed={self.consumed:,} enriched={self.enriched:,} "
                f"delivered={self.delivered:,} dlq={self.dlq:,} "
                f"errors={self.errors:,}")


_stop = False


def _handle_signal(signum, _frame) -> None:
    global _stop
    _stop = True
    log.info("signal %s -- finishing batch then committing", signal.Signals(signum).name)


def dsn() -> str:
    return (
        f"host={os.environ.get('WAREHOUSE_HOST', 'localhost')} "
        f"port={os.environ.get('WAREHOUSE_PORT', '5434')} "
        f"dbname={os.environ['POSTGRES_DB']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']}"
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="consumers.enrichment.run", description=__doc__)
    ap.add_argument("--status", action="store_true", help="show version + schema state, exit")
    ap.add_argument("--dry-run", action="store_true", help="enrich but publish nothing")
    ap.add_argument("--schema-version", type=int, choices=(1, 2), default=1,
                    help="1 = static join, 2 = + spatial (Phase 2D)")
    ap.add_argument("--from-beginning", action="store_true",
                    help="start at the earliest offset if the group has none")
    ap.add_argument("--batch", type=int, default=500, help="records per commit")
    ap.add_argument("--report-every", type=float, default=60.0)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S",
    )
    load_dotenv()

    sr = schema_mod.registry_client()
    if args.status:
        print(json.dumps(schema_mod.describe(sr), indent=2, default=str))
        with psycopg.connect(dsn()) as conn:
            try:
                vid, label = resolve_version(conn)
                print(f"static version: {vid} [{label or 'no feed_info.txt'}]")
            except RuntimeError as exc:
                print(f"static version: {exc}")
        return 0

    with psycopg.connect(dsn()) as conn:
        version_id, feed_version = resolve_version(conn)
        ref = ReferenceData(version_id, feed_version)
        ref.load(conn)
    log.info("reference %s", ref.summary())

    enrich = enrich_v1 if args.schema_version == 1 else enrich_v2
    log.info("enriching with v%d -> %s", args.schema_version, TARGET_TOPIC)

    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        "group.id": GROUP,
        # Manual commit: see the module docstring. Committing before the
        # produce is flushed would drop records Kafka thinks are done.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
    })
    consumer.subscribe([SOURCE_TOPIC])
    publisher = None if args.dry_run else TopicPublisher(
        os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))

    # Serializer only when there is something to serialize. The generated
    # module is build output (gitignored), so importing it at module scope
    # would break --status and --dry-run on a fresh clone that has not run
    # `make schema-gen` yet.
    serializer = None
    if publisher:
        from schemas.enriched_vehicle_position_pb2 import EnrichedVehiclePosition

        serializer = schema_mod.build_serializer(sr, EnrichedVehiclePosition)
        log.info("serializing with %s (%s)", schema_mod.SUBJECT,
                 schema_mod.describe(sr).get("latest_version", "?"))

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    counters = Counters()
    last_report = time.monotonic()
    batch: list = []

    try:
        while not _stop:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                counters.consumed += 1
                try:
                    raw = json.loads(msg.value())
                    result = enrich(raw, ref)
                except Exception as exc:  # noqa: BLE001 -- one record must not stop the stream
                    counters.errors += 1
                    log.exception("enrichment failed: %s", exc)
                    result = None

                if result is None:
                    pass
                elif result.ok:
                    batch.append(result.record)
                    counters.enriched += 1
                else:
                    counters.dlq += 1
                    if publisher:
                        publisher.publish_dlq(
                            DLQ_TOPIC, result.reason or DlqReason.UNKNOWN_TRIP_ID,
                            msg.value(), detail=result.detail,
                            entity_key=msg.key().decode() if msg.key() else None)
            elif msg is not None and msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                counters.errors += 1
                log.error("consume error: %s", msg.error())

            if batch and (len(batch) >= args.batch or _stop):
                if publisher:
                    for record in batch:
                        # Protobuf from here on. The schema id travels in the
                        # message prefix, so a consumer written against v1
                        # keeps reading v2 records by skipping the fields it
                        # does not know (ADR 0005).
                        #
                        # A serialization failure is NOT caught here, on
                        # purpose: it means the record and the registered
                        # schema disagree, which is a deploy error rather than
                        # a bad payload. Retrying the same batch would fail
                        # identically, and because the offsets are committed
                        # only after a successful flush, nothing is lost by
                        # stopping loudly.
                        #
                        # The KEY stays plain UTF-8 and must not change --
                        # Kafka partitions on the key bytes, so re-encoding it
                        # would reshuffle every vehicle_id to a different
                        # partition and break per-vehicle ordering for the
                        # whole history (ADR 0002).
                        publisher._producer.produce(
                            TARGET_TOPIC,
                            key=record["vehicle_id"].encode(),
                            value=serializer(
                                schema_mod.dict_to_message(record, EnrichedVehiclePosition),
                                SerializationContext(TARGET_TOPIC, MessageField.VALUE)),
                            on_delivery=publisher._on_delivery)
                        publisher._producer.poll(0)
                    # TWO failure modes, and flush() only reports one.
                    #
                    # flush() returns messages still QUEUED. A message the
                    # broker permanently rejected fires its delivery callback
                    # and LEAVES the queue, so flush returns 0 and this batch
                    # looks clean -- offsets commit and the record is gone.
                    # That is exactly the loss at-least-once exists to
                    # prevent, so the failed-callback count is checked too.
                    failed_before = len(publisher.failed)
                    undelivered = publisher.flush()
                    rejected = len(publisher.failed) - failed_before

                    if undelivered or rejected:
                        log.error(
                            "NOT committing offsets: %d undelivered, %d rejected "
                            "(last: %s)",
                            undelivered, rejected,
                            publisher.failed[-1] if publisher.failed else "n/a",
                        )
                        counters.errors += 1
                        # Do NOT clear the batch. Leaving it means the next
                        # pass retries these records, and the uncommitted
                        # offsets mean a restart replays them regardless.
                        continue

                    # Delivered, not merely enqueued.
                    counters.delivered += len(batch)
                # Offsets last, after the flush. See the module docstring.
                consumer.commit(asynchronous=False)
                batch.clear()

            if time.monotonic() - last_report >= args.report_every:
                log.info("%s | %s | %s", counters, ENRICH_STATS, ref.summary())
                last_report = time.monotonic()
    finally:
        consumer.close()

    log.info("final: %s | %s", counters, ENRICH_STATS)
    if ENRICH_STATS.implausible_deviation:
        # An alarm, not a tally -- see EnrichStats. Nonzero means a bug
        # shipped, so the run exits nonzero even with no hard errors.
        log.error("%d implausible deviation(s) (%.1f%%) -- see ADR 0005",
                  ENRICH_STATS.implausible_deviation,
                  100 * ENRICH_STATS.implausible_rate)
        return 1
    return 1 if counters.errors else 0


if __name__ == "__main__":
    sys.exit(main())
