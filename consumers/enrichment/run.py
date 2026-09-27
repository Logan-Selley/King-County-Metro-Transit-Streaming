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
from producer.heartbeat import beat
from producer.publish import TopicPublisher

log = logging.getLogger("enrichment")

# The live defaults. settings_from_args uses these when --source, --target, --dlq
# or --group is absent, so a run that names none of them is the live consumer.
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
    """The warehouse connection, as the enrichment role rather than the superuser.

    That role reads static.* and nothing else, and never reads raw.* from
    Postgres at all, because its source is the raw topic.
    """
    return (
        f"host={os.environ.get('WAREHOUSE_HOST', 'localhost')} "
        f"port={os.environ.get('WAREHOUSE_PORT', '5434')} "
        f"dbname={os.environ['POSTGRES_DB']} "
        f"user={os.environ.get('ENRICHMENT_USER', 'enrichment')} "
        f"password={os.environ['ENRICHMENT_PASSWORD']}"
    )


# --- how a run is wired: live, or a replay ------------------------------------
#
# A replay runs archived payloads through this same consumer, so the four
# names it reads and writes and the group it commits under have to be
# overridable. The defaults are the live ones, and a run may not mix: reading
# the replay topic while writing the live enriched topic would push replayed
# history into the warehouse through the live sink, which is the one thing the
# namespace exists to prevent (ADR 0010).

# A deliberate second copy of the namespace prefix, not an import: this module
# and consumers.bunching.config both name it, neither can import the other's
# (the Flink image mounts consumers/ only, and the enrichment is not in the job
# image), and test_replay_contract.py pins both strings.
REPLAY_PREFIX = "replay"


@dataclass(frozen=True)
class EnrichmentSettings:
    """Where this run reads, where it writes, and when it stops.

    The operational flags are fields too, so main() parses its arguments once
    and every use below reads a resolved value rather than a namespace.
    """

    # wiring
    source: str
    target: str
    dlq: str
    group: str
    # None for the live consumer, which runs until it is stopped. A replay
    # passes a number of seconds: its input is bounded, so it must end, and
    # "no records for N seconds" is the only signal a Kafka consumer gets that
    # the end has been reached.
    exit_when_idle_s: float | None
    # operational
    status: bool
    dry_run: bool
    schema_version: int
    from_beginning: bool
    batch: int
    report_every: float
    verbose: bool


def _cli() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="consumers.enrichment.run", description=__doc__)
    ap.add_argument("--status", action="store_true", help="show version + schema state, exit")
    ap.add_argument("--dry-run", action="store_true", help="enrich but publish nothing")
    ap.add_argument("--schema-version", type=int, choices=(1, 2), default=1,
                    help="1 = static join, 2 = + spatial")
    ap.add_argument("--from-beginning", action="store_true",
                    help="start at the earliest offset if the group has none")
    ap.add_argument("--batch", type=int, default=500, help="records per commit")
    ap.add_argument("--report-every", type=float, default=60.0)
    ap.add_argument("-v", "--verbose", action="store_true")
    # Replay wiring. Defaults are the live wiring, so a run that names none of
    # these is the live consumer.
    ap.add_argument("--source", default=SOURCE_TOPIC)
    ap.add_argument("--target", default=TARGET_TOPIC)
    ap.add_argument("--dlq", default=DLQ_TOPIC)
    ap.add_argument("--group", default=GROUP)
    ap.add_argument("--exit-when-idle", type=float, default=None, metavar="SECONDS",
                    help="exit after this long with no records (a replay's end)")
    return ap


def settings_from_args(argv: list[str] | None = None) -> EnrichmentSettings:
    """Parse the CLI, and refuse a run that spans the live and replay namespaces."""
    ns = _cli().parse_args(argv)
    _refuse_mixed_namespaces(ns.source, ns.target, ns.dlq, ns.group)
    return EnrichmentSettings(
        source=ns.source, target=ns.target, dlq=ns.dlq, group=ns.group,
        exit_when_idle_s=ns.exit_when_idle,
        status=ns.status, dry_run=ns.dry_run, schema_version=ns.schema_version,
        from_beginning=ns.from_beginning, batch=ns.batch,
        report_every=ns.report_every, verbose=ns.verbose,
    )


def _refuse_mixed_namespaces(source: str, target: str, dlq: str,
                             group: str) -> None:
    """All four names in the replay namespace, or none of them.

    The group is matched without the dot: a consumer group is named for humans
    in `rpk group list`, so a replay's is `replay-enrichment` while its topics
    are `replay.raw.vehicle_positions`. Same convention as
    consumers.bunching.config.run_settings.
    """
    names = {"source": source, "target": target, "dlq": dlq, "group": group}
    replayed = {k: v for k, v in names.items() if v.startswith(REPLAY_PREFIX)}
    if replayed and len(replayed) != len(names):
        live = ", ".join(f"--{k} {names[k]}"
                         for k in sorted(set(names) - set(replayed)))
        raise ValueError(
            f"refusing to mix namespaces: {', '.join(sorted(replayed))} "
            f"resolve(s) to replay names while {live} are live. A run that read "
            "the replay topic and wrote the live enriched topic would put "
            "replayed history into the warehouse; give all four "
            "--source/--target/--dlq/--group")


def main(argv: list[str] | None = None) -> int:
    cfg = settings_from_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if cfg.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S",
    )
    load_dotenv()

    sr = schema_mod.registry_client()
    if cfg.status:
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

    enrich = enrich_v1 if cfg.schema_version == 1 else enrich_v2
    log.info("enriching with v%d -> %s", cfg.schema_version, cfg.target)

    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        "group.id": cfg.group,
        # Manual commit: see the module docstring. Committing before the
        # produce is flushed would drop records Kafka thinks are done.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest" if cfg.from_beginning else "latest",
    })
    consumer.subscribe([cfg.source])
    publisher = None if cfg.dry_run else TopicPublisher(
        os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))

    # Serializer only when there is something to serialize. The generated
    # module is build output (gitignored), so importing it at module scope
    # would break --status and --dry-run on a fresh clone that has not run
    # `make schema-gen` yet.
    serializer = None
    if publisher:
        from schemas.enriched_vehicle_position_pb2 import EnrichedVehiclePosition

        serializer = schema_mod.build_serializer(
            sr, EnrichedVehiclePosition,
            # A replay writes records of the LIVE schema into a replay topic, so
            # it must be framed with the LIVE subject id: deriving it from the
            # topic would look up replay.enriched.vehicle_positions-value, which
            # nobody registered, and auto-registration is off (ADR 0010). The
            # live run passes None, which derives exactly the same subject.
            subject=f"{TARGET_TOPIC}-value"
            if cfg.target.startswith(REPLAY_PREFIX) else None,
        )
        log.info("serializing with %s (%s)", schema_mod.SUBJECT,
                 schema_mod.describe(sr).get("latest_version", "?"))

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    counters = Counters()
    last_report = time.monotonic()
    # When a record last arrived, for --exit-when-idle. Seeded here rather than
    # at the first record, so a replay whose input is already drained still
    # finishes instead of waiting forever for a first message.
    last_record = time.monotonic()
    batch: list = []

    try:
        while not _stop:
            beat()
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                counters.consumed += 1
                last_record = time.monotonic()
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
                            cfg.dlq, result.reason or DlqReason.UNKNOWN_TRIP_ID,
                            msg.value(), detail=result.detail,
                            entity_key=msg.key().decode() if msg.key() else None)
            elif msg is not None and msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                counters.errors += 1
                log.error("consume error: %s", msg.error())

            # A replay's end. A bounded source has no EOF on the wire: silence is
            # the only signal, so a run that must finish exits after N seconds
            # with no record. Folded into the flush condition below so the last
            # partial batch is published before the exit, which matters because
            # the fidelity comparison counts what this run wrote.
            idle_over = (cfg.exit_when_idle_s is not None
                         and time.monotonic() - last_record >= cfg.exit_when_idle_s)

            if batch and (len(batch) >= cfg.batch or _stop or idle_over):
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
                            cfg.target,
                            key=record["vehicle_id"].encode(),
                            value=serializer(
                                schema_mod.dict_to_message(record, EnrichedVehiclePosition),
                                SerializationContext(cfg.target, MessageField.VALUE)),
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

            if idle_over:
                log.info("no records for %.0fs: finishing (%s)",
                         cfg.exit_when_idle_s, counters)
                break

            if time.monotonic() - last_report >= cfg.report_every:
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
