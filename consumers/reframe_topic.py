"""Re-frame an existing topic's records in place, so the JDBC sink can read them.

    python -m consumers.reframe_topic alerts.bunching --dry-run
    python -m consumers.reframe_topic alerts.bunching

WHY THIS EXISTS. The two Flink jobs emit Confluent-framed records
(consumers/framing.py) so the sink can read them. Every record written before
framing has no magic byte, and a Confluent deserializer rejects those outright,
so a connector registered with `consumer.override.auto.offset.reset=earliest` --
what the enriched connector uses, and what a replay needs -- would die on the
first one.

Re-pointing the connector at `latest` would read nothing historical. That is
rejected on the numbers: `analytics.prediction_accuracy` holds about 900,000
records from an overnight run of the prediction job, and the error curve is
drawn FROM THE WAREHOUSE. Re-deriving them means replaying 47M trip updates
through the join; re-framing them is a read, five bytes each, and a write.

DESTRUCTIVE, SO IT DUMPS FIRST. Flink's output cannot be regenerated cheaply,
and a crash between "delete the topic" and "finish republishing" would lose the
tail. The framed records are therefore written to a file before anything is
dropped, and `--load` replays that file if the publish half fails.

The file holds keys and values base64-encoded, one JSON object per line.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import struct
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from consumers import framing

log = logging.getLogger("reframe")


def _client_config() -> dict:
    return {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")}


def drain(topic: str, poll_timeout_s: float = 30.0):
    """Every record in the topic, plus its partition count.

    Consumes to the high watermark rather than polling for a fixed duration: a
    quiet topic finishes at once, and a busy one is not cut off.
    """
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(_client_config())
    meta = admin.list_topics(topic=topic, timeout=10).topics[topic]
    if meta.error is not None:
        raise SystemExit(f"cannot read {topic}: {meta.error}")
    partitions = sorted(meta.partitions)

    consumer = Consumer({**_client_config(),
                         "group.id": f"reframe-{topic}-{int(time.time())}",
                         "enable.auto.commit": False,
                         "auto.offset.reset": "earliest"})
    try:
        # End offsets FIRST, or the drain races the producer and never
        # terminates on a topic that is still being written.
        ends = {}
        for part in partitions:
            _lo, hi = consumer.get_watermark_offsets(
                TopicPartition(topic, part), timeout=10)
            ends[part] = hi
        consumer.assign([TopicPartition(topic, p, 0) for p in partitions])

        out = []
        remaining = dict(ends)
        while any(remaining.values()):
            msg = consumer.poll(poll_timeout_s)
            if msg is None:
                raise SystemExit(
                    f"timed out with {sum(remaining.values())} of "
                    f"{sum(ends.values())} record(s) left")
            if msg.error():
                raise SystemExit(f"consume error: {msg.error()}")
            out.append((msg.key(), msg.value()))
            remaining[msg.partition()] -= 1
        return out, len(partitions)
    finally:
        consumer.close()


def topic_config(topic: str) -> dict:
    """The topic's existing non-default config, to carry over the recreate.

    READ rather than hardcoded. `make topics` owns these (both topics are 30-day
    retention), and a second copy of that number here is exactly how the two
    would drift apart.

    Non-default only: Redpanda reports a long list of broker-level defaults per
    topic, and replaying those back as overrides would pin values that were
    never ours to pin.
    """
    from confluent_kafka.admin import AdminClient, ConfigResource

    admin = AdminClient(_client_config())
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    for _res, fut in admin.describe_configs([resource]).items():
        entries = fut.result(timeout=30)
        return {name: entry.value
                for name, entry in entries.items()
                if not entry.is_default and not entry.is_read_only}
    return {}


def reframe(value: bytes, sid: int) -> bytes:
    """Frame a payload, or re-frame it if it carries a different schema id.

    Two cases. Bare JSON has no magic byte. A record that IS framed but with an
    OLDER id is one whose shape changed when the schemas gained integer epoch
    fields and the registry issued new ids: the id has to match the payload's
    shape, so carrying the old one forward would hand the connector a schema
    describing different types.

    The bare-JSON test is unambiguous. JSON always starts with '{' (0x7B), '[',
    '"', a digit, or one of `tfn-`; a Confluent frame always starts with 0x00.
    """
    if value[:1] == bytes([framing.MAGIC]):
        if len(value) >= 5 and struct.unpack(">I", value[1:5])[0] == sid:
            return value
        value = value[5:]
    # bytes(), because framing.frame returns a bytearray on purpose: PyFlink
    # writes a bytearray raw and pickles a bytes object (see framing.py). The
    # Kafka producer this file publishes through wants the opposite, and says so
    # with "argument 2 must be read-only bytes-like object, not bytearray".
    return bytes(framing.frame(value, sid))


# The epoch fields the connector converts to timestamps, per topic. They have
# to be INTEGERS, which is why the schemas declare `integer`: with floats the
# conversion dies on
#     Schema Schema{FLOAT64} does not correspond to a known timestamp type format
# having written nothing. Records already in the topic were written as floats,
# so the migration fixes the values, not only the schema.
TIMESTAMP_FIELDS = {
    "alerts.bunching": ("window_end",),
    "analytics.prediction_accuracy": ("issued_at", "predicted_arrival", "observed_at"),
}


def _json_candidates(payload: bytes):
    """Places the JSON might start, most likely first."""
    yield payload                                   # no frame at all
    if payload[:1] == bytes([framing.MAGIC]) and len(payload) >= 5:
        yield payload[5:]                           # one clean frame
    start = payload.find(b'{"')
    if start > 0:
        yield payload[start:]                       # junk, then the JSON


# raw_decode, not loads: it parses the FIRST JSON value and tolerates whatever
# follows. Some records here carry trailing bytes after their JSON, which makes
# loads() raise "Extra data" and cost the record.
_JSON = json.JSONDecoder()


def inner(payload: bytes) -> bytes:
    """The JSON out of whatever is in front of it, and behind it.

    Not a fixed five-byte strip, because not every record in these topics is
    cleanly framed. Measured: 1,218 records in alerts.bunching carry a valid
    frame, then 16 bytes of binary, then the frame and the JSON again; the job
    emitted a pickle protocol 5 wrapper (80 05 95 ...) around every record
    before framing.frame was changed to return a bytearray. A migration that
    assumes a clean header either crashes (on byte 0x80) or, worse, re-frames
    the junk.

    Returns exactly the JSON object's bytes: leading junk dropped, trailing
    bytes dropped.
    """
    for candidate in _json_candidates(payload):
        # Trim to the last closing brace before decoding. What follows a JSON
        # object in these records is binary -- pickle's MEMOIZE and STOP (94 2e)
        # after the pickle wrapper's length field -- and one undecodable byte
        # fails the whole candidate: measured, 335 of 2,554 records were skipped
        # with "can't decode byte 0x94 in position 261" while the JSON in front
        # of it was perfectly good.
        #
        # Trimming is safe rather than clever. If the last brace is not the
        # object's end, raw_decode fails and the record is skipped, so a wrong
        # guess costs a record instead of publishing a corrupted value.
        brace = candidate.rfind(b"}")
        if brace < 0:
            continue
        candidate = candidate[:brace + 1]
        try:
            text = candidate.decode("utf-8")
            _value, end = _JSON.raw_decode(text)
        except (ValueError, UnicodeDecodeError):
            continue
        return text[:end].encode("utf-8")
    return payload


def coerce_epochs(payload: bytes, fields) -> bytes:
    """Float epoch seconds -> int, for the fields named.

    Only integral floats, and only those fields: this is the one shape change
    the schemas made, and rewriting anything else would be the migration
    inventing edits. Returns the original bytes when there was nothing to do, so
    a second run is a no-op.
    """
    record = json.loads(payload.decode("utf-8"))
    changed = False
    for field in fields:
        value = record.get(field)
        if isinstance(value, float) and value.is_integer():
            record[field] = int(value)
            changed = True
    return json.dumps(record).encode() if changed else payload


def dump(records, path: Path) -> None:
    """Write the drained (key, value) pairs to `path`, one base64 JSON object per line."""
    with path.open("w") as fh:
        for key, value in records:
            fh.write(json.dumps({
                "key": base64.b64encode(key).decode() if key else None,
                "value": base64.b64encode(value).decode(),
            }) + "\n")


def load(path: Path):
    """Read back a dump written by dump(): the framed records, ready to republish."""
    out = []
    for line in path.read_text().splitlines():
        row = json.loads(line)
        out.append((base64.b64decode(row["key"]) if row["key"] else None,
                    base64.b64decode(row["value"])))
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="consumers.reframe_topic", description=__doc__)
    ap.add_argument("topic")
    ap.add_argument("--dump", type=Path, default=None,
                    help="where to write the framed records "
                         "(default: /tmp/<topic>.reframed.jsonl)")
    ap.add_argument("--load", type=Path, default=None,
                    help="skip the read and republish an existing dump")
    ap.add_argument("--dry-run", action="store_true",
                    help="read and frame, write nothing and delete nothing")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    load_dotenv()

    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    dump_path = args.dump or Path(
        f"/tmp/{args.topic.replace('.', '_')}.reframed.jsonl")

    admin = AdminClient(_client_config())
    if args.load:
        # Already framed and coerced by the --dump that produced this file.
        framed = load(args.load)
        partitions = len(admin.list_topics(topic=args.topic,
                                           timeout=10).topics[args.topic].partitions)
        log.info("loaded %s framed record(s) from %s", f"{len(framed):,}", args.load)
    else:
        raw, partitions = drain(args.topic)
        sid = framing.schema_id(
            os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081"),
            f"{args.topic}-value")
        log.info("%s: %s record(s) across %s partition(s), schema id %s",
                 args.topic, f"{len(raw):,}", partitions, sid)
        fields = TIMESTAMP_FIELDS.get(args.topic, ())
        framed, skipped, first_error = [], 0, None
        for key, value in raw:
            try:
                framed.append((key, reframe(coerce_epochs(inner(value), fields), sid)))
            except (ValueError, UnicodeDecodeError) as exc:
                # A record whose JSON cannot be recovered from whatever is in
                # front of it. Skipped rather than published: a record the
                # connector cannot read fails the whole task under
                # errors.tolerance=none, so keeping one costs the entire sink.
                skipped += 1
                first_error = first_error or exc
        if skipped:
            log.warning("skipped %s of %s record(s) as unnormalisable: %s",
                        f"{skipped:,}", f"{len(raw):,}", first_error)
        dump(framed, dump_path)
        log.info("dumped to %s", dump_path)

    if args.dry_run:
        log.info("dry run: %s record(s) framed, nothing written or deleted",
                 f"{len(framed):,}")
        return 0

    # Only now is anything destroyed, and the dump above is the undo.
    try:
        config = topic_config(args.topic)
    except Exception as exc:  # noqa: BLE001 -- an absent topic is a normal --load case
        log.warning("could not read the topic config (%s); recreating with defaults", exc)
        config = {}

    for _topic, fut in admin.delete_topics([args.topic]).items():
        fut.result(timeout=30)
    log.info("deleted %s", args.topic)

    # Partition count AND config are carried over rather than defaulted. Both
    # were chosen in `make topics` for their own reasons (the same argument ADR
    # 0002 makes for the enriched topic), and `rpk topic create` does not
    # re-apply config to a topic that already exists -- so re-running `make
    # topics` afterwards would NOT restore the retention window.
    create = admin.create_topics([NewTopic(args.topic, num_partitions=partitions,
                                           replication_factor=1,
                                           config=config)])
    for _topic, fut in create.items():
        fut.result(timeout=30)
    log.info("recreated %s with %s partition(s) and config %s",
             args.topic, partitions, config or "{}")

    producer = Producer(_client_config())
    counter = {"ok": 0, "failed": 0}

    def _acked(err, _msg):
        counter["failed" if err else "ok"] += 1
        if err is not None:
            log.error("delivery failed: %s", err)

    for key, value in framed:
        # The producer's local queue holds 100k messages / 30 MB by default, so
        # a 1.3M-record backfill overruns it and `produce` raises BufferError
        # rather than blocking. Poll until there is room: this is a backfill, so
        # waiting is correct and dropping is not. (Measured: without this the
        # first run died having published 325,808 of 1,346,226.)
        while True:
            try:
                producer.produce(args.topic, key=key, value=value, callback=_acked)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
    producer.flush(60)

    log.info("republished %s of %s record(s)",
             f"{counter['ok']:,}", f"{len(framed):,}")
    if counter["failed"] or counter["ok"] != len(framed):
        log.error("NOT everything was delivered. Replay the dump: "
                  "python -m consumers.reframe_topic %s --load %s",
                  args.topic, dump_path)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
