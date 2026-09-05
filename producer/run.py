"""CLI entry point for the feed producer.

    python -m producer.run --list
    python -m producer.run --once
    python -m producer.run --feed vehicle_positions --once
    python -m producer.run --duration 24h
    python -m producer.run --dry-run --once      # fetch + decode, publish nothing

Deliberately runnable standalone. Airflow does NOT orchestrate this -- the
streaming/batch boundary in the proposal is that Airflow owns the static feed
refresh, dbt runs, partition maintenance and DLQ reporting, and never the
stream. This process runs under systemd, docker, or a terminal, and knows
nothing about Airflow. Same separation the parcel extractor has from dbt.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from dotenv import load_dotenv

from producer import feeds
from producer.archive import RawArchive
from producer.decode import decode
from producer.dedupe import LastValueCache
from producer.errors import FeedError, DecodeError, DlqReason
from producer.fetch import ConditionalFetcher
from producer.feeds import FeedSpec
from producer.publish import TopicPublisher, serialize

log = logging.getLogger("producer")


@dataclass
class FeedCounters:
    """Per-feed tallies for the periodic report."""

    fetched: int = 0
    unchanged: int = 0
    decoded: int = 0
    published: int = 0
    suppressed: int = 0
    archived: int = 0
    errors: int = 0
    dlq: int = 0


@dataclass
class Pipeline:
    """Everything one process needs to run.

    Bundled rather than passed as six arguments so process_feed's signature
    stays readable and so adding a component later is not a cascade of
    signature changes.
    """

    fetcher: ConditionalFetcher
    archive: RawArchive | None
    publisher: TopicPublisher | None
    caches: dict[str, LastValueCache] = field(default_factory=dict)
    counters: dict[str, FeedCounters] = field(default_factory=dict)

    def cache_for(self, spec: FeedSpec) -> LastValueCache:
        # Sized per feed, not from one global default -- the three differ by
        # more than an order of magnitude in distinct identities per poll.
        if spec.name not in self.caches:
            self.caches[spec.name] = LastValueCache(maxsize=spec.dedupe_maxsize)
        return self.caches[spec.name]

    def counters_for(self, spec: FeedSpec) -> FeedCounters:
        return self.counters.setdefault(spec.name, FeedCounters())


# --- the core -----------------------------------------------------------------


def process_feed(spec: FeedSpec, pipeline: Pipeline, dry_run: bool = False) -> None:
    """Run one feed through one full tick: fetch, archive, decode, dedupe, publish.

    The order is load-bearing:

      * A 304 short-circuits everything. It is the common path (~half of all
        ticks at the configured intervals) and there is nothing to do.
      * The payload is ARCHIVED before decoding. A payload that fails to
        decode is exactly the one you most want kept -- it is the evidence
        for the bug you are about to fix, and the replay demo re-runs
        archived bytes through changed decoders. An S3Error from the archive
        propagates on purpose: main() counts it as a feed error and the tick
        ends before any publish, because a record in Kafka with no
        replayable origin is worse than a missed tick.
      * Decode failures and per-record rejects (out-of-bounds positions) are
        DLQ candidates, not crashes: each is routed to dlq.<feed> with a
        reason from DlqReason, counted, and the tick continues. One bad poll
        must not kill a 24-hour run.
      * Value dedup runs after decode and before publish; only records whose
        fingerprint changed reach Kafka (ADR 0004).

    dry_run skips only the Kafka writes (publish and DLQ); fetch, archive,
    decode and dedupe all run, which is what makes `--dry-run --once` a safe
    rehearsal against the live feed.
    """
    counters = pipeline.counters_for(spec)
    result = pipeline.fetcher.fetch(spec)

    if result.unchanged:
        counters.unchanged += 1
        return
    counters.fetched += 1

    if pipeline.archive is not None:
        pipeline.archive.put(spec=spec, payload=result.body, fetched_at=result.fetched_at,
                             etag=result.etag, last_modified=result.last_modified)
        counters.archived += 1

    quarantine: list = []
    try:
        records = decode(spec=spec, payload=result.body, etag=result.etag,
                         quarantine=quarantine)
    except DecodeError as exc:
        counters.dlq += 1
        log.error("[%s] malformed payload -> DLQ: %s", spec.name, exc)
        if not dry_run and pipeline.publisher is not None:
            pipeline.publisher.publish_dlq(f"dlq.{spec.name}", DlqReason.MALFORMED_PAYLOAD,
                                           result.body, detail=str(exc))
        return

    counters.decoded += len(records)

    # Per-record rejects (out-of-bounds positions) go to the DLQ with their
    # reason rather than vanishing: they are a data-quality signal (GPS fix
    # absent, transposed coordinates), and the proposal wants them counted
    # and kept, not dropped.
    if quarantine:
        counters.dlq += len(quarantine)
        log.warning("[%s] %d record(s) out of bounds -> DLQ", spec.name, len(quarantine))
        if not dry_run and pipeline.publisher is not None:
            for record in quarantine:
                pipeline.publisher.publish_dlq(
                    f"dlq.{spec.name}",
                    DlqReason.POSITION_OUT_OF_BOUNDS,
                    serialize(record),
                    detail=f"lat={record.latitude} lon={record.longitude}",
                    entity_key=record.key,
                )

    fresh = pipeline.cache_for(spec).filter_new(records)
    counters.suppressed += len(records) - len(fresh)

    if not dry_run and pipeline.publisher is not None:
        counters.published += pipeline.publisher.publish(spec, fresh)

# --- scheduling ----------------------------------------------------------------


class Scheduler:
    """Tracks when each feed is next due.

    Per-feed intervals, because the feeds do not move at the same rate and
    polling alerts at the positions rate is rude for no benefit. A single
    thread is deliberate: three feeds at 10s intervals is nothing, and one
    thread means the dedupe caches need no locking and the ordering of
    archive-then-publish is trivially guaranteed.
    """

    def __init__(self, specs: list[FeedSpec]) -> None:
        self.specs = specs
        # Everything due immediately on the first pass.
        self._due: dict[str, float] = {s.name: 0.0 for s in specs}

    def ready(self, now: float) -> list[FeedSpec]:
        return [s for s in self.specs if now >= self._due[s.name]]

    def mark(self, spec: FeedSpec, now: float) -> None:
        """Schedule the next poll.

        Interval from `now` rather than from the previous due time: if a tick
        overruns (a 600 KB trip-updates decode on a loaded machine), fixed-rate
        scheduling would immediately fire again and could spiral. Skipping the
        missed slot is the right failure mode for a poller.
        """
        self._due[spec.name] = now + spec.poll_interval_s

    def sleep_until_next(self, now: float) -> float:
        """Seconds until the next feed is due, floored at zero."""
        return max(0.0, min(self._due.values()) - now)


_stop = False


def _handle_signal(signum, _frame) -> None:
    """SIGINT/SIGTERM -> finish the current tick, then exit cleanly.

    Important for a 24-hour run: a hard kill mid-publish leaves records in the
    local queue undelivered, and the flush in main() is what prevents that.
    """
    global _stop
    _stop = True
    log.info("signal %s received -- finishing current tick", signal.Signals(signum).name)


DURATION_RE = re.compile(r"^(\d+)([smhd])$")
_MULTIPLIER = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> float:
    """'24h' -> 86400.0."""
    match = DURATION_RE.match(text.strip())
    if not match:
        raise argparse.ArgumentTypeError(f"{text!r} is not a duration (e.g. 90s, 30m, 24h)")
    return float(match.group(1)) * _MULTIPLIER[match.group(2)]


def report(pipeline: Pipeline) -> None:
    """Periodic health line."""
    for name, c in sorted(pipeline.counters.items()):
        cache = pipeline.caches.get(name)
        log.info(
            "[%s] fetched=%d unchanged=%d decoded=%d published=%d "
            "suppressed=%d archived=%d dlq=%d errors=%d cache=%d",
            name, c.fetched, c.unchanged, c.decoded, c.published,
            c.suppressed, c.archived, c.dlq, c.errors,
            len(cache) if cache else 0,
        )
    log.info("feeds: %s", pipeline.fetcher.summary())


# --- CLI ----------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="producer.run", description=__doc__)
    parser.add_argument(
        "--feed", action="append", choices=sorted(feeds.FEEDS),
        help="restrict to one feed (repeatable). Default: all three.",
    )
    parser.add_argument("--once", action="store_true", help="one tick per feed, then exit")
    parser.add_argument(
        "--duration", type=parse_duration, metavar="T",
        help="run for this long then exit cleanly, e.g. 24h. Default: forever.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="fetch, archive and decode, but publish nothing. Exercises the "
             "whole path against the live feed without touching Kafka.",
    )
    parser.add_argument(
        "--no-archive", action="store_true",
        help="skip the MinIO write. For iterating on a decoder; never for a "
             "collection run, since it discards the replay story.",
    )
    parser.add_argument(
        "--report-every", type=float, default=60.0, metavar="S",
        help="seconds between health lines (default 60)",
    )
    parser.add_argument("--list", action="store_true", help="show the feed manifest and exit")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    load_dotenv()

    specs = [feeds.get(n) for n in (args.feed or sorted(feeds.FEEDS))]

    if args.list:
        print(f"{'feed':20} {'every':>6}  {'format':9} {'key':11} topic")
        for s in specs:
            print(
                f"{s.name:20} {s.poll_interval_s:>5}s  {s.wire_format:9} "
                f"{s.key_field:11} {s.topic}"
                + ("  [compacted]" if s.compacted else "")
            )
        return 0

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    pipeline = Pipeline(
        fetcher=ConditionalFetcher(),
        archive=None if args.no_archive else RawArchive(),
        publisher=None if args.dry_run else TopicPublisher(bootstrap),
    )
    if pipeline.archive:
        pipeline.archive.ensure_bucket()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info(
        "producing %s -> %s%s",
        ", ".join(s.name for s in specs),
        "(dry run, nothing published)" if args.dry_run else bootstrap,
        " [no archive]" if args.no_archive else "",
    )

    scheduler = Scheduler(specs)
    started = time.monotonic()
    last_report = started
    failures = 0

    while not _stop:
        now = time.monotonic()

        for spec in scheduler.ready(now):
            try:
                process_feed(spec, pipeline, dry_run=args.dry_run)
            except FeedError as exc:
                # One dead endpoint must not stop the others -- same isolation
                # the parcel extractor uses per county.
                pipeline.counters_for(spec).errors += 1
                failures += 1
                log.error("[%s] %s", spec.name, exc)
            except NotImplementedError as exc:
                log.error("not implemented: %s", exc)
                return 2
            except Exception as exc:  # noqa: BLE001 -- a 24h run must survive surprises
                pipeline.counters_for(spec).errors += 1
                failures += 1
                log.exception("[%s] unexpected: %s", spec.name, exc)
            scheduler.mark(spec, time.monotonic())

        if time.monotonic() - last_report >= args.report_every:
            report(pipeline)
            last_report = time.monotonic()

        if args.once:
            break
        if args.duration and time.monotonic() - started >= args.duration:
            log.info("duration reached")
            break

        time.sleep(min(scheduler.sleep_until_next(time.monotonic()), 1.0))

    if pipeline.publisher:
        undelivered = pipeline.publisher.flush()
        if undelivered:
            failures += 1

    report(pipeline)
    elapsed = time.monotonic() - started
    log.info(
        "ran %.1fs, %d failure(s), finished %s",
        elapsed, failures, datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
