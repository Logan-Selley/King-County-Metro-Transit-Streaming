"""Replay archived feed payloads into the replay namespace. Phase 6, step 6B.

    python -m producer.replay --feed vehicle_positions \\
        --start 2026-09-24T00:00:00Z --end 2026-09-25T00:00:00Z

Re-runs stored payloads through the SAME code the live producer runs, with the
archive standing in for the network. `process_feed` in producer/run.py reads
its input from `pipeline.fetcher`, so a replay is that loop with an
ArchiveFetcher plugged in: decode, DLQ routing, value dedupe and publish are the
live implementations, not copies of them. That is the property the whole phase
rests on. A replay built from a second implementation of the pipeline would
prove that the second implementation works.

THE THREE PIECES OF THIS STEP: `ArchiveFetcher`, `replay_spec` and
`run_replay`, specified in their docstrings and executed by
tests/test_replay_contract.py. Plus two small changes outside this file, both in
the contract suite:

  * FeedSpec gains a `dlq` field and a `dlq_topic` property
    (`self.dlq or f"dlq.{self.name}"`), and process_feed routes both of its DLQ
    publishes through `spec.dlq_topic`. Today they are the literal
    f"dlq.{spec.name}", so a replay would write its rejects into the LIVE DLQ
    topics, and transit_health's dlq_report would count them.
  * Nothing else in the producer changes. If the replay needs a change to
    process_feed beyond that one, the replay is no longer running the live code.

THE TWO DETERMINISM TRAPS, measured by reading the code rather than guessed:

  * The clock. The only wall-clock read on this path is the fetcher's
    `fetched_at = datetime.now()`. The archive key carries the original fetch
    time (<epoch>-<etag>.<ext>), so the replay's fetched_at comes from the key,
    and nothing downstream of the fetcher reads the clock. (Enrichment does,
    for `enriched_at`; see the compare step.)
  * The dedupe cache starts empty. The live LastValueCache had seen everything
    before the window, so the replay's FIRST payload publishes every vehicle,
    where the live producer published only what had changed. Those extra
    records carry position timestamps from before the window. They are not
    wrong, they are the same records the live topic received earlier, and the
    compare step restricts both sides to position timestamps inside the window.
    The live producer also restarted with empty caches on every crash on 09-23,
    so this is the same thing live already did several times.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

from producer.feeds import FEEDS, FeedSpec
from producer.fetch import FetchResult

log = logging.getLogger("replay")

# Every topic a replay may write to starts with this. Checked before anything is
# produced (guard_topics), because the one mistake a replay must not be able to
# make is writing history into a live topic, where the sinks would upsert it
# into the warehouse and the detector would alert on it.
REPLAY_PREFIX = "replay."

# <prefix>/<YYYY>/<MM>/<DD>/<HH>/<epoch>-<etag>.<ext>, as producer/archive.py's
# key_for writes it. The epoch is whole seconds of the ORIGINAL fetch, in UTC.
_KEY = re.compile(
    r"^(?P<prefix>raw/[a-z_]+)/(?P<y>\d{4})/(?P<m>\d{2})/(?P<d>\d{2})/(?P<h>\d{2})/"
    r"(?P<epoch>\d+)-(?P<etag>[^/]+)\.(?P<ext>pb|json)$"
)


@dataclass(frozen=True)
class ArchivedPayload:
    """One archive key, parsed."""

    key: str
    fetched_at: datetime
    etag: str


def parse_key(key: str) -> ArchivedPayload:
    """The archive key back into its original fetch time and ETag.

    Raises ValueError on anything that is not an archive key, rather than
    skipping it: an unparseable key in the replay range means the archive has
    something in it this code does not understand, and a replay that silently
    drops it would report fidelity it does not have.

    The hour directory and the epoch are checked against each other. They come
    from the same datetime in key_for, so a disagreement means a hand-copied or
    renamed object, not a clock.
    """
    m = _KEY.match(key)
    if not m:
        raise ValueError(f"not an archive key: {key!r}")
    fetched_at = datetime.fromtimestamp(int(m["epoch"]), tz=timezone.utc)
    if fetched_at.strftime("%Y%m%d%H") != f"{m['y']}{m['m']}{m['d']}{m['h']}":
        raise ValueError(f"hour directory and epoch disagree: {key!r}")
    return ArchivedPayload(key=key, fetched_at=fetched_at, etag=m["etag"])


def guard_topics(topics: list[str]) -> None:
    """Refuse to run if any topic is outside the replay namespace."""
    live = [t for t in topics if not t.startswith(REPLAY_PREFIX)]
    if live:
        raise SystemExit(f"refusing to replay into live topic(s): {live}")


# =============================================================================
# 6B: the fetcher, the spec and the driver
# =============================================================================

@dataclass(frozen=True)
class _Pending:
    """A key queued for replay: what fetch() needs except the body itself."""

    key: str
    fetched_at: datetime
    etag: str


class ArchiveFetcher:
    """The archive in the fetcher's seat.

    Built from a reader (anything with `iter_keys(spec, prefix)` and
    `get(key)`, which producer/archive.py's RawArchive has), a half-open
    window [start, end) in UTC, and `feeds`: the FeedSpecs this replay is for.

    CONTRACT
      * ONLY THE FEEDS GIVEN. The archive holds
        every feed in every hour, measured: 204 vehicle_positions, 203
        trip_updates and 60 service_alerts keys in 09-24 08:00. The first
        implementation, told nothing about which feed it served, queued all
        three, so a vehicle-positions replay never reached `exhausted` and
        its next fetch popped an empty queue (IndexError). Feeds are matched
        by NAME, because fetch is called with the replay spec, whose name is
        the live one. fetch for a feed not given is an error.
      * BODIES ARE READ IN fetch(), ONE AT A TIME. Listing the window up front
        is fine (keys are small, and `exhausted` needs the count before the
        first fetch); downloading it up front is not: a day of trip_updates is
        ~4,900 payloads at ~130 KB, and even vehicle_positions alone is ~30 MB
        a day held for nothing.
      * fetch(spec) returns the NEXT archived payload for that feed as a
        FetchResult: status 200, body = the archived bytes, etag from the key,
        fetched_at from the key (never the clock), elapsed_ms 0. Oldest first,
        each key exactly once.
      * Only keys whose fetched_at is inside [start, end). List by hour prefix
        (the archive is laid out so "replay 08:00-09:00" is a prefix scan) and
        filter the edges by the parsed time, not by string comparison.
      * No 304s. A 304 was never archived (process_feed returns before the
        archive on `unchanged`), so there is nothing to replay for it.
      * `exhausted` is True once every key in the window has been returned for
        every feed in `feeds`. run_replay stops on it.
      * A key that fails parse_key propagates, for the reason parse_key gives.
      * THE ETAG IS NOT STORED THE WAY THE LIVE PATH SAW IT. key_for strips the
        quotes (and any W/) before writing the key, and so does the object's
        x-amz-meta-source-etag. But decode copies the header value into every
        record's feed_etag, and live rows carry it quoted. Measured in
        raw.enriched_vehicle_positions: "210aa75e91dbe67c470ed26eb21d6906",
        quotes included. A fetcher that hands decode the key's bare form makes
        every replayed row differ from live in that one column, with correct
        logic. The replay restores the quotes in _keys, and the case it cannot
        recover is a weak W/ ETag, whose prefix key_for drops with the quotes.
    """

    def __init__(self, reader, start: datetime, end: datetime,
                 feeds: Iterable[FeedSpec]) -> None:
        self._reader = reader
        self._start = start
        self._end = end
        # ONLY the feeds given, keyed by name, because fetch is called with the
        # REPLAY spec and its name is the live one. The archive holds every feed
        # in every hour (204 vehicle_positions, 203 trip_updates and 60
        # service_alerts keys in 09-24 08:00), so queuing the others left
        # `exhausted` False forever for a one-feed replay, whose next fetch then
        # popped an empty queue.
        #
        # Listed here rather than on first use because `exhausted` has to be
        # answerable before the first fetch: a window with nothing in it must
        # report exhaustion immediately, and run_replay's loop asks before it
        # fetches. Listing is what that costs, and listing is cheap. The bodies
        # are read in fetch().
        self._queues: dict[str, list[_Pending]] = {
            spec.name: self._keys(spec) for spec in feeds
        }
        # For ReplayStats: the window actually covered, which is narrower than
        # [start, end) when the archive has gaps.
        self.first_fetched: datetime | None = None
        self.last_fetched: datetime | None = None

    def _hour_prefixes(self):
        """The hour prefixes [start, end) touches, oldest first.

        The archive is laid out so that an hour is a prefix scan
        (FEEDS.archive_prefix), which is what keeps a one-hour replay from
        listing the whole feed.
        """
        hour = self._start.replace(minute=0, second=0, microsecond=0)
        while hour < self._end:
            yield f"{hour:%Y/%m/%d/%H}/"
            hour += timedelta(hours=1)

    def _keys(self, spec: FeedSpec) -> list[_Pending]:
        out: list[_Pending] = []
        for prefix in self._hour_prefixes():
            for obj in self._reader.iter_keys(spec, prefix):
                # RawArchive.iter_keys yields ArchivedObject; the contract test's
                # fake yields the key itself. Both are keys to this code.
                key = getattr(obj, "key", obj)
                # NOT skipped when it does not parse: a key the archive holds
                # that this code cannot read means the replay does not know what
                # it is replaying, and quietly dropping it would report fidelity
                # it does not have.
                payload = parse_key(key)
                if not self._start <= payload.fetched_at < self._end:
                    continue
                # The quotes are RESTORED, not decoration: archive.py's key_for
                # strips them (and any W/) before writing the key, while every
                # live row's feed_etag carries the header's quoted form. Without
                # this every replayed row would differ from live in that one
                # column, with correct logic. Measured 2026-09-26: all 244,906
                # rows in the last six hours carry quoted strong ETags, none are
                # W/, so restoring the quotes is exact for this feed.
                out.append(_Pending(key=key, fetched_at=payload.fetched_at,
                                    etag=f'"{payload.etag}"'))
        # By parsed time, not by key: the key sorts right, but the contract is
        # about the fetch time, and this is where the two could diverge.
        out.sort(key=lambda pending: pending.fetched_at)
        return out

    def fetch(self, spec: FeedSpec) -> FetchResult:
        try:
            queue = self._queues[spec.name]
        except KeyError:
            raise KeyError(f"{spec.name} is not one of this replay's feeds "
                           f"({', '.join(sorted(self._queues)) or 'none'})") from None
        pending = queue.pop(0)
        # The body is read here, one fetch at a time, not when the window was
        # listed: a day of trip_updates is ~4,900 payloads at ~130 KB, and a
        # listing that downloaded them would hold all of it for a replay that
        # reads one feed.
        result = FetchResult(
            spec=spec,
            status=200,
            fetched_at=pending.fetched_at,
            elapsed_ms=0,
            body=self._reader.get(pending.key),
            etag=pending.etag,
        )
        self.last_fetched = result.fetched_at
        if self.first_fetched is None:
            self.first_fetched = result.fetched_at
        return result

    @property
    def exhausted(self) -> bool:
        return all(not queue for queue in self._queues.values())


def replay_spec(spec: FeedSpec) -> FeedSpec:
    """The same feed, publishing into the replay namespace.

    CONTRACT
      * topic -> REPLAY_PREFIX + spec.topic
      * dlq_topic -> REPLAY_PREFIX + the live dlq topic (needs the FeedSpec
        change described in the module docstring)
      * EVERYTHING ELSE UNCHANGED: name (so archive_prefix still points at the
        live archive, which is the input), wire format, key field, dedupe size.
        A replay spec that differs in anything but where it writes is running
        different logic, and the fidelity check would be measuring that.
    """
    return replace(
        spec,
        topic=f"{REPLAY_PREFIX}{spec.topic}",
        # Through the property, so a replay of a spec that already carries an
        # override still lands in the namespace rather than at the override's
        # live name.
        dlq=f"{REPLAY_PREFIX}{spec.dlq_topic}",
    )


@dataclass
class ReplayStats:
    payloads: int = 0
    published: int = 0
    suppressed: int = 0
    dlq: int = 0
    first_fetch: datetime | None = None
    last_fetch: datetime | None = None


def run_replay(spec: FeedSpec, fetcher: ArchiveFetcher, publisher) -> ReplayStats:
    """Drive process_feed over the archive until the window is exhausted.

    CONTRACT
      * guard_topics([replay topic, replay dlq topic]) BEFORE anything is
        produced; a spec that is not a replay spec fails here.
      * A Pipeline with this fetcher, archive=None, and this publisher. archive
        must be None: the replay reads the archive and must not write to it.
        (archive_reader's MinIO policy also forbids it, so a mistake fails on
        the first put rather than duplicating history, but the None is the
        intent and the policy is the backstop.)
      * process_feed(spec, pipeline) once per archived payload, in order, until
        fetcher.exhausted. Nothing else in the loop: no scheduler, no sleeps,
        no reports on a timer. The replay runs as fast as the broker accepts.
      * Flush the publisher before returning, and fill ReplayStats from the
        pipeline's FeedCounters.
      * Deterministic: the same window twice gives the same published records
        in the same order.
    """
    # Before anything is produced: a spec that is not a replay spec fails here,
    # which is the one mistake a replay must not be able to make.
    guard_topics([spec.topic, spec.dlq_topic])

    # Imported here, not at module scope: producer.run pulls in the Kafka client,
    # and this module's CLI and contract tests do not need it until they run a
    # replay.
    from producer.run import Pipeline, process_feed

    # archive=None is the intent, not an oversight: archive_reader's MinIO policy
    # also forbids a put, so a mistake here fails on the first one rather than
    # duplicating the history the replay is reading.
    pipeline = Pipeline(fetcher=fetcher, archive=None, publisher=publisher)

    # As fast as the broker accepts: no scheduler, no sleeps, no timed reports.
    while not fetcher.exhausted:
        process_feed(spec, pipeline)

    publisher.flush()

    counters = pipeline.counters_for(spec)
    return ReplayStats(
        payloads=counters.fetched,
        published=counters.published,
        suppressed=counters.suppressed,
        dlq=counters.dlq,
        first_fetch=fetcher.first_fetched,
        last_fetch=fetcher.last_fetched,
    )


# =============================================================================
# CLI
# =============================================================================

def _utc(text: str) -> datetime:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{text!r}: give a timezone (Z or +00:00)")
    return dt.astimezone(timezone.utc)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="producer.replay", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--feed", default="vehicle_positions", choices=sorted(FEEDS))
    ap.add_argument("--start", type=_utc, required=True, help="UTC, inclusive")
    ap.add_argument("--end", type=_utc, required=True, help="UTC, exclusive")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")
    from dotenv import load_dotenv

    from producer.archive import RawArchive
    from producer.publish import TopicPublisher

    load_dotenv()
    spec = replay_spec(FEEDS[args.feed])
    guard_topics([spec.topic, spec.dlq_topic])

    # archive_reader, not archive_writer and not the root: list + get under
    # raw/ and nothing else (terraform/core/replay.tf).
    reader = RawArchive(access_key=os.environ.get("ARCHIVE_READER_USER", "archive_reader"),
                        secret_key=os.environ["ARCHIVE_READER_SECRET"])
    publisher = TopicPublisher(bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))
    stats = run_replay(spec, ArchiveFetcher(reader, args.start, args.end, feeds=[spec]),
                       publisher)

    log.info("replayed %s payloads [%s .. %s]: published %s, suppressed %s, dlq %s",
             f"{stats.payloads:,}", stats.first_fetch, stats.last_fetch,
             f"{stats.published:,}", f"{stats.suppressed:,}", f"{stats.dlq:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
