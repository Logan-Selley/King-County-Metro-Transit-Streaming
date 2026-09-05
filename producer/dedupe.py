"""Value-level deduplication.

All three feeds are FULL_DATASET snapshots (asserted in
tests/test_feed_semantics.py). Every poll restates every active entity whether
or not anything changed. Duplicates are not an edge case produced by retries;
they are the normal, dominant content of the stream.

Measured: 67-72% of each trip-updates poll is byte-identical to the previous
one (findings.md §6). Suppressing that is ~two thirds of the write volume for
free, and it is the difference between ~80M rows/day and ~24M.

The invariant: dedupe on the VALUE, never on the key alone. Keyed dedupe of
trip updates on (trip_id, stop_id) would collapse successive predictions for
the same stop into one. Those successive predictions ARE the data that
prediction-accuracy-by-lead-time consumes -- the whole analysis is "how does
the estimate for this stop change as the bus approaches". It looks like a
sensible optimisation and silently removes proposal §6.6.

The dedupe identity is the record's `dedupe_key`, which coincides with the
Kafka key for positions and alerts but is (trip_id, stop_id, stop_sequence)
for trip updates -- see filter_new and ADR 0004 for why those must differ.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass

log = logging.getLogger("producer.dedupe")

# Fallback only. Real sizing is per feed, on FeedSpec.dedupe_maxsize, because
# the three feeds differ by more than an order of magnitude in distinct
# identities per poll and one global number cannot serve all three.
#
# THE FAILURE MODE THIS EXISTS TO AVOID IS A CLIFF, NOT A SLOPE.
#
# Once one poll's distinct identities exceed maxsize, that poll evicts its own
# earliest entries before the next poll can read them, and suppression drops
# from ~100% to 0% -- measured, on the trip updates fixture:
#
#     maxsize=2000 (>1295 records)   2nd pass on identical payload: 0 passed
#     maxsize=1295 (=1295 records)   2nd pass: 0 passed
#     maxsize= 800 (<1295 records)   2nd pass: 1295 passed   <-- total collapse
#
# There is no intermediate regime where suppression merely degrades. So the
# per-feed sizes below are set against PEAK, not the measured evening trough:
# dedup working all night and silently stopping at the 06:00 peak is the
# worst possible shape for this bug, and it would look like a volume spike
# rather than a cache problem.
DEFAULT_MAXSIZE = 60_000


@dataclass
class DedupeStats:
    seen: int = 0
    passed: int = 0
    suppressed: int = 0

    @property
    def suppression_rate(self) -> float:
        return self.suppressed / self.seen if self.seen else 0.0

    def __str__(self) -> str:
        return (
            f"{self.passed:,}/{self.seen:,} passed "
            f"({self.suppression_rate:.0%} suppressed)"
        )


class LastValueCache:
    """Remembers the last fingerprint seen per key, bounded LRU.

    One instance per feed -- keys from different feeds share a namespace
    otherwise, and a trip_id colliding with a vehicle_id is not hypothetical
    when both are bare numeric strings.

    Bounded because this process is meant to run for days. An unbounded dict
    keyed on trip_id grows with every service day and is a slow leak that
    only shows up on the run you care about.
    """

    def __init__(self, maxsize: int = DEFAULT_MAXSIZE) -> None:
        self.maxsize = maxsize
        self._seen: OrderedDict[str, tuple] = OrderedDict()
        self.stats = DedupeStats()

    def __len__(self) -> int:
        return len(self._seen)

    def _remember(self, key: str, fingerprint: tuple) -> None:
        """Record a fingerprint, evicting the least recently used if full."""
        if key in self._seen:
            self._seen.move_to_end(key)
        self._seen[key] = fingerprint
        while len(self._seen) > self.maxsize:
            self._seen.popitem(last=False)

    def _touch(self, key: str) -> None:
        """Refresh an identity's LRU position without changing its fingerprint.

        Called on a SUPPRESSION hit. Without it the cache is only ordered by
        last WRITE, not last use -- so a prediction that is stable for hours
        is never touched, drifts to the eviction end, and is eventually
        dropped and then re-emitted as though it were new. That is a spurious
        republish of data that did not change, which is exactly what this
        class exists to prevent.

        Only observable once the cache is near capacity, which the per-feed
        sizing above is meant to avoid -- but the two defences are
        independent, and this one costs a dict move.
        """
        self._seen.move_to_end(key)

    def filter_new(self, records: list) -> list:
        """Return only records whose value identity differs from the last seen.

        The cache is keyed on the record's DEDUPE identity (dedupe_key), not
        its Kafka key -- and the two differ for trip updates. One Kafka key
        (trip_id) carries ~30 stop predictions whose values change
        independently, so the dedupe identity is
        (trip_id, stop_id, stop_sequence), per ADR 0004. Keyed on the Kafka
        key, consecutive stops of one trip each look "changed" against the
        previous stop's fingerprint, every poll would republish everything,
        and suppression collapses to ~0%.

        Behaviour:

          * Unseen identity -> passes, and its fingerprint is remembered.
          * Seen identity, CHANGED fingerprint -> passes and updates the
            cache. This is the case that matters most: suppressing it would
            be keyed dedupe, which deletes the successive-prediction data
            the prediction-accuracy analysis consumes (ADR 0004).
          * Seen identity, same fingerprint -> suppressed.
          * Order is preserved, and the cache updates as the loop runs, so
            duplicates within one batch are also suppressed.
          * stats.seen / passed / suppressed are updated for every record.
            The feed health mart reads the suppression rate: ~70% for trip
            updates, much lower for positions. A rate near 0% means
            fingerprints are not comparing equal -- usually a float or a
            naive/aware datetime mismatch.
        """
        new_records = []
        for record in records:
            self.stats.seen += 1
            identity = record.dedupe_key if record.dedupe_key is not None else record.key
            seen_fingerprint = self._seen.get(identity)
            if seen_fingerprint is None or seen_fingerprint != record.fingerprint:
                self._remember(identity, record.fingerprint)
                new_records.append(record)
                self.stats.passed += 1
            else:
                # Suppressed, but still USED -- refresh its LRU position so a
                # long-stable identity does not age out and re-emit.
                self._touch(identity)
                self.stats.suppressed += 1
        return new_records

    # ------------------------------------------------------------------------
