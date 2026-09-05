# ADR 0004 — At-least-once with idempotent writes, and where dedup happens

**Status:** Accepted (Phase 1)
**Date:** 2026-09-04

## Context

All three GTFS-RT feeds are `FULL_DATASET` snapshots — confirmed on the wire
for each, and asserted in `tests/test_feed_semantics.py`. Every poll restates
every active entity whether or not anything changed. Duplicates are therefore
not an edge case produced by retries; they are the normal, dominant content of
the stream.

Phase 0 measured how dominant (`docs/findings.md` §6): across polls 45 s
apart, **67–72% of trip-update stop predictions are byte-identical to the
previous poll.**

## Decision

**At-least-once delivery with idempotent writes.** Not exactly-once.

Dedup happens at two distinct levels, which are easy to conflate and must not
be:

**1. Value-level dedup at the producer**, before publishing. A record is
published only if it differs from the last record seen for its key:

- positions: key `vehicle_id`, compared on `position.timestamp`
- trip updates: key `(trip_id, stop_id, stop_sequence)`, compared on
  `(arrival.time, departure.time)`
- alerts: key `alert_id`, compared on the whole payload

**2. Idempotent writes at the sink.** `raw.vehicle_positions` has primary key
`(vehicle_id, position_timestamp)` and the sink writes `ON CONFLICT DO
NOTHING`.

## Consequences

**Exactly-once is unnecessary, not merely expensive.** Its entire benefit is
preventing duplicate side effects; here the only side effect is a row write
whose key is naturally unique. At-least-once plus that key is already exactly
once *in effect*, without transactional producers, without a coordinator, and
without the throughput cost.

**Value-level dedup removes ~70% of trip-update write volume at no analytical
cost.** An unchanged prediction is not a new prediction. Given the projected
~80 M rows/day naive against ~24 M after dedup (§7 of findings), this is the
difference between a table that is a problem on this hardware and one that is
merely large.

**The critical distinction: dedup on the VALUE, never on the key alone.**
Deduping trip updates on `(trip_id, stop_id)` would collapse successive
predictions for the same stop into one — and successive predictions for one
stop *are* the data that the prediction-accuracy-by-lead-time analysis
(proposal §6.6) consumes. That dedup would silently delete the project's most
interesting finding while looking like a sensible optimisation.

This is why `raw.trip_updates`' primary key includes `ingested_at`: each
*changed* prediction for a stop is a distinct row, and only *unchanged*
restatements are suppressed.

**Positions dedup is simpler and stricter.** `(vehicle_id, position_timestamp)`
is unique within a poll (asserted in tests), and a vehicle whose GPS timestamp
has not advanced has genuinely not reported anything new.

**What at-least-once actually costs.** On consumer restart, records between
the last commit and the failure are reprocessed. For the raw sink that is
absorbed by the primary key. For the *stateful* processors in Phase 3 it is
not automatically safe — a windowed bunching count that double-counts a
replayed record produces a wrong alert. Phase 3 therefore has to make its
state updates idempotent or keyed on event identity. That obligation is
recorded here rather than discovered later.

**Ordering is a separate guarantee** and comes from the partition key
(ADR 0002), not from delivery semantics. At-least-once says nothing about
order; per-partition ordering does.
