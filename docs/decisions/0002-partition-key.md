# ADR 0002: `vehicle_id` as the partition key for position topics

**Status:** Accepted (Phase 1)
**Date:** 2026-09-04

## Context

Kafka guarantees ordering *within a partition*, not across a topic. The
partition key therefore decides what the pipeline can compute correctly, and
it is effectively irreversible once history exists, rekeying means
reprocessing.

Three candidates, per the proposal:

| Key | Ordering guarantee | Load distribution |
|---|---|---|
| `vehicle_id` | per vehicle | even (fleet is homogeneous) |
| `route_id` | per route | severe hot partitions on RapidRide |
| geohash prefix | per geographic cell | even |

## Decision

Key `raw.vehicle_positions` and `enriched.vehicle_positions` on `vehicle_id`.

Key `raw.trip_updates` on `trip_id` (see ADR 0004, the trip-updates feed's
`vehicle.id` is populated on only ~46% of records).

Key `raw.service_alerts` on `alert_id`, which is a compaction requirement
rather than an ordering one.

## Consequences

**Per-vehicle ordering is guaranteed, and the analysis depends on it more than
the proposal anticipated.** Two independent computations require consecutive
positions for one vehicle to arrive in order:

1. Schedule deviation. A position update arriving out of order corrupts the
   linear-referencing calculation, which was the original justification.
2. **Heading and speed.** Phase 0 measured `position.bearing` at 2.1%
   population and `position.speed` at 1.8% (`docs/findings.md` §4). Both are
   effectively absent from the feed, so any enrichment needing them must
   derive them from successive positions of the same vehicle. This was not
   known when the proposal was written and it makes the key choice
   substantially less optional.

**Load is even.** 280 vehicles observed at an evening trough, several times
that at peak, each emitting at the same ~20 s cadence. There is no equivalent
of a hot route because the fleet is homogeneous at the vehicle level.

**What is given up.** Route-level windowed aggregation, bus bunching being
the obvious one, cannot rely on all vehicles of a route landing in one
partition. The bunching detector (Phase 3) must therefore either consume all
partitions and key its own state store by `route_id`, or re-key through an
intermediate topic. This is a genuine cost and it lands squarely on the
hardest phase.

It is accepted because the alternative is worse: `route_id` creates hot
partitions on high-frequency routes like RapidRide, and it breaks the two
computations above, which are foundational rather than one analysis among six.
A stream processor re-keying its own state is ordinary; reconstructing
per-vehicle ordering after the fact is not possible.

**Geohash prefix** balances load and preserves spatial locality, which is
attractive for the neighbourhood join. It breaks per-vehicle ordering
entirely, a vehicle crossing a cell boundary changes partition mid-trip,
which rules it out for the same reason.

## Notes

Partition counts (3 for positions, 6 for trip updates) are not throughput
driven; at these volumes one partition would keep up comfortably. They exist
so that consumer-group rebalancing and per-key ordering are demonstrable at
all, which needs more than one partition.
