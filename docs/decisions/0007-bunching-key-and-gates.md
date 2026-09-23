# ADR 0007: Keying the bunching detector, and confirming its candidates

**Status:** Accepted (Phase 3)
**Date:** 2026-09-20

## Context

`consumers/bunching/config.py` detects bunching on `shape_dist_traveled`
rather than stop arrivals, because only ~27% of position records carry
`STOPPED_AT` while 100% carry a shape distance. That decision stands. It
rests on an assumption the file states plainly:

> The detector keys on (route_id, direction_id), and a route's vehicles share
> a shape, so it stays on the safe side.

Measured against the loaded feed, **that assumption is false.**

```
(route, direction) pairs with 1 shape : 179
                             2 shapes :  73
                             3 shapes :  16
                             4 shapes :   9
                             5 shapes :   3

trips on a multi-shape pair: 14,054 of 31,688 = 44.4%
```

`shape_dist_traveled` is measured along a specific geometry from that
geometry's origin. Two shapes on the same route and direction can start in
different places, so equal distances do not imply adjacency. Sorting a mixed
set by distance and comparing neighbours computes gaps between buses that
are miles apart.

How bad depends on the shapes, and that is also measurable. Across every
multi-shape (route, direction) group:

| shape-pair origin offset | shape pairs | route-directions affected |
|---|---|---|
| within 50 m | 105 | 78 |
| 50 m to 500 m | 8 | 7 |
| **over 500 m** | **92** | **38** |

So the exposure is real (38 route-directions can produce a bogus gap) but so
is the coverage that a naive fix would destroy: 105 shape pairs share an
origin and are genuinely comparable.

## Decision

**Key on `(route_id, direction_id)`. Confirm every candidate pair with
straight-line distance before emitting.**

Detection stays a two-step filter:

1. `shape_dist_traveled` gap below `gap_threshold_ft` selects a **candidate**.
2. `confirm_proximity` requires the two vehicles' reported `latitude`/
   `longitude` to be within the same bound, plus 25% for GPS scatter.

Both must hold.

**Also: `shape_dist_traveled` below `MIN_PROGRESS_FT` (100 ft) excludes a
vehicle entirely.** Separate gate, separate finding, below.

## Consequences

**The conjunction is correct in both directions, which neither half is.**

For two points on one path, straight-line distance never exceeds distance
along the path. A genuine pair therefore satisfies both tests, and a pair
whose shapes start 8 km apart reports a tiny gap and a huge straight-line
distance, and fails the second.

The reverse matters equally. Straight-line proximity alone calls two buses
bunched when they are on opposite legs of a loop, close on a map and 5 km
apart along the route. Shape distance is what carries direction and ordering;
straight-line distance is what carries physical truth. Neither substitutes for
the other.

**Two alternatives were considered and rejected.**

*Key on `(route_id, direction_id, shape_id)`.* Correct, and it throws away
comparisons that matter. A Line direction 1 runs two shapes that share a start
point and differ by 44 m in total length over 18 km:

```
A Line  dir 1  shape 10671009  209 trips  18,117 m
A Line  dir 1  shape 10671010  101 trips  18,073 m
```

Those vehicles are comparable to within metres, and a shape key stops
comparing 101 of 310 trips against the other 209, on one of the exact
RapidRide lines the Phase 3 exit criterion requires the detector to be
credible on. Of the 15 RapidRide (route, direction) pairs, 11 run a single
shape and the 4 that do not all share origins. The one genuinely offset
variant in that set is C Line direction 0 shape `21673005`: **one trip**, 6.9
km long, starting 8.2 km from the main shape. Splitting every route's key to
isolate one trip is the wrong trade.

*Precompute shape compatibility from static GTFS.* This is the rigorous
version: group shapes by shared origin, key on the group. It needs the static
feed inside the Flink job, which means either a broadcast stream or a second
PostGIS connection from a TaskManager, and it still does not answer whether
two origin-sharing shapes stay together at the point the vehicles actually
are. Shared origin is not sufficient: of the 105 origin-sharing pairs, **67
differ in total length by more than 10%**, so they diverge somewhere. The
confirmation check answers the question those shapes raise, per pair, per
window, using data already on the record.

**The cost is one haversine per candidate pair**, not per record. Candidates
are rare. `latitude` and `longitude` become required fields in
`parse_record`, which is why they are in the contract suite.

**What this gives up.** A pair on genuinely incomparable shapes that happens
to be physically close is still reported, with a gap number measured along
two different geometries. The straight-line distance in the alert is the
trustworthy field in that case. Worth watching in the 3E spot-check rather
than pre-solving.

## The layover gate

Separate finding, same review pass. Sampling 8,000 live enriched records:

```
shape_dist_traveled == 0          12.6%
                     < 100 ft      0.9%
                     < 1,000 ft    2.6%
                    >= 1,000 ft   83.9%

zeros by current_status:  STOPPED_AT 966 | IN_TRANSIT_TO 45
```

12.6% of the stream sitting at exactly zero looks like a loader bug. It is
not. Sampling 40 of those records and measuring each against its shape's start
point in PostGIS: **1.7 m minimum, 27.8 m mean, 86.5 m maximum.** The
projection is correct and `locate_on_shape` is fine. These are buses parked at
the first stop of a trip that has not begun.

Two of them have a gap of zero and would alert. That is a layover, not
bunching.

`MIN_PROGRESS_FT = 100` excludes them. The threshold is safe because the
distribution near the origin is **bimodal**, not continuous: 12.6% at exactly
zero, then 0.9% across the entire next 100 ft. The gate discards under 1% of
real positions to remove the whole layover population. The bimodality is
itself the evidence that zero is a distinct state rather than the bottom of a
range.

Requiring both vehicles to be `IN_TRANSIT_TO` was the other option. It
discards ~27% of records including buses genuinely bunched at a stop together,
which is real bunching and arguably the most visible kind.

## Note on the alert format

`alerts.bunching` carries **JSON**, and no schema is registered for it.

Not an oversight, and the reasoning is ADR 0006's. Protobuf on that topic
needs the `ProtobufSerializer` inside the Flink image, and `apache-flink` pins
`protobuf<6` through `apache-beam` while this project runs 7.36.1, the exact
conflict that put the job in its own image. Registering the subject would mean
either downgrading protobuf inside that image or maintaining two generated
bindings for one message.

The precedent is ADR 0003's: `raw.service_alerts` is JSON for its own reasons,
and the registry story lives on `enriched.vehicle_positions`, which is this
project's actual contract with downstream consumers. A second protobuf subject
demonstrates nothing the first does not.

If Phase 4 sinks `alerts.bunching` to PostGIS through Connect, this gets
revisited. That is the point at which an enforced schema starts paying for
itself.
