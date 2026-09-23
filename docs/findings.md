# Phase 0: Reconnaissance findings

*Measured 2026-09-04 04:43-05:10 UTC (2026-09-03 21:43-22:10 PDT).*
Reproduce with `make recon` and `make cadence M=11`; raw evidence is in
`data/recon/snapshot.json` and `data/recon/cadence.json`.

**Read the timing caveat first.** Everything below was measured on a Thursday
evening, roughly 21:45 local. 280 in-service vehicles is a *trough*, not a
peak, Metro's weekday peak fleet is several times that. Payload sizes, row
counts, and archive projections scale with it. The cadence, field-population,
and protocol findings do not, and those are the ones that drive design.

---

## 1. Volume, per poll

| Feed | Entities | Protobuf | Enhanced JSON | Inflation | Bytes/entity |
|---|---:|---:|---:|---:|---:|
| `vehiclepositions.pb` | 280 vehicles | 27,284 b | 194,803 b | 7.1× | 97 |
| `tripupdates.pb` | 612 trips / **18,623 stop predictions** | 628,560 b | 6,773,180 b | **10.8×** | 1,027 |
| `alerts.pb` | 53 alerts | 38,523 b | 171,797 b | 4.5× | 726 |

Static `google_transit.zip` is 10,966,815 b, last modified 2026-08-29,
five days before measurement, consistent with service-change-date publishing
rather than a rolling update.

**Trip updates dominate, and by more than the byte count suggests.** The unit
of the feed is not the trip, it is the *stop prediction*: 612 trips carry
18,623 stop-time updates, a median of 28 and a mean of 30.4 per trip (max 88).
Any sizing that reasons in entities underestimates this table by ~30×.

## 2. Refresh cadence

Measured by polling every 5 s for 11 minutes with `If-None-Match` and
recording when each ETag actually moved.

| Feed | Median | Range | Changes / polls | 304 rate at 5 s |
|---|---:|---:|---:|---:|
| vehicle positions | **20.0 s** | 5.0-20.0 | 35 / 132 | 72.7% |
| trip updates | **20.0 s** | 5.0-20.2 | 35 / 132 | 72.7% |
| service alerts | **60.0 s** | 9.6-60.4 | 11 / 132 | 90.9% |

Two things fall out of this that the proposal guessed at:

**Positions and trip updates turn over in lockstep.** Identical change counts
and identical timings, tick for tick, across the whole window. Metro publishes
both from one upstream job. That is worth knowing operationally: if one goes
stale and the other does not, the fault is downstream of their publisher, not
in the publisher. It also means a single poll loop can drive both without
separate scheduling.

**Conditional GET is not a micro-optimisation here, it is the staleness
signal.** At a 5 s poll, 73% of requests return 304 for a few hundred bytes.
The proposal's instinct was right and the numbers support the feed-health
mart: a stalled ETag is unambiguous, because the true period is a tight
20.0 s rather than a noisy 15-30 s.

The occasional 15 s reading is aliasing against the 5 s poll grid, not a real
15 s publish, the true period is marginally under 20 s, so a change
occasionally lands one bucket early.

### Poll intervals chosen

`.env.example` sets 10 s / 10 s / 30 s, i.e. **half the observed publish
period** for each feed. Polling *at* the period risks phase-locking just
behind the publisher and consistently seeing each version one full period
late; polling at half of it bounds detection latency to 10 s at a cost of
~50% conditional 304s, which are a few hundred bytes each. This is the whole
argument for conditional GET, so the design should actually spend it.

## 3. Payload format: the protobuf is not a strict subset, but it is close

The proposal expected basic protobuf, basic JSON, and enhanced JSON per feed.
**The basic JSON mirrors do not exist**, `vehiclepositions.json` and its
siblings return `403`. Only `*_enhanced.json` is served. The Phase 0 step
"diff basic vs enhanced JSON" is therefore not possible as written; the real
comparison is basic protobuf against enhanced JSON.

**Vehicle positions.** The enhanced JSON carries exactly one field the
protobuf cannot: `block_id`, populated on 100% of entities. Everything else in
the JSON is present in the protobuf. `block_id` identifies the vehicle's day
of work and is the natural key for linking consecutive trips run by the same
bus, genuinely useful, and the cost of having it is a 7.1× payload.

**Service alerts.** Here the enhanced JSON adds substantially more:
`alert_lifecycle`, `created_timestamp`, `last_modified_timestamp`, `severity`,
`short_header_text`, `service_effect_text`, `timeframe_text`,
`informed_entity.activities`, `informed_entity.facility_id`. The protobuf in
turn carries `cause_detail` and `effect_detail`, which the JSON drops. Neither
format is a superset.

`last_modified_timestamp` is the interesting one, because the alerts topic is
log-compacted and keyed by `alert_id`: an explicit modification timestamp is
exactly what you want to order compacted updates by, and it exists only in the
JSON.

**Trip updates.** No meaningful difference. Take the protobuf; a 10.8×
inflation for nothing is not a trade.

See `docs/decisions/0003-feed-format-per-topic.md`.

## 4. Field population: what is actually on the wire

Every field below the top level of `gtfs-realtime.proto` is optional, so the
spec says nothing about what Metro sets. Measured across 280 vehicles:

| Field | Populated | Consequence |
|---|---:|---|
| `position.latitude` / `longitude` | 100% | n/a |
| `trip.trip_id` / `route_id` / `direction_id` / `start_date` | 100% | join keys are safe |
| `vehicle.id` / `label` | 100% | partition key is safe |
| `occupancy_status` | 99.6% | **Open question 2 answered: yes, it is populated** |
| `current_stop_sequence`, `stop_id`, `timestamp` | 99.6% | n/a |
| `current_status` | 27.1% *set* | **see below, absence is meaningful** |
| `position.bearing` | **2.1%** | unusable; derive heading from consecutive positions |
| `position.speed` | **1.8%** | unusable; derive speed from consecutive positions |

### `occupancy_status` is populated (open question 2)

99.6%, with real variation (`EMPTY`, `MANY_SEATS_AVAILABLE`,
`FEW_SEATS_AVAILABLE`, …) and it is in the **basic protobuf**, not a
JSON-only extension. It was the main thing the enhanced-JSON question hinged
on, and it turns out not to require the enhanced JSON at all.

### `bearing` and `speed` are effectively absent

2.1% and 1.8%. They exist in the spec and on the wire, which makes them look
available right up until an enrichment silently produces nulls for 98% of
rows. Heading and speed must be **derived from consecutive positions for the
same vehicle**.

That is not a workaround, it is a reinforcement: deriving anything from
consecutive positions requires per-vehicle ordering, which is the entire
argument for `vehicle_id` as the partition key. The proposal justified that
key on the deviation calculation; this is a second, independent reason.

### `current_status`: absence means `IN_TRANSIT_TO`, not unknown

The most dangerous finding here, and the least visible.

`gtfs-realtime.proto` declares `current_status` with
`[default = IN_TRANSIT_TO]`. Metro omits the field for **204 of 280** vehicles
(73%). Under proto2 presence semantics an unset field is not on the wire at
all, so a decoder that checks presence (`HasField`, or walking `ListFields`)
and maps absence to `NULL` discards the status of nearly three quarters of the
fleet.

Read through the protobuf attribute accessor instead, which applies the
declared default. Verified: doing so yields `IN_TRANSIT_TO`: 204,
`STOPPED_AT`: 76, exactly matching what the enhanced JSON materialises, which
is the independent confirmation that the default is the correct reading.

`raw.vehicle_positions.current_status` is therefore `NOT NULL DEFAULT
'IN_TRANSIT_TO'`, and `tests/test_feed_semantics.py` asserts this so it cannot
regress quietly.

## 5. Trip updates: the join key is `trip_id`, not `vehicle_id`

| Field | Populated |
|---|---:|
| `trip.trip_id`, `route_id`, `direction_id`, `start_date` | 100% |
| `stop_time_update.stop_id` / `stop_sequence` | 100% (≈30 per trip) |
| `stop_time_update.arrival` / `departure` (+ `.time`, `.delay`) | ~99% |
| `trip_update.vehicle.id` | **45.8%** |
| `trip_update.timestamp` | **45.6%** |

45.8% looked like a problem for the flagship two-stream prediction-accuracy
join. It is not, it is a coincidence worth checking, and the check resolves
it:

- trip updates with a vehicle assigned: **280**
- entities in the vehicle positions feed: **280**
- `trip_id` overlap between the two feeds: **280 / 280 = 100%**
- `vehicle_id` overlap: **280 / 280 = 100%**

The 332 trip updates without a vehicle are **scheduled trips that have not
started yet**. `vehicle.id` and `timestamp` populate precisely when a trip is
under way. So:

1. **The join must key on `trip_id`.** Keying on `vehicle_id` would drop every
   prediction issued before its vehicle started, which is exactly the
   long-lead-time end of the prediction-error curve, the interesting half of
   proposal §6.6.
2. **This is a feature.** The feed hands you predictions well before the trip
   begins, which is what makes "how wrong is the estimate at 20 minutes out
   vs. 5" answerable at all.

## 6. Churn: ~70% of every trip-updates poll is unchanged

Because the feeds are `FULL_DATASET` snapshots (confirmed on all three), every
poll restates everything. Measured across three polls 45 s apart, comparing
`(trip_id, stop_id, stop_sequence) → (arrival.time, departure.time)`:

| | Predictions | Unchanged | Changed | New | Dropped |
|---|---:|---:|---:|---:|---:|
| poll 2 vs 1 | 17,914 | 12,042 (**67.2%**) | 5,657 | 215 | 215 |
| poll 3 vs 2 | 18,021 | 13,013 (**72.2%**) | 4,760 | 248 | 141 |

Roughly 70% of each poll is byte-identical to the last. Deduplicating on the
prediction *value*, not just on arrival order, removes about two thirds of
the write volume at no analytical cost, because an unchanged prediction is not
a new prediction.

The one thing this must not do is dedupe on `(trip_id, stop_id)` alone.
Successive *changed* predictions for one stop are the data that the
prediction-accuracy analysis consumes; collapsing them deletes the finding.
Hence the primary key on `raw.trip_updates` includes `ingested_at`.

## 7. Projected daily volume

At the measured evening density, and assuming the archive stores every
distinct published version:

| Feed | Versions/day | Raw archive/day | Naive rows/day | After value-dedup |
|---|---:|---:|---:|---:|
| vehicle positions | 4,320 | ~118 MB | ~1.2 M | ~0.8 M |
| trip updates | 4,320 | **~2.6 GB** | **~80 M** | ~24 M |
| service alerts | 1,440 | ~55 MB | 53 (compacted) | 53 |

**Scale for peak.** These are trough numbers; a weekday peak fleet several
times 280 scales positions and trip updates proportionally.

This confirms the proposal's first risk as the real one. 2.6 GB/day of raw
trip-updates archive is fine, `/mnt/F` has 2.8 TB free and a week is ~18 GB.
80 M rows/day into Postgres is not fine on this hardware. Mitigations in
priority order:

1. **Value-level dedup at the producer** (§6), removes ~70% for free, and is
   the right behaviour regardless of volume.
2. **Short retention on `raw.trip_updates`**, set to 3 days versus 7 for
   positions, already configured in `make topics`.
3. **Route sampling**, the proposal's stated fallback. Keep it in reserve;
   1 and 2 may make it unnecessary, and sampling costs analytical coverage.

The archive is deliberately *not* sampled either way. Per the Terms of Use,
access may be discontinued without notice, so the MinIO archive is the thing
that makes the project survive that, sampling it would be sampling the
insurance policy.

---

## Answers to the proposal's open questions

**Q2, carry enhanced-JSON extension fields, or stay within the spec?**
Answerable now, and the answer is per-feed rather than global. `occupancy_status`,
the field the question was really about, is in the basic protobuf and 99.6%
populated, so it costs nothing. Positions gain only `block_id` from the
enhanced JSON at a 7.1× payload; alerts gain a great deal, including
`last_modified_timestamp`, at 4.5× on a 53-entity feed where payload is
irrelevant. Trip updates gain nothing at 10.8×. See ADR 0003.

**Q1, Faust or Flink?** Untouched; correctly deferred to after Phase 2. One
data point in Flink's favour: §5 makes the prediction join key `trip_id` with
records arriving well before their vehicle exists, so the state has a long and
uneven lifetime, which is the kind of thing Flink's timers handle explicitly.

**Q3 (cloud spend), Q4 (monorepo), Q5 (multi-agency)**, no Phase 0 evidence
bears on these.

---

# 8. Phase 1: the 24-hour collection run

*2026-09-05 04:10 UTC to 2026-09-06 04:10 UTC. Single process,
`python -m producer.run --duration 24h`. Log: `logs/producer-20260904-2110.log`.*

Phase 1's exit criterion was 24 hours of continuous uninterrupted collection
across all three feeds. It was met, and the run corrected four numbers that
§1-§7 above had to project rather than measure.

## Throughput

| Feed | Polls | Changed | 304s | Decoded | Published | Suppressed |
|---|---:|---:|---:|---:|---:|---:|
| vehicle positions | 8,096 | 4,629 | 3,455 | 1,215,136 | 1,107,028 | 8.9% |
| trip updates | 8,097 | 4,625 | 3,435 | **83,499,895** | 15,156,319 | **81.9%** |
| service alerts | 2,847 | 1,470 | 1,351 | 86,672 | 141 | 99.8% |
| DLQ (out of bounds) | - | - | - | - | 18,941 | - |

16,282,429 messages total. Raw archive **3.0 GiB across 10,741 objects**, trip
updates 2.6 GiB, alerts 274 MiB, positions 115 MiB. Redpanda holds 1.3 GB on
disk for the whole day, which is snappy doing its job.

## Continuity

4,629 archived position payloads spanning exactly 24.00 h. **Median gap 21 s,
maximum 65 s, zero gaps over 90 s.**

75 polls failed, all transient network faults, 36 DNS resolution failures and
39 connection-aborted, scattered across the day rather than one outage. No
decode failures, no delivery failures, no archive failures. Per-feed isolation
absorbed every one.

Worth stating why a failed poll costs so little here: the feeds are
`FULL_DATASET` snapshots, so a missed poll loses an intermediate state, never
an entity. The next successful poll restates everything current. This is the
same property that makes dedup necessary, paying off in the other direction.

## Corrections to §7's projections

**Peak fleet is 423 vehicles.** The proposal estimated "high hundreds to low
thousands"; §7 scaled the 280-vehicle trough by "several times". Actual peak
was 423 at 15:07, trough **0** at 03:10, service genuinely stops overnight.
Peak is only 1.5× the evening trough, not 3-5×.

**Trip updates do not scale with fleet size.** §7 projected ~91,000 stop
predictions per poll at peak. Actual peak was **~27,800**, the feed is
dominated by *scheduled* trips, so its size tracks the timetable rather than
the running fleet. This is why trip updates are 30× positions at the trough
but only ~11× at peak.

**Dedup beat the estimate.** §6 measured 67-72% churn suppression over 45 s
spacing and §7 assumed ~70%; the run sustained **81.9%** at 20 s spacing,
closer polling sees fewer changes. Naive volume was 83.5M rows/day against
§7's ~80M projection (good), post-dedup 15.2M against ~24M (pessimistic).

**Suppression held through the peak.** Worst 5-minute interval all day was
74.4%; the 06:00-09:00 ramp averaged 85.0%. The dedupe cache is sized per feed
(`FeedSpec.dedupe_maxsize`) precisely because this degrades as a cliff rather
than a slope, see `producer/dedupe.py`.

## A finding the DLQ produced for free

18,956 positions were rejected for falling outside the King County bounding
box, 1.54% of everything decoded. They are not spread across the fleet:

- **199 distinct vehicles** produced all of them
- the top 20% of those vehicles account for **92%** of the bad positions
- median offender: 11 bad positions
- worst: vehicle 7210 reported null island **2,172 times**

That is not sensor noise, it is a small population of chronically broken GPS
units. A vehicle reporting (0, 0) two thousand times in a day is a maintenance
ticket, not a data-quality nuisance. The bounding-box check was written to stop
transposed coordinates from corrupting spatial joins; it produced a fleet
health signal as a side effect, which is the better half of the argument for
routing rejects to a DLQ instead of dropping them.

---

---

# 9. A service change happened mid-project

*2026-09-14. Observed while scoping Phase 2, not simulated.*

The static feed this project was scoped against was replaced while the work
was in progress. That is the "static/realtime join staleness" problem the
proposal calls its most realistic operational issue, and it arrived on its
own schedule rather than being staged.

| | 2026-08-29 | FAL26-161.1 (2026-09-14) |
|---|---|---|
| ETag | `682c87c4c237dd1:0` | `1895a7c83f44dd1:0` |
| Size | 10,966,815 b | 10,663,540 b |
| Members | **12** | **18** |
| `trips` | 32,060 | 31,688 |
| `stop_times` | 1,107,713 | 1,101,970 |
| `shapes` | 172,617 | 167,088 |
| `routes` | 144 | 142 |

**The structure changed, not just the data.**

- **Added:** `feed_info.txt`, `networks.txt`, `route_networks.txt`, and five
  GTFS-Fares v2 files (`fare_leg_rules`, `fare_media`, `fare_products`,
  `fare_transfer_rules`, `rider_categories`).
- **Removed:** `block.txt` and `block_trip.txt`, the two non-standard Metro
  extensions.

## The unknown-trip rate measures staleness, not feed noise

This is the finding that changes how the DLQ should be read.

| Static feed | Live position `trip_id`s resolving |
|---|---:|
| 2026-08-29 (two weeks old) | 97.4% |
| FAL26-161.1 (same day) | **100.0%** |

The 2.6% miss was never a property of the feed. It was the *age of the static
load*, trips scheduled under a version newer than the one held. So
`UNKNOWN_TRIP_ID` has no acceptable baseline to tolerate. A healthy pipeline
sits near zero and climbs as a service change approaches, which makes that DLQ
rate the cheapest available alarm for "the static data needs refreshing".

Phase 2's scoping documents previously stated a ~2.6% baseline as a property
of the feed. That was wrong and has been corrected in `static/feed.py`,
`static/load.py`, and `consumers/enrichment/reference.py`.

## Two decisions this validated

**Sourcing `block_id` from `trips.txt` (ADR 0003 revision).** `block_trip.txt`
was deleted outright. Enrichment built on it would have broken on a Sunday;
`block_id` in `trips.txt` survived untouched.

**Naming required files rather than iterating the archive.** Six files
appeared and two vanished, and nothing about whether the loader works changed,
because the manifest declares what it needs and ignores the rest.

## `feed_info.txt`: carry it alongside the ETag

The new feed publishes the agency's own identity:

```
feed_version     FAL26-161.1
feed_start_date  20260914
feed_end_date    20270326
```

Both identifiers are now recorded on every static load, and both reach every
enriched record (`static_feed_version` and `gtfs_feed_version`), because they
answer different questions and fail in opposite directions:

| | Answers | Fails when |
|---|---|---|
| **ETag** | "are these different bytes?" | An identical schedule is rebuilt, same data, new ETag |
| **`feed_version`** | "which schedule is this?" | A corrected feed republishes under the same label |
| **start/end dates** | "should an in-flight trip use the old version or the new one?" | Absent, it is optional in the spec |

The ETag stays the unique key, because it is the only one guaranteed to exist
and to differ per publish. `feed_info.txt` is optional in GTFS and Metro
published none before this service change, so a loader that required it would
have failed against every earlier feed.

---

---

# 10. Phase 2: enrichment, and what the schema gate does not check

*2026-09-16 to 2026-09-20, against FAL26-161.1.*

## The enrichment works, measured live

1,204 records through `enrich_v2` against a same-day static load:

| | |
|---|---:|
| trip join rate | **100.0%** |
| `shape_dist_traveled` populated | 100% |
| `schedule_deviation_seconds` populated | 100% |
| `neighborhood_name` populated | 84.7% *(projected 83.4%)* |
| DLQ / errors | 0 / 0 |

**Median schedule deviation +105 s**, the median Metro bus is 1.8 minutes
behind schedule. p10 −270 s (4.5 min early), p90 +410 s (6.8 min late), range
−2,069 s to +1,905 s. Zero records showed a ~7 h (timezone) or ~24 h
(service-date) anchor error.

A later v2 run over 33,474 records held: deviations 3 s to 196 s,
`implausible=0`.

## The service day starts at 06:00, and 4.5% of trips cross midnight

From `stop_times` under FAL26-161.1:

```
offsets present          03:52:00 .. 29:30:00     (a 30-hour span)
hours 00:00-02:59        ZERO rows
trips with offset >=24h  1,433 of 31,688 = 4.5%
```

Metro publishes no offsets before 03:00, so **all pre-06:00 service is the
previous service date's tail**. At 04:30 clock time two populations run
simultaneously: new-day trips at offset 04:30:00 and previous-day trips at
28:30:00, same instant, same clock, different service dates.

Two consequences for schedule deviation, both load-bearing:

1. **Never fold an offset modulo 24 h.** Normalising 29:30 to 05:30 moves a
   trip a full day.
2. **Anchor on the trip's `start_date`, not the observation's calendar date.**
   The positions feed supplies it on 100% of records, and inferring it would
   be ambiguous in exactly the 04:00-05:59 window where it matters.

The origin is **noon minus 12 h in `America/Los_Angeles`**, per the GTFS spec's
own phrasing, which exists so the two DST days come out at 23 and 25 hours.
An earlier helper returned UTC midnight, which was 7 hours wrong every day.

## Units: `shape_dist_traveled` is in feet, except once

`ST_Length(geom::geography) / max_dist` = **0.3048** across 423 of 424 shapes: the foot-to-metre constant falling out of the data, which also proves the
linestrings assembled in the right order. Shape `63424` publishes **metres**.

Harmless only because of how the comparison is framed: `stop_times` agrees
with its own shape's scale on all 31,688 trips (ratio 0.998-1.000), and the
detector normalises per shape. Any mart that ranks or aggregates distances
**across** routes must normalise first, or that one route is 3.3× wrong.

## What the Schema Registry enforces, and what it cannot

The substantive finding, measured against the live registry at BACKWARD.

Redpanda implements **protobuf's own wire-compatibility table**. It answers
*"can an old reader parse these bytes without error?"*, it does not and
cannot answer *"do the values still mean the same thing?"*

| Change | Verdict | Damage |
|---|---|---|
| `string → int32`, `int32 → sint32`, `double → float` | INCOMPATIBLE | n/a |
| lat/lon **number swap** | COMPATIBLE | transposes every coordinate |
| `int32 → bool` | COMPATIBLE | every nonzero deviation → `true` |
| `int32 → uint32` | COMPATIBLE | early buses → ~4.29e9 |
| **`optional` dropped** | COMPATIBLE | absence collapses into zero |
| field removed / renumbered | COMPATIBLE | silent data loss |
| `int32 → int64`, `string → bytes` | COMPATIBLE | benign |

`optional` is the sharpest case. On the wire, an unset `optional int32` is
**0 bytes** and one explicitly set to 0 is **3 bytes**, the zero *is*
written. Dropping `optional` does not change the producer's output at all; it
changes what a reader can recover, collapsing "could not compute" into
"exactly on time". 18 fields in this schema are declared `optional`, six of
them numeric where zero is legal and the collapse is therefore silent.

ADR 0005 originally claimed removal and renumbering "fail at registration".
They do not, that was an Avro intuition (Avro resolves by field *name*;
protobuf resolves by *number* and skips unknowns). The ADR now carries the
correction and an amendment on the Decision paragraph itself.

**Two complements were built, because the registry structurally cannot
provide them:**

- `MAX_PLAUSIBLE_DEVIATION_S` (3 h), a value-domain bound catching the
  `uint32` wrap (~4.29e9), a wrong service-date anchor (~86,400 s), and a UTC
  origin (~25,200 s) with one predicate. Same role as `KING_COUNTY_BBOX`, one
  layer up.
- `consumers/enrichment/semantic_diff.py`, a three-way classifier
  (wire-incompatible / semantic-hazard / benign) wired into
  `register --check`. Three-way rather than two because a gate that blocks
  safe widening is a gate people bypass.

Worth sitting with: the lat/lon swap is caught by the project's **own bbox
DLQ check**, not by the schema gate. The compatibility check does not protect
the one field pair where a swap is both plausible and catastrophic.

---

## Phase 3 scoping: two things that would have made the detector lie

Both found by measuring before writing the detector rather than after, which
is the only reason they are findings rather than a debugging story. Full
reasoning in [ADR 0007](decisions/0007-bunching-key-and-gates.md).

**A (route, direction) does not mean one shape.** `config.py` asserted it did,
and used that to justify comparing `shape_dist_traveled` between any two
vehicles sharing a key:

```
(route, direction) pairs with 1 shape : 179
                             2 shapes :  73
                             3 shapes :  16
                             4 shapes :   9
                             5 shapes :   3
trips on a multi-shape pair: 14,054 of 31,688 = 44.4%
```

Distance is measured from each geometry's own origin, and **92 shape pairs
start more than 500 m apart**, affecting 38 route-directions. Equal distances
on two such shapes mean two buses miles apart. The other 105 shape pairs start
within 50 m and are genuinely comparable, so keying on `shape_id` would have
destroyed real coverage to fix this: A Line direction 1 runs two shapes 44 m
apart in total length over 18 km, and a shape key stops comparing 101 of its
310 trips against the other 209.

**12.6% of enriched records report `shape_dist_traveled` of exactly 0.0.**
That looks like a loader bug. It is not. 95.6% of them are `STOPPED_AT`, and
sampling 40 against PostGIS puts every one **1.7-86.5 m from its shape's start
point** (mean 27.8 m), so the projection is correct: these are buses waiting
at the first stop of a trip that has not started. Two of them have a gap of
zero and would alert on a layover.

The distribution near the origin is **bimodal**, which is what makes a
threshold safe:

```
== 0 ft      12.6%
< 100 ft      0.9%
< 1,000 ft    2.6%
>= 1,000 ft  83.9%
```

Under 1% of real positions sit between "parked" and "under way", so a 100 ft
floor removes the entire layover population at almost no cost.

## The feed publishes stale-timestamp bursts, and event time is what absorbs them

Found while running the finished detector over the live topic. Of 46,286
enriched records, **10 carry a `position_timestamp` 17 days in the past**:

```
part  offset   position_timestamp         vehicle  route  feed_version
   1   11817   2026-09-04 04:47:05+00:00  4322     10     FAL26-161.1
   1   11818   2026-09-04 04:47:11+00:00  6956     101    FAL26-161.1
   ...                                                    (10 total)
   2    9725   2026-09-04 04:47:04+00:00  7133     106    FAL26-161.1
```

Three things make this more than a curiosity:

**They are not old records.** Every one carries `FAL26-161.1`, the feed
version published on 2026-09-14, so they were enriched ten days *after* the
date their own GPS timestamp claims. The timestamp is wrong, not stale.

**They arrive mid-log, not at the start.** Offsets 11817-11821 and 9721-9725
sit about 70% of the way through the topic, surrounded by current records.
So this is one upstream snapshot that carried a block of vehicles with bad
timestamps, all within 9 seconds of each other, across six different routes.

**This is exactly the case event time exists for.** With
`for_bounded_out_of_orderness(120s)`, the watermark has long passed and
Flink drops all ten as late data, which is correct. Windowing on arrival time
would have placed them alongside current positions, where three route-106
vehicles and two route-101 vehicles could have been paired against live buses
and produced confident false bunching alerts.

`job.py`'s docstring argued for event time from Phase 1's 65 s maximum
inter-payload gap. The real justification turned out to be larger than that
by four orders of magnitude.

## The detector, run against the topic before Flink touches it

`detect.py` over all 46,286 records, tumbling 60 s windows on
`position_timestamp`, same key and same functions the Flink job will use:

```
records 46,286   undecodable 0   unparseable 0
windows  8,554   alerts     67
gap_ft:  min 0   median 324   max 981
```

Alert rate normalised by window count, which is comparable because every
route has the same 116 windows over the same span:

| route | alerts | rate per 100 windows |
|---|---|---|
| **E Line** | 26 | **22.4** |
| **7** | 24 | **20.7** |
| A Line | 6 | 5.2 |
| G Line | 4 | 6.9 |
| 255 | 3 | 2.6 |
| 72, 40, 65 | 1-2 | under 2 |

The two routes on top are the two that should be. The E Line is Metro's
highest-frequency RapidRide corridor and Route 7 is one of its busiest
non-RapidRide lines; both run headways short enough for bunching to be
possible at all, which is the premise `HIGH_FREQUENCY_ROUTES` encodes. A
detector that ranked a 30-minute suburban route first would be measuring
something else.

The deviations make individual alerts readable: `dev=737/141s` is a bus 12
minutes late closing on one 2 minutes late, which is the textbook mechanism
rather than two buses simply near each other.

**67 alerts come from only 15 distinct vehicle pairs**, one of them 24 times.
That is `min_consecutive_windows` and `cooldown_s` measuring their own
necessity before either is implemented: without them the alert topic becomes
a position feed with extra steps, which is what `config.py` predicted.

## PyFlink drops record timestamps across a Python operator

The textbook placement for a watermark strategy is on the source, where Flink
tracks a watermark per Kafka partition and emits the minimum. Moving it there
made the job read all 46,286 records, drop nothing, and emit nothing.

A probe printing each record's Flink timestamp next to its own
`position_timestamp` shows why:

```
rec_ts=1789960793771  wm=1789960673416  pos=1789959734
rec_ts=1789960793771  wm=1789960673416  pos=1789959748
rec_ts=1789960793771  wm=1789960673416  pos=1789959741
```

`pos` varies per record. `rec_ts`, which is what the window actually uses, is
identical for all of them. **A Python map operator does not carry its input
record's timestamp to its output**, so a timestamp assigned before
`.map(decode)` is gone by the time the window sees it. Every record lands in
one window whose end sits just past the final watermark, and it never fires.

The failure has no error and no dropped records. `numLateRecordsDropped` reads
**0**, which looks like the healthiest possible number and actually means no
record has a real timestamp at all.

So the assigner has to sit after the last Python operator, which gives up
per-split watermarking. **The first version of this section then guessed wrong
about what that costs**, claiming the 23.4% seen on replay was a catch-up
artifact that would vanish once the job was tailing. Measured over an evening
of steady state it was *worse*: **193,364 of 649,949 records dropped, 29.75%**.

The disorder is still not in the data:

```
within a partition       max lateness 111s, 0 records over the 120s bound
3 partitions interleaved max lateness 342s, 33.1% over the bound
```

Three partitions do not advance in lockstep just because the job has caught
up, because the enrichment consumer fills them in bursts.

### The fix is parallelism, not a bigger bound

Raising `allowed_lateness_s` from 120s to 360s was the obvious response, and
the interleaved distribution supports it (p99 297s, max 342s). It did not
help on a replay, where cross-partition skew has no bound at all: the rate
went to **32.64%**.

The real fix is to stop interleaving. Setting job parallelism to **3, the
partition count**, gives each subtask exactly one partition, so its single
watermark *is* a per-partition watermark. `map` and `filter` preserve
partitioning, so the assigner downstream of them still sees one partition per
subtask. That recovers the property the source placement would have given.

Verified against an offline run of the same detector over the same 1,560,949
records, which uses no watermark and therefore drops nothing:

```
                    offline (truth)   Flink p=2   Flink p=3
alerts                    823             340         824
capture                   100%             41%        100%
```

Hour by hour and route by route, parallelism 3 reproduces the offline result
exactly. Parallelism 2 had been losing 59% of alerts, uniformly across the
day, with no error and a healthy-looking dashboard.

Two lessons worth separating. A watermark bound has to be measured where the
watermark is computed, not where the data is produced. And when a framework
will not give you per-partition watermarks, one partition per subtask is the
same guarantee bought a different way.

## Metaspace, not memory, is what a repeatedly-submitted Flink cluster runs out of

After roughly a dozen job submissions in one session the TaskManager died:

```
java.lang.OutOfMemoryError: Metaspace ... The task executor has to be
shutdown...
```

Each submission loads the job's classes into a fresh `ChildFirstClassLoader`,
and PyFlink drags Beam in with it, so metaspace grows per submit rather than
per record. The default 256 MB is sized for a cluster that runs one job.

It kills the TaskManager rather than the job, so the symptom is a job that was
healthy a moment ago sitting in `RESTARTING` with no exception of its own.
`restart: unless-stopped` brings the container back and the job recovers from
its last checkpoint, which hides it further: the only trace is in the
TaskManager log. Raised to 512 MB in `docker-compose.yml`.

## Phase 3 result: a day of bunching, and the route that was lying

19 hours, 1,560,949 enriched records, 243,102 windows. After the cooldown,
**607 alerts**.

```
06:00 ######### 9
07:00 ######################### 25
08:00 ########################### 27
09:00 ########################################## 41
10:00 ############################# 29
11:00 ########################## 26
12:00 ########################### 27
13:00 ################################## 34
14:00 ############################ 28
15:00 ####################################### 39
16:00 ####################################################### 73
17:00 ####################################################### 96   <- peak
18:00 ####################################################### 75
19:00 ############################################ 44
20:00 ##################### 21
```

**16:00-18:00 carries 244 alerts, 40% of the day in 3 of 19 hours.** The
morning peak (07:00-09:00) totals 93, a little over a third of the evening's.
Delay accumulates across a service day rather than resetting each morning,
and the detector reproduces that without being told about it.

| route | alerts | |
|---|---:|---|
| G Line | 104 | RapidRide, Madison |
| E Line | 100 | RapidRide, Aurora |
| 7 | 90 | trunk, Rainier |
| 36 | 37 | trunk, Beacon Hill |
| D Line | 26 | RapidRide |
| H Line | 25 | RapidRide |
| 40 | 25 | trunk |

Six of the top seven are RapidRide or high-frequency trunk routes, which is
the credibility check Phase 3 set for itself.

### Route 255 ranked 4th, and it was an artifact

The first run put **255 at 79 alerts**, ahead of every RapidRide line except
G and E. A suburban express across SR-520 out-ranking most of RapidRide is
not a finding, it is a symptom, and chasing it produced the better gate.

```
255 gap_ft:   min 0   median 0   max 802
              49 of 79 alerts under 50 ft
              46 of 79 with BOTH vehicles running early
              72 of 79 in direction 1
```

A median gap of zero is vehicles projecting to the same point, not vehicles
following each other. 87% of them sat at `shape_dist` ~1,600 ft on a 74,000 ft
shape. Resolving that against PostGIS put them at **NE 128th St & 116th Ave
NE**, which is stop sequence 2-3 of the route, immediately after **Totem Lake
Transit Center**: coaches staging before departure.

`MIN_PROGRESS_FT = 100` could never have caught it. Distance along the shape
identifies "at the terminal" only when the terminal sits at the shape's
origin, and Totem Lake is 1,600 ft along.

`current_stop_sequence` answers the question directly and route-agnostically,
and the two populations are distinct rather than a continuum:

```
stop_seq 1-3   n=1,201   34.3% of pairs under 25 ft apart
stop_seq 4+    n=3,906    3.8%
```

Gating at sequence 4: **255 falls 79 -> 10**, while G Line loses 9%, E Line
3%, and 36 5%. The cost is real (route 7 loses 17%, and genuine bunching near
a terminal is discarded) and it is accepted because buses leave a terminal on
a dispatch schedule rather than a headway. Bunching that matters to a rider
develops along the route.

**The peak got sharper, which is the check that it removed noise.** The PM
peak's share of the day rose from 35% to 40% after the gate. Terminal staging
happens all day; real bunching concentrates at peak, so a filter that
strengthens the peak signal is removing the right records.

### G Line was checked and is clean

It tops the list, and it has two properties that looked like they might
explain that away. Both were measured and neither does.

Its GTFS carries **only `direction_id=1`** for all 420 trips, so the detector
key does not separate its two directions. It is safe anyway, because its
single shape is a **loop**: 7,365 m with start and end 54 m apart, so an
eastbound bus sits at 0-12,000 ft and a westbound one at 12,000-24,000 ft and
they never pair. The key works here for a different reason than the one
`config.py` gives, which is worth knowing before someone "fixes" it.

The second worry was its turnaround. Alerts are spread along the whole loop
with no spike at the 12,052 ft midpoint, so vehicles reversing at the west
end are not pairing with each other either.

## The two topics disagree about `start_date`, and it is nobody's bug

Found while scaffolding the 3F join. Measured on the live topics:

```
raw.trip_updates            start_date = "2026-09-22"   ISO
enriched.vehicle_positions  start_date = "20260922"     GTFS
```

Neither side is wrong, and the divergence is deliberate on the enriched side.
The producer decodes the realtime field to a `date` and serialises it with
`.isoformat()`, so the raw topic carries dashes. `consumers/enrichment/
schema.py` then strips them on the way into protobuf, with a docstring saying
"as the agency publishes it" -- the realtime wire form really is GTFS, and the
dashes were an artifact of the decode.

It only becomes a bug where the two streams meet. A join key built from the
raw values compares `2026-09-22:803357351:76731` against
`20260922:803357351:76731`, which never match. Every prediction would sit in
state until its TTL expired, the sink would receive nothing, and the job would
report a healthy pipeline: no errors, no late records, no backpressure. The
same silent shape as Phase 2's `schedule_deviation` returning None for every
record.

**The first version of the 3F spec did not catch it**, because both fixtures
used the ISO form and the key test compared two identical strings. That is the
Phase 1 `dedupe_key` mistake repeated: fixtures self-consistent with each
other rather than faithful to the wire. The fixtures now carry the two
formats they actually have, which turns that test into a regression test.

## Repeated predictions are real revisions, not restatements

The trip-updates feed restates every stop of every active trip on every poll,
so a single (start_date, trip_id, stop_id) accumulates ~18 predictions. If
those were identical, the error curve would be weighted by poll frequency
rather than by distinct estimates, and its shape would be an artifact.

Measured over 350,000 predictions covering 15,989 keys:

```
records on keys seen more than once     275,405
DISTINCT arrival_time values among them 253,515  (92.1%)
distinct estimates per key              p50 15, p90 31, max 46
keys whose estimate never changed       184  (1.2%)
```

Metro revises the estimate on almost every poll, so each buffered prediction
is a genuine data point. The 7.9% that repeat inflate the curve slightly
rather than distorting it.

## Phase 3F result: Metro's arrival estimates run pessimistic

`consumers/prediction/accuracy.py` run over both topics offline: every
STOPPED_AT on `enriched.vehicle_positions` (200,559 distinct arrivals) against
the newest ~3M trip-update predictions. 27.5% of predictions found their
arrival; the rest were for stops the slice had not reached yet. **608,140
accuracy records.**

```
lead     n        median |err|  p90 |err|  mean err
0-2m     20,149        45s         86s        +17s
2-5m     84,093        80s        152s        +59s
5-10m   123,980        96s        205s        +71s
10-15m  100,900       109s        250s        +75s
15-20m   81,071       123s        286s        +79s
20-30m  109,973       139s        334s        +81s
30-45m   71,206       167s        390s       +111s
45-60m   16,633       196s        424s       +151s
```

Median, p90 and mean all rise across every forward bucket, which is the exit
criterion `config.py` set as a shape rather than a number.

**The sign of the error is the finding.** Error is predicted minus actual, so
positive means the bus arrived *earlier* than the estimate, and it is positive
in every bucket, growing from 17 s at under two minutes to 2.5 minutes at an
hour out. A rider who trusts the sign is the one who misses the bus.

The 0-2m bucket bounds how much of that could be measurement. If
STOPPED_AT fired early (a geofence around the stop, say), it would show as a
constant offset at every lead, and at 0-2m the whole bias is 17 s. The growth
above that is the prediction model.

### The first run bent the curve, and the cause was a forecast that wasn't one

The first pass put 0-2m at an 85 s median, worse than 2-5m. Two measurements
found why:

```
trip updates with arrival_time == departure_time   97.0%
joined pairs issued AFTER the bus had arrived      13.4%
    ...of the 0-2m bucket                          74.6%
    ...of "past"                                   99.5%
```

Metro publishes one time per stop and keeps restating it while the bus sits
there. A prediction issued after the arrival is not a forecast, and
`accuracy_record` now drops it. That took 0-2m from 85 s to 45 s and "past"
from 26,042 records to 135.

It also retired a wrong rationale from the spec. `keep_negative_lead` was
justified as "the sign said 3 minutes ago", a late bus the estimate lagged
behind. 99.5% of the negative-lead records were not that; they were a bus that
had already come and gone. The 135 left are the case the comment described.

**Dwell was the first suspect and is not the explanation.** Median time
between first and last STOPPED_AT at a stop is 0 s (p90 30 s), and scoring
against departure instead of arrival moves the mean by about 10 s.

**One known contamination, left in.** 35 of 31,688 trips visit the same
`stop_id` twice (routes 914 and Easy Loop), so their second visit joins
against the first. Keying on stop sequence would close it; at 0.1% of trips it
was not worth a key change before the curve existed.

## Corrections to the proposal

1. Basic JSON mirrors do not exist (403). Only enhanced JSON. §2's feed table
   should say protobuf + enhanced JSON.
2. Refresh cadence is 20.0 s for positions and trip updates and 60.0 s for
   alerts, tighter than the estimated 15-30 s, and alerts move faster than
   the "irregular, very low cadence" characterisation.
3. Trip updates are ~30× larger than positions by row count (18,623 vs 280),
   not merely "much higher volume".
4. `bearing` and `speed` are unusable at ~2% population, worth stating,
   because both appear in every GTFS-RT tutorial.
