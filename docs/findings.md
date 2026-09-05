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
