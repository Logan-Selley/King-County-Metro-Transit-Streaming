# producer — Phase 1

Polls the three GTFS-RT feeds, archives raw payloads, decodes, deduplicates,
and publishes to Kafka.

**Phase 1 exit criterion:** 24 hours of continuous uninterrupted collection
across all three feeds.

## What's given vs. what you write

| File | State | What it is |
|---|---|---|
| `feeds.py` | **complete** | Declarative manifest — intervals, formats, keys, topics |
| `errors.py` | **complete** | Exception types, DLQ reason codes, bbox |
| `archive.py` | **complete** | MinIO raw archive — the worked example; match its style |
| `fetch.py` | **stub** | `ConditionalFetcher.fetch()` |
| `decode.py` | **stub** | Three decoders (record types + dispatch given) |
| `dedupe.py` | **stub** | `LastValueCache.filter_new()` |
| `publish.py` | **stub** | `TopicPublisher.publish()` (config given) |
| `run.py` | **stub** | `process_feed()` (CLI, scheduler, signals given) |

Each stub carries its full contract in the docstring. `archive.py` is the
reference for how a finished module should read.

## Build order

Work through it in this order — each step is verifiable before the next.

### 1. `dedupe.LastValueCache.filter_new()`

Pure logic, no I/O, fastest feedback loop.

```bash
make contract K=dedupe
```

The one that matters is `test_changed_fingerprint_passes`. Dedupe on the
**value**, never the key alone — keyed dedupe would collapse successive
predictions for a stop, which is exactly the data the prediction-accuracy
analysis consumes.

### 2. `decode.decode_vehicle_positions()`

```bash
make contract K=VehiclePositions
```

The trap is `current_status`. Read it through the attribute accessor so the
proto2 declared default applies; a presence check maps 73% of the fleet to
NULL. `test_current_status_applies_the_declared_default` fails loudly if you
get this backwards.

### 3. `decode.decode_trip_updates()`

```bash
make contract K=TripUpdates
```

The trap here is shape: one entity is a *trip*, and each carries ~30
`stop_time_update`s. One record per entity is wrong by 30×.

### 4. `decode.decode_service_alerts()`

```bash
make contract K=ServiceAlerts
```

JSON, not protobuf (ADR 0003). Translated strings are nested under
`{"translation": [...]}`.

### 5. `fetch.ConditionalFetcher.fetch()`

```bash
make contract K=ConditionalFetcher
```

A small state machine. The subtle one is
`test_304_does_not_clear_the_stored_etag` — clear it and every poll returns
200, the conditional GET silently stops working, and nothing looks broken.

### 6. `publish.TopicPublisher.publish()`

No contract test (it needs a broker). Verify against the live stack in step 7.
Remember `self._producer.poll(0)` inside the loop, or delivery callbacks never
fire.

### 7. `run.process_feed()`

Wire it together, then:

```bash
make produce-dry
```

Fetches, archives and decodes against the live feed without touching Kafka.
When that looks right:

```bash
make produce D=5m
```

Then check it landed:

```bash
make check
```

## Verifying a real collection run

```bash
make produce D=24h
```

While it runs — topic offsets should climb, MinIO should fill:

```bash
docker compose exec -T redpanda rpk topic describe raw.vehicle_positions -p
```

Expected orders of magnitude at the measured evening density
(`docs/findings.md` §7):

| | per hour |
|---|---:|
| positions published | ~30–100k |
| trip updates published (post-dedup) | ~1M |
| archive objects | ~540 |
| archive bytes | ~110 MB |

If trip-update suppression is far below ~70%, fingerprints are not comparing
equal — usually a float or a naive/aware datetime mismatch.

## Design decisions already made for you

Don't re-litigate these while implementing; they're settled and documented.

- **Partition keys** — `vehicle_id` / `trip_id` / `alert_id`
  ([ADR 0002](../docs/decisions/0002-partition-key.md))
- **Wire format per feed** — protobuf, protobuf, enhanced JSON
  ([ADR 0003](../docs/decisions/0003-feed-format-per-topic.md))
- **At-least-once + value dedup** — not exactly-once
  ([ADR 0004](../docs/decisions/0004-delivery-semantics-and-dedup.md))
- **Poll intervals** — half the measured publish period
  ([findings §2](../docs/findings.md))
- **JSON serialization for now** — Phase 2 swaps in protobuf + Schema
  Registry, and doing that against a topic with history *is* the schema
  evolution exercise

## Things worth getting right the first time

**Archive before decode.** Not after. A payload that fails to decode is the
one you most want kept — it's the evidence for the bug you're about to fix.

**Timezone-aware datetimes everywhere.** Naive datetimes land in Postgres as
local time and shift silently.

**Empty string is not NULL.** Protobuf returns `""` for unset strings.
`""` in a nullable column breaks `is null` predicates.

**A malformed poll must not kill a 24-hour run.** Route to the DLQ with a
reason from `errors.DlqReason`, count it, continue.
