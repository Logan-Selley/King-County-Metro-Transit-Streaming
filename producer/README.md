# producer: Phase 1

Polls the three GTFS-RT feeds, archives raw payloads, decodes, deduplicates,
and publishes to Kafka. Runs standalone (`python -m producer.run`); Airflow
never touches the stream.

## Modules

| File | What it is |
|---|---|
| `feeds.py` | Declarative manifest, intervals, formats, keys, topics |
| `errors.py` | Exception types, DLQ reason codes, bbox |
| `fetch.py` | Conditional GET with per-feed state and staleness tracking |
| `archive.py` | MinIO raw archive, bytes land before any decoding |
| `decode.py` | The three decoders and the frozen record types |
| `dedupe.py` | Value-level dedup, the suppression numbers |
| `publish.py` | Kafka production, DLQ routing, JSON serialization |
| `run.py` | Process wiring: scheduler, signals, counters, exit codes |

Each module docstring carries its own rationale and the measurement behind it;
the short version of the ones that matter:

- **Archive before decode.** A payload that fails to decode is the one you
  most want kept. It is the evidence for the bug you are about to fix, and
  the Phase 6 replay demo re-runs archived bytes through changed decoders.
- **Dedupe on the value, never the key alone.** Keyed dedupe of trip updates
  would collapse successive predictions for a stop, which is the exact data
  the prediction-accuracy analysis consumes (ADR 0004).
- **A malformed poll must not kill a 24-hour run.** Decode failures and
  per-record rejects route to the DLQ with a reason from `errors.DlqReason`,
  get counted, and the tick continues.

## Running it

```bash
python -m producer.run --list                 # feed manifest
python -m producer.run --once                 # one tick per feed
python -m producer.run --dry-run --once       # fetch + decode, publish nothing
python -m producer.run --duration 24h         # a collection run
```

`--dry-run` exercises the whole path against the live feed without touching
Kafka. `--no-archive` skips the MinIO write; fine while iterating on a
decoder, never for a collection run.

## Verifying a collection run

While it runs, topic offsets should climb and MinIO should fill:

```bash
docker compose exec -T redpanda rpk topic describe raw.vehicle_positions -p
```

Expected orders of magnitude at the measured evening density
(`docs/findings.md` §7):

| | per hour |
|---|---:|
| positions published | ~30-100k |
| trip updates published (post-dedup) | ~1M |
| archive objects | ~540 |
| archive bytes | ~110 MB |

If trip-update suppression is far below ~70%, fingerprints are not comparing
equal, usually a float or a naive/aware datetime mismatch.

## Design decisions already settled

Don't re-litigate these; they're measured and documented.

- **Partition keys**, `vehicle_id` / `trip_id` / `alert_id`
  ([ADR 0002](../docs/decisions/0002-partition-key.md))
- **Wire format per feed**, protobuf, protobuf, enhanced JSON
  ([ADR 0003](../docs/decisions/0003-feed-format-per-topic.md))
- **At-least-once + value dedup**, not exactly-once
  ([ADR 0004](../docs/decisions/0004-delivery-semantics-and-dedup.md))
- **Poll intervals**, half the measured publish period
  ([findings §2](../docs/findings.md))
- **JSON serialization for now**, Phase 2 swaps in protobuf + Schema
  Registry, and doing that against a topic with history *is* the schema
  evolution exercise

## Things worth getting right the first time

**Timezone-aware datetimes everywhere.** Naive datetimes land in Postgres as
local time and shift silently.

**Empty string is not NULL.** Protobuf returns `""` for unset strings.
`""` in a nullable column breaks `is null` predicates.
