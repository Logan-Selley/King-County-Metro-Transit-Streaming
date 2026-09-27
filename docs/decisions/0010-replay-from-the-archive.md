# ADR 0010: Replay from the archive into an isolated namespace, and prove fidelity first

**Status:** Accepted. The design is settled and the code has landed: 6A (the
namespace), 6B (`ArchiveFetcher`, `replay_spec`, `run_replay`), 6C (the
`REPLAY_*` settings for both jobs) and 6D (`replay/compare.py`) are implemented
and checked by the 50 tests in `tests/test_replay_contract.py`. 6E ran on
2026-09-27 over 09-24 Pacific; the fidelity and experiment results are in
docs/findings.md section 13.
**Date:** 2026-09-25

## Context

The proposal's Phase 6: "reprocess archived MinIO payloads through a modified
consumer to prove the replay story", with before/after results from the same
source data. The producer was built for it: every payload is archived before it
is decoded, and `process_feed` reads its input from a swappable fetcher.

What the archive actually holds, measured by listing it:

| period | days (archived hours) | what it is |
|---|---|---|
| A | 09-05 (21), 09-06 (5) | the Phase 1 24-hour run |
| B | 09-21 (22), 09-22 (21), 09-23 (14), 09-24 (24), 09-25 onward | Phase 3 onward; 09-23 short because of the crashes |

Nothing was archived between 09-06 and 09-21: the producer was not running.
Period A predates the 09-14 service change, and the only static version in the
warehouse is FAL26-161.1 (from 09-14); no copy of the 08-29 zip exists on this
machine. Period B overlaps what the live pipeline wrote, which makes a fidelity
check possible there and only there.

## Decision

**The experiment is the bunching detector's terminal gate** (MIN_STOP_SEQUENCE),
replayed over period B: `baseline` with the live gate, `variant` with the gate
off. Phase 3 measured the effect offline (route 255: 79 alerts to 10); the replay
reproduces it through the real job, from archived bytes.

**Fidelity before the experiment.** The baseline replay must reproduce what the
live pipeline wrote from the same input, both enriched positions and alerts,
keyed as the sinks key them. A difference the variant shows only means
something once the baseline matches.

**A fixed replay namespace**, declared in Terraform (`terraform/core/replay.tf`):
five topics, each partitioned like the live topic it stands in for, reset
between runs by trimming rather than recreated. Every writer refuses any topic
outside `replay.` before it produces.

**The live code, not copies.** The re-ingest runs `process_feed` with an
archive-backed fetcher. The replayed enrichment and detector are the live
modules with their topics, groups and (for the variant) gate supplied by
settings. Only where things are read and written changes.

**The replayed detector runs in PyFlink local mode**, in its own container,
with a bounded source. The live cluster's 9 slots are all taken by the two live
jobs, and a bounded job ends when its input does, with the final watermark
firing the last windows. Prototyped: 5,662 records through a Python map in
8.2 s, no consumer group left behind.

**Read-only archive access.** A new MinIO user, `archive_reader`, may list and
get under `raw/` and nothing else. `archive_writer` cannot read, and the replay
must not be able to write into the history it reads.

**Replayed writers keep the live schema subjects.** Their records have exactly
the live schema, so framing them with the live id is correct, and the
registry does not gain subjects nobody owns (auto-registration stays off,
ADR 0005).

## What cannot replay faithfully, and is left out on purpose

- **The prediction-accuracy join.** Its state TTL is processing time
  (`consumers/prediction/job.py`), so days of data replayed in minutes expire
  nothing and match nothing like the live run. Bunching replays faithfully for
  the opposite reason: it is event-time throughout, down to the stale gate
  (`max_position_age_s` is measured against the window end).
- **`enriched_at`**, the one wall-clock field on the replayed path. The
  comparison ignores it, and says so.
- **Period A**, as a fidelity check: there is no live output to compare with,
  and its trips ran on a timetable the warehouse no longer has.

## Traps found while designing, each now in the contract

| trap | what would have happened |
|---|---|
| `process_feed` hardcodes `f"dlq.{spec.name}"` | the replay's rejects land in the LIVE DLQ, counted by `transit_health` |
| the archive key stores the ETag without quotes; live rows carry `"..."` | every replayed row differs from live in `feed_etag` with correct logic |
| serializers derive the subject from the topic | `replay.*-value` subjects do not exist; the writers fail at first record |
| the dedupe cache starts empty | the first payload republishes pre-window positions; compare restricts to event time in the window |
| the live alerts topic holds duplicates (1,037 records, 611 keys = the warehouse's 611) | naive counting would call them differences |
| the enrichment consumer starts at `latest` and never exits | a replay run neither reads its input nor finishes |
| the fetcher's queues built lazily, on first `fetch` | `run_replay` asks `exhausted` before its first fetch, so every drain loop would exit having read nothing, and report a successful empty replay |

## Consequences

- A replay is `make replay-reset`, then ingest, enrich, detect (baseline),
  compare, detect (variant), compare. Each writes only into the namespace.
- The namespace's topics are under the same drift check as everything else.
- The experiment's credibility rests on the fidelity numbers, which the
  write-up reports first.
