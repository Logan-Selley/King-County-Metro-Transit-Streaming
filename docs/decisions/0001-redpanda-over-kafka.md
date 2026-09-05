# ADR 0001 — Redpanda instead of Apache Kafka for local development

**Status:** Accepted (Phase 1)
**Date:** 2026-09-04

## Context

The project needs a Kafka-API broker that runs on one developer machine
alongside an existing PostGIS stack, on a host whose root filesystem is at 96%
(15 GB free) and which is expected to accumulate event data continuously for
24-hour collection runs.

Apache Kafka in Docker Compose means either a ZooKeeper container or a KRaft
controller configuration, plus the broker, plus a separate Confluent Schema
Registry container. That is three to four containers and roughly 2 GB of image
layers before a single message is produced.

## Decision

Use Redpanda (`v25.2.1`) in `--mode=dev-container`, single node.

Redpanda's built-in Schema Registry is used rather than a separate Confluent
Schema Registry container.

## Consequences

**What is gained.** One container instead of three or four. No ZooKeeper and
no KRaft controller to configure or babysit. The Schema Registry disappears as
a separate deployment concern while remaining a first-class part of the
project — it is the same API on port 8081 of the same binary, and the ordinary
`confluent-kafka` client talks to it unchanged.

**What is preserved.** The Kafka wire protocol, so every client library,
`rpk`'s Kafka-compatible surface, consumer groups, partitions, offsets,
retention, and log compaction behave as they do on Kafka. Nothing learned here
is Redpanda-specific. This is tested rather than asserted: Kafka Connect in
this stack is `confluentinc/cp-kafka-connect`, a Confluent image pointed at a
non-Confluent broker.

**What is given up.** `--mode=dev-container` disables fsync-per-write and
relaxes memory checks. It trades the durability guarantee for not needing a
tuned host, which is correct for a laptop and wrong for anything real. The
mode is named explicitly in `docker-compose.yml` so the trade is visible
rather than inherited from a quickstart.

Single node also means replication factor 1 everywhere, so this stack cannot
demonstrate ISR behaviour, leader election, or partition reassignment. Those
are genuinely out of reach and should not be claimed.

**Why not just use Kafka anyway, for the résumé line?** Because the résumé line
is identical either way — the API, the semantics, and the operational concepts
are the same — and the difference is entirely in how much of the development
budget goes to container orchestration versus to the partition-key, schema
evolution, and stateful-join work that the project actually exists to
demonstrate. An interviewer asking "why not Kafka" gets this answer, which is
a better answer than a shrug.

## Alternatives considered

- **Kafka + KRaft + Confluent Schema Registry.** Rejected on image size and
  configuration surface, given the disk constraint. Would be the right call if
  the project needed to demonstrate multi-broker operations.
- **Confluent Cloud / Redpanda Cloud.** Rejected: the proposal requires
  infrastructure runnable locally end to end, and a continuously-running
  ingest against a metered cloud broker is an open-ended cost.
- **Redpanda + separate Confluent Schema Registry.** Rejected as strictly more
  moving parts for no capability gain; the built-in registry is API-compatible.
