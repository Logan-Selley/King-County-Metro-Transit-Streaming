# ADR 0003: Wire format chosen per feed, not globally

**Status:** Accepted (Phase 1)
**Date:** 2026-09-04
**Supersedes:** the proposal's open question 2

## Context

The proposal assumed each feed was available as basic protobuf, basic JSON,
and "enhanced" JSON, and asked whether to carry agency extension fields from
the enhanced variant or stay strictly within the GTFS-RT spec. It flagged
`occupancy_status`, block IDs, and vehicle labels as likely extensions.

Phase 0 measurement (`docs/findings.md` §3-4) changed the premises:

1. **The basic JSON mirrors do not exist.** `vehiclepositions.json` and its
   siblings return `403`. Only `*_enhanced.json` is served. The comparison is
   protobuf vs. enhanced JSON.
2. **`occupancy_status` is in the basic protobuf**, populated on 99.6% of
   entities. The field the question was really about costs nothing.
3. **Neither format is a superset of the other**, and the gap differs sharply
   per feed.

## Decision

Per feed, rather than one global answer:

| Feed | Format | Reason |
|---|---|---|
| vehicle positions | **basic protobuf** | Enhanced JSON adds only `block_id`, at 7.1× payload |
| trip updates | **basic protobuf** | No meaningful gain, at 10.8× payload |
| service alerts | **enhanced JSON** | Adds ~9 fields incl. `last_modified_timestamp`, at 4.5× on a 53-entity feed |

## Consequences

**Positions and trip updates stay protobuf**, which keeps the Schema Registry
work in Phase 2 natural rather than bolted on, GTFS-RT is already protobuf,
so registering and evolving these schemas is the real thing.

**`block_id` is not carried initially.** It is 100% populated and genuinely
useful, it identifies a vehicle's day of work and links consecutive trips run
by the same bus, but paying 7.1× on the highest-cadence feed for one field is
not justified while nothing consumes it. `raw.vehicle_positions` has the column
so that reversing this is an ingest change, not a migration.

If the headway analysis (proposal §6.3) turns out to need trip linkage, this
gets revisited, and doing so is a clean demonstration of exactly the schema
evolution Phase 2 exists to show: add a field, bump the version, migrate.

**Alerts take the enhanced JSON**, which is the interesting call. Payload is
irrelevant at 53 entities and 60 s cadence, 172 KB versus 38 KB is noise,
and the JSON carries `last_modified_timestamp`, `alert_lifecycle`, `severity`,
`short_header_text`, `service_effect_text`, and `informed_entity.activities`.

`last_modified_timestamp` matters specifically because the alerts topic is
log-compacted and keyed by `alert_id`. Compaction retains the last record per
key, so an explicit modification timestamp is precisely the right ordering
signal, and it exists only in the JSON.

**The cost is real and worth stating.** This feed is JSON, so it does not
participate in the Schema Registry protobuf story; its schema is enforced by
the consumer rather than the registry. It also means the pipeline handles two
wire formats, which is more code than one. Both are accepted because the
alternative is discarding the alert metadata that makes a compacted topic
worth having, and because the protobuf drops `cause_detail` and
`effect_detail`, so "just use protobuf everywhere" is not the lossless option
it appears to be.

## Note on the archive

MinIO archives **the bytes actually fetched**, per feed, in whatever format
was fetched. The archive is a faithful record of what the endpoint served, not
a normalised representation, normalising at archive time would defeat the
purpose of being able to replay history through changed decoding logic.
