# ADR 0005: BACKWARD compatibility, and registering as an explicit step

**Status:** Accepted (Phase 2)
**Date:** 2026-09-06

## Context

Phase 2 registers protobuf schemas for `enriched.vehicle_positions` and then
deliberately evolves them. Two settings decide what that exercise actually
demonstrates: the subject's compatibility level, and whether producers
auto-register.

The registry is Redpanda's built-in one (ADR 0001), serving the Confluent
Schema Registry API on the broker binary.

## Decision

**Compatibility: `BACKWARD`** on `enriched.vehicle_positions-value`.

New schema versions must be readable by consumers written against the
*previous* version.

> **AMENDED 2026-09-20.** This paragraph originally continued: *"adding
> optional fields is allowed; removing a field, renumbering one, or narrowing
> a type is rejected at registration."* **All three rejections are wrong.**
> Measured: removal, renumbering, and same-kind narrowing (`int64 → int32`)
> are all accepted. What the registry actually rejects is a change of **scalar
> kind** on an existing number. The full measured boundary and the reasoning
> are in [the revision below](#revision-2026-09-20-the-rejection-claim-above-is-wrong-for-protobuf)
> ...read it before relying on anything in this section.

**Auto-registration: off.** `auto.register.schemas=false` in the serializer.
Registration is an explicit step (`make schema-register`).

**Raw topics stay JSON and unregistered.** Only the enriched topic gets
protobuf.

## Consequences

**The v1 → v2 bump is a real change, not a demonstration prop.** v1 is the
static GTFS join; v2 adds linear referencing, schedule deviation, and
neighborhood. Those are four new optional fields, which BACKWARD permits, so
the registry accepts the bump and a v1 consumer keeps reading v2 records by
skipping fields it does not know. The evolution has an engineering reason
behind it rather than being a field added to prove a point.

**The rejection is the more instructive half.** Attempting to remove or
renumber one of the identity fields (`vehicle_id`, `position_timestamp`,
`trip_id`) fails at registration, in the process trying to register it,
before a single unreadable record exists. Demonstrating a *predicted*
rejection and resolving it properly is worth more than demonstrating a
successful bump, which only shows an API call succeeding.

**Why BACKWARD rather than FORWARD or FULL.** The consumers here lag the
producer, the enrichment consumer, the Connect sink, dbt models over the
sink. Producers move first and consumers catch up, which is exactly the
direction BACKWARD protects. FORWARD protects the opposite (old producers,
new consumers) and would be the wrong guarantee. FULL requires both and would
block adding an optional field without a default, which is the ordinary,
useful change this schema will make repeatedly.

`NONE` was never a candidate. A registry in NONE mode records what happened
without preventing anything, which is a filing cabinet rather than a contract.

**Auto-registration off costs one deploy step and buys determinism.** With it
on, any process that starts with a modified `.proto` mutates the shared
subject as a side effect, a developer iterating locally silently publishes a
schema version, and the first anyone knows is a compatibility failure
somewhere downstream. Off, a producer whose schema is not registered fails
loudly at startup, which correctly identifies a skipped deploy step rather
than corrupting a shared resource.

**Raw topics stay JSON, deliberately.** They hold 24 hours of history from
the Phase 1 collection run, and the replay path reads archived *bytes* from
MinIO rather than the topic, so migrating them buys a harder story rather than
a better one. The enriched topic is protobuf from v1, which is where the
registry work belongs anyway, the raw topics are a faithful record of what
the agency published, and the enriched topic is this project's own contract.

**What is given up.** Field numbers become permanent. A number retired from
`enriched_vehicle_position.proto` can never be reused, because old records
still on the topic would decode into the new field silently and plausibly.
The `.proto` carries a `reserved` block for exactly this, currently empty.

## Revision, 2026-09-20: the rejection claim above is WRONG for protobuf

**Retracted:** "Attempting to remove or renumber one of the identity fields
(`vehicle_id`, `position_timestamp`, `trip_id`) fails at registration."

It does not fail. Measured against the live registry with
`register --check` at BACKWARD, subject at version 2:

| Change | Verdict |
|---|---|
| `vehicle_id` **removed** | COMPATIBLE |
| `vehicle_id` **renumbered** 1 → 34 | COMPATIBLE |
| `latitude`/`longitude` numbers **swapped** | COMPATIBLE |
| `vehicle_id` `string` → `int32` | **INCOMPATIBLE** |
| `neighborhood_name` `string` → `int64` | **INCOMPATIBLE** |

The registry is behaving correctly and the ADR imported an Avro intuition.
Avro resolves a reader schema against a writer schema **by field name**, so
dropping a field without a default genuinely breaks readers. Protobuf resolves
**by field number** and skips unknown numbers by design, so removal and
renumbering are wire-compatible. Nothing is unreadable afterwards; the field
is simply absent, which is what `optional` already means.

### The gate is not lax: it answers a different question

The first draft of this revision framed the allowed changes as gaps. That is
still the wrong shape. Redpanda implements **protobuf's own wire-compatibility
table**, which explicitly declares `int32`/`uint32`/`int64`/`uint64`/`bool`
interchangeable (all varint) and `string`/`bytes` interchangeable (both
length-delimited). `sint32` differs by *encoding* (zigzag) and `float`/`double`
by *wire type* (fixed32 vs fixed64), which is why those are rejected.

So the gate answers **"can an old reader parse these bytes without error?"**
It does not and cannot answer **"do the values still mean the same thing?"**

Every dangerous change below is a case where protobuf says interchangeable and
semantics say catastrophic. That is not an oversight in the registry; it is the
definition of the word it is using. The complement has to come from somewhere
else, and naming where is the useful part of this ADR.

### What the registry actually enforces

`FIELD_SCALAR_KIND_CHANGED`, a change to a field's **scalar kind** on an
existing number. Measured, probing the boundary on field 31:

```
COMPATIBLE     int32 -> int64 | uint32 | bool      (same varint kind)
               string -> bytes                     (same length-delimited kind)
INCOMPATIBLE   int32 -> sint32                     (zigzag is its own kind)
               int32 -> double | fixed32 | string  (crosses kinds)
```

The `sint32` case is the registry being smarter than wire type alone would
suggest: `int32` and `sint32` are both varint, but zigzag encoding means
reinterpreting one as the other silently changes values, and the check catches
it.

### The complete measured boundary

| Change | Verdict | Damage if made |
|---|---|---|
| `string → int32` | INCOMPATIBLE | n/a |
| `int32 → sint32` | INCOMPATIBLE | n/a |
| `double → float` | INCOMPATIBLE | n/a |
| lat/lon **number swap** | COMPATIBLE | transposes every coordinate |
| same-typed field swap (`route_type ↔ direction_id`) | COMPATIBLE | two columns silently exchanged |
| `int32 → bool` | COMPATIBLE | every nonzero deviation → `true` |
| `int32 → uint32` | COMPATIBLE | early buses → ~4.29e9, half the distribution |
| **`optional` dropped** | COMPATIBLE | absence collapses into zero |
| `int32 → int64` | COMPATIBLE | benign widening |
| `string → bytes` | COMPATIBLE | benign |
| field removed / renumbered | COMPATIBLE | silent data loss |

### Two things it does NOT catch, and they matter more than the ones it does

**Silent value corruption inside a kind.** `int32 -> bool` passes: every
nonzero deviation becomes `true`. `int32 -> uint32` passes: every negative
deviation, a bus running *early*, half the distribution, wraps to a huge
positive. Both are compatible by the rule and destroy the data.

**Dropping `optional`, which is the worst one for this schema specifically.**
The `.proto` states its reason for using proto3 explicit presence: an unset
`schedule_deviation_seconds` must stay distinguishable from a bus exactly on
time. Measured on the wire:

```
optional int32, unset        HasField=False    0 bytes
optional int32, set to 0     HasField=True     3 bytes
```

The zero **is** written. Removing `optional` does not change the bytes the
producer emits, it changes what a reader can recover from them, collapsing
"could not compute" and "exactly on time" into the same `0`. Registry says
compatible; the stated reason for the field's declaration evaporates.

**Blast radius, counted rather than estimated: 18 fields are declared
`optional`.** Six of those are numeric, where zero is a legal value and the
collapse is therefore *silent*: `bearing`, `speed`, `route_type`,
`shape_dist_traveled`, `schedule_deviation_seconds`, `neighborhood_num`. The
other twelve are strings, where the project's own "empty string is not NULL"
rule already makes the distinction matter. An earlier draft of this revision
said "six fields", which understated it by 3×.

**Swapping numbers between same-typed fields.** `latitude` and `longitude`
are both `double`, so exchanging their field numbers passes cleanly and
transposes every coordinate in the stream. That is precisely the transposed
-coordinate failure `errors.KING_COUNTY_BBOX` exists to catch, and it is the
project's own DLQ bounds check, not the registry, that would catch it. Worth
sitting with: the compatibility gate does not protect the one field pair where
a swap is both plausible and catastrophic.

### Consequences for the demonstration

1. **The instructive rejection is a TYPE CHANGE, not a removal.** Phase 2E
   should demonstrate `string -> int32` on an identity field being rejected,
   and explain why removal is *not*. "I predicted a rejection and got one" is
   worth less than "I predicted the wrong rejection, measured, and learned
   that protobuf and Avro have different compatibility models."

2. **`reserved` is the only control for the genuinely dangerous case.**
   Reusing a retired field number with the same scalar kind but a different
   meaning is wire-compatible and semantically catastrophic, and the registry
   **cannot** catch it. That makes the `.proto`'s instruction (the first
   field this schema drops must add its number to `reserved` in the same
   commit) a process control rather than a stylistic note.

## Operational note: `normalize.schemas` must be False against Redpanda

ADR 0001 claims Redpanda's registry is API-compatible with Confluent's,
"tested rather than asserted". Here is the one measured divergence.

The `ProtobufSerializer` looks a schema up as a base64-encoded
`FileDescriptorProto`. Against Redpanda, with the serializer's own schema
string:

```
GET /subjects/<subject>?normalize=false  ->  200  version 2, id 3
GET /subjects/<subject>?normalize=true   ->  404  error 40403
```

Redpanda resolves protobuf on the non-normalizing path only; its
canonicalisation is Avro-shaped and a protobuf schema never matches through
it. With `normalize.schemas: True`, a plausible default to set, **every
produce fails** with "Schema not found (40403)" while the subject is
registered and `make schema-status` cheerfully lists it. The flag is pinned
`False` in `schema.py` with the reasoning inline, so the next reader does not
helpfully turn it back on.

This does not undermine ADR 0001's compatibility claim, but it does qualify
it: compatible in protocol, not identical in every option.

## Notes

Subject naming is TopicNameStrategy (`<topic>-value`), so one topic carries
one message type and the compatibility check is per topic. RecordNameStrategy
would allow several types per topic and move the check per type; nothing here
needs that.
