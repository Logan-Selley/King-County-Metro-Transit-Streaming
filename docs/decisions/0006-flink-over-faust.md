# ADR 0006: PyFlink for stateful processing, isolated from the project venv

**Status:** Accepted (Phase 3)
**Date:** 2026-09-20
**Resolves:** the proposal's open question 1, deferred until Phase 2 was done

## Context

Phase 3 needs two things the project has not demonstrated: windowed bunching
detection over a single stream, and a two-stream join between trip-update
predictions and observed arrivals. The proposal named Faust and Flink and
deliberately postponed the choice, observing that "the two-stream join leans
toward Flink; the rest of the project leans toward Faust."

Two facts changed between the proposal and this decision.

**The disk constraint is gone.** ADR 0001 and the compose file document a root
filesystem at 96% with ~15 GB free, which was a real argument against a
JVM-based stack. Root is now a different device with **414 GB free at 36%**.
The objection no longer exists and should not be cited as if it did.

**Both libraries are alive.** `faust-streaming` 0.15.3 (2026-08-23) and
`apache-flink` 2.3.0 (2026-06-21) both publish py3.12 wheels. Neither is
abandoned, so the choice is on merits.

## Decision

**PyFlink 2.2.0**, running as JobManager + TaskManager containers behind a
compose profile.

> **Amended, 2026-09-20, the same day.** This decision originally said 2.3.0.
> Shipped is 2.2.0, because 2.3.0 has no matching Kafka connector:
> `flink-sql-connector-kafka` tops out at 5.0.0-2.2. Pairing 2.3.0 with a
> 1.20-built connector imported cleanly, submitted cleanly, and crash-looped
> in RESTARTING with a `NoSuchMethodError`; the failure is documented in
> [docker/Dockerfile.flink](../../docker/Dockerfile.flink). Connectors lag
> the runtime, so the runtime gets pinned to what has a connector.

**The Flink job does NOT share the project virtualenv.** Job code lives in its
own image with its own Python environment, and is submitted to the cluster.

## Why Flink

**Event time is the whole point of Phase 3, and Faust does not really have
it.** Bunching is "two vehicles too close together *at the same time*", and
prediction accuracy is "how wrong was an estimate issued *N minutes before*
the event". Both are event-time questions over a stream whose records arrive
out of order, vehicles go through tunnels and report bursts of stale
positions, which the proposal flagged in section 5.

Flink has watermarks, event-time windows, allowed lateness, and interval joins
as first-class constructs. Faust has stateful tables and processing-time
windows; the same analysis is possible but the ordering semantics become
hand-rolled, which is precisely the part worth demonstrating.

**The project already demonstrates hand-rolled Python consumers.** The
enrichment consumer is a keyed, stateful, in-memory-joined Kafka consumer, and
it works. Doing Phase 3 in Faust would produce more of the same shape. Flink
adds a capability the repo does not otherwise show.

**Market weight.** The proposal's framing is closing gaps that appear in
target postings, and Flink appears in them far more than Faust.

## The packaging finding, and why it forces the isolation

This is not a stylistic preference. Adding `apache-flink` to the project
venv **resolves successfully** and silently downgrades protobuf:

```
project venv today            protobuf 7.36.1
with apache-flink added       protobuf 5.29.6   (apache-beam<=2.61.0 pins <6.0)
```

It resolves, so `uv add apache-flink` prints no error. And the downgrade is
not cosmetic, measured:

```
protobuf 5.29.6    is_repeated: False    label: True
protobuf 7.36.1    is_repeated: True     label: False
```

`recon/probe.py` walks protobuf descriptors with `field.is_repeated`, which
does not exist in 5.29. Adding Flink to the venv would break the Phase 0
instrument with an `AttributeError`, the exact mirror of the bug hit at the
start of this project when that code was first written against `field.label`
on protobuf 7.

So the isolation is load-bearing rather than tidy. It also happens to be the
normal Flink deployment model: a job is submitted to a cluster and runs inside
the TaskManager's environment, not in the developer's.

### The second-order cost, found 2026-09-21

The isolation has a consequence this ADR did not anticipate: **the job cannot
import the project's generated protobuf bindings at all.** The same pin that
protects `recon/probe.py` also makes the image's runtime older than the
gencode the project emits, and protobuf refuses that combination at import:

```
Flink image protobuf   5.29.6
project bindings       gencode 7.35.1

VersionError: Detected incompatible Protobuf Gencode/Runtime versions when
loading enriched_vehicle_position.proto: gencode 7.35.1 runtime 5.29.6.
Runtime version cannot be older than the linked gencode version.
```

That is protobuf behaving correctly. Its cross-version guarantee runs one way:
gencode works against newer runtimes, never older ones. Isolating the
environment therefore isolates the *message class* too, and the job needs
another way to get one.

**Resolved by shipping a descriptor rather than code.** `make schema-gen` now
emits a serialized `FileDescriptorSet` from the same protoc invocation that
produces the bindings, and `consumers/bunching/decode.py` builds the message
class from it at startup. A FileDescriptorSet is protobuf's own wire format
for a schema, so it is version-neutral where generated Python is not.
Measured: a descriptor emitted by protoc 7.x builds a working class under
runtime 5.29.6 with explicit presence intact, and the same module decodes the
same fixture identically in both environments.

One protoc call emits both artifacts deliberately. Two commands is how the
descriptor ends up describing a different schema than the bindings the
enrichment consumer produces with.

**Generating a second set of bindings inside the image was the alternative.**
It works, and it adds a fourth version that must agree with the other three
(Flink runtime, PyFlink, Kafka connector, and now protoc). Given that this
ADR's own subject is a version conflict, adding a version seemed like the
wrong direction.

**`ProtobufDeserializer` was the other alternative**, and it is not available:
`confluent-kafka` is installed in the image but its registry client lives
behind the `[schemaregistry]` extra and imports `httpx`, then `authlib`'s
OAuth machinery. For a job whose real need is "skip six bytes of framing",
that is a large dependency to add to an image the whole ADR exists to keep
narrow. The framing is parsed in `decode.py` instead, in about fifteen lines,
asserted against a committed fixture captured off the live topic.

## Consequences

**Two Python environments, deliberately.** The project venv keeps protobuf
7.36.1 and serves the producer, the enrichment consumer, and the static
loader. The Flink image carries PyFlink and its protobuf 5.x. Nothing imports
across the boundary: the job talks to Kafka, not to `producer/` or
`consumers/`.

`pyproject.toml` must never gain `apache-flink`. A comment there says so, next
to the existing note about why dbt and Airflow are absent.

**Debugging crosses a JVM boundary.** A PyFlink error can surface as a Java
stack trace wrapping a Python one. This is the real cost and it is worth
naming: it is slower to debug than the enrichment consumer was.

**Heavier local stack.** JobManager plus TaskManager, behind a `flink` compose
profile so `make up` does not start them. Same pattern as Kafka Connect.

**The budget risk is real.** The proposal allots 4-5 days and flags Phase 3 as
the likeliest to overrun. The mitigation is already in the proposal and is
kept: bunching ships first as a standalone artifact, the prediction-accuracy
join comes second, and the second is cut if it stalls.

## Alternatives considered

- **Faust.** Lower friction, same idioms as the existing consumers, fastest
  path to a shipped bunching detector. Rejected because its event-time story
  is weak enough that the two-stream join would be hand-rolled state, which
  undercuts the claim the phase exists to support.
- **A plain Python consumer with explicit state.** Cheapest, and the project
  already proves it can do this. Rejected for the same reason as Faust, more
  so: it demonstrates nothing new.
- **Spark Structured Streaming.** Ruled out by the proposal's non-goals; the
  target postings are not Spark shops.
- **Redpanda Connect (Benthos).** Good at routing and transformation, not at
  windowed stateful joins.
