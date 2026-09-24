# ADR 0008: Kafka Connect sinks the registered topic; the Flink output needs a schema first

**Status:** Accepted, all three topics. `enriched.vehicle_positions` in step 4A;
the two Flink topics in 4B, which adopted the JSON Schema option below.
**Date:** 2026-09-23

## Context

Phase 4 starts from an empty warehouse. At the end of Phase 3, every table in
`raw.*` held zero rows, `connect/` was empty, and the Connect image had no
sink connector installed. Nothing had ever crossed from Kafka into PostGIS.
dbt and Airflow need that boundary to exist, so Phase 4 builds it first.

There are six topics a mart might want:

| topic | format | registered |
|---|---|---|
| `enriched.vehicle_positions` | protobuf | yes (ADR 0005) |
| `alerts.bunching` | JSON Schema | yes (4B, revisiting ADR 0007) |
| `analytics.prediction_accuracy` | JSON Schema | yes (4B) |
| `raw.vehicle_positions`, `raw.trip_updates`, `raw.service_alerts` | JSON | no (ADR 0005) |

The JDBC sink needs a schema for every record. Against a schemaless JSON topic
it fails on the first one, measured:

```
requires records with a non-null Struct value and non-null Struct schema,
but found record at (topic='alerts.bunching', ...) with a HashMap value and
null value schema.
```

## Decision

**Sink `enriched.vehicle_positions` with the Confluent JDBC sink** into
`raw.enriched_vehicle_positions`: daily partitions, primary key
`(vehicle_id, position_timestamp)`, `insert.mode=upsert`. The table is created
by `docker/initdb/03-sink.sql`, not by Connect. Built and verified; see below.

**For the two Flink topics, register a JSON Schema and have the jobs write
Confluent framing** (built and verified in 4B: `consumers/framing.py`,
`consumers/sink_schemas.py`, `schemas/json/`). Connect then reads them with
`JsonSchemaConverter`, the same way it reads the protobuf topic.

**Do not sink the raw topics.** The three Phase 0 tables designed for them
(`raw.vehicle_positions`, `raw.trip_updates`, `raw.service_alerts` in
`01-schema.sql`) cannot be filled by the JDBC sink, and nothing reads them:
the enriched table is a superset of raw positions plus the joins, and
prediction accuracy is computed in the stream rather than from raw trip
updates. **Dropped in 4F** by `docker/initdb/06-drop-unsunk-raw.sql`, which runs
during the same `make migrate`; `01-schema.sql` still defines them as the record
of what Phase 0 designed, and on a fresh database they exist for the length of
one migrate run and then are gone.

## The enriched sink: four things broke, all now fixed

None of these showed up in configuration review. Each was found by running it.

1. **The image's own ProtobufConverter crashes.** `java.lang.VerifyError` on
   `MetaProto`, because the worker classpath puts
   `/usr/share/java/kafka/protobuf-java-3.23.4.jar` ahead of the 3.25.4 the
   converter was built against. It's the same rule the Flink decoder hit in
   Python (ADR 0006's revision): a protobuf runtime may not be older than its
   generated code. The first fix, copying 3.25.4 into `/etc/kafka-connect/jars`,
   did nothing, because the base image declares that path a `VOLUME` and
   Docker discards build-time writes to one. `docker/Dockerfile.connect`
   replaces the old jar in place.
2. **The sink couldn't see a partitioned table.** Postgres reports one
   through JDBC metadata as `PARTITIONED TABLE`, and the sink looks only for
   `TABLE` unless `table.types` says otherwise. Every record failed with
   "Table ... is missing", on a table that existed.
3. **Error tolerance hid that total failure.** With `errors.tolerance=all`,
   JDBC sink 10.x routes database write errors to the DLQ as well, so
   317,144 records went there, none reached the table, and every task
   reported `RUNNING` for over ten minutes. The sink is now `none`: this
   topic is registry-validated, so a per-record failure is close to
   impossible, and anything that fails here is systemic.
4. **Connect's internal topics must be created compacted, up front.** If a
   client touches them first, Redpanda auto-creates them with
   `cleanup.policy=delete` and the worker refuses to start. `make topics`
   now creates them.

**Verified:**

| check | result |
|---|---|
| rows landed vs topic records | 2,579,416 of 2,579,949; the difference is duplicate keys collapsed by upsert |
| stale 2026-09-04 records | all 10 in the default partition, as designed |
| optional numerics | `bearing` 96.4% NULL, 0% zero: absence survives the converter (ADR 0005's point) |
| **full replay from offset 0** | rows existing before replay: 2,578,202, and **2,578,202 after**. Idempotent. |
| credentials | the REST API returns `${env:POSTGRES_PASSWORD}`, not the password |

## Options for the Flink topics (4B)

Measured on real records from both topics:

| option | bytes per accuracy record | what it costs |
|---|---|---|
| Connect JSON envelope (`schemas.enable=true`) | 1,004 (**3.7×**) | The schema repeats in every message. At ~1M accuracy records a day, that's ~1.1 GB/day instead of ~300 MB. No compatibility checking. |
| **JSON Schema in the registry, Confluent framing** | 277 (+5) | A schema registration step, and a small encoder in the job. |
| A Python sink consumer instead of Connect | 272 | A second sink mechanism and another long-running process, when the point of this phase is one boundary done well. |

**JSON Schema with registry framing is what 4B adopted.** The framing for
JSON Schema is five bytes (magic `0x00` plus a big-endian schema id), with no
message-index array, so the encoder is simpler than `decode.strip_framing`,
which already handles the harder protobuf case.

This also revisits ADR 0007, which kept `alerts.bunching` unregistered. Its
only reason was that the Confluent `ProtobufSerializer` can't run in the
Flink image. Writing framing by hand needs no serializer at all, so that
reason doesn't apply to JSON Schema, and both topics would get the same
compatibility enforcement the enriched topic has.

## Consequences

- The warehouse has a daily-partitioned landing table that stays correct
  under replay, and dbt builds on it now (`make dbt`).
- Partitions must exist before the data does. Once the default partition
  holds a day's rows, Postgres refuses to create that day's partition, so
  `03-sink.sql` pre-creates the topic's full retention window and
  `transit_partitions` runs well before midnight.
- **Retention was decided in 4E rather than defaulted.** 90 days
  (`RETENTION_DAYS` in `airflow/dags/transit_partitions.py`), against a measured
  730 MB/day and 2.8 T free. The reasoning, the alternatives and the
  irreversibility argument are in `docker/initdb/04-retention.sql`.
