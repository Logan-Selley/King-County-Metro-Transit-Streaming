# Kafka Connect sink configs

One file per connector. The file name is the connector name, and the file
body is the bare config map that `PUT /connectors/<name>/config` takes.

These files are the source of truth, and `terraform/connectors/` reads them, so
`plan` shows a config edited in the worker behind Terraform's back and a
connector deleted by hand comes back on the next apply.

```
make tf-apply R=core          # Terraform creates Connect's compacted internal topics
make connect-up              # builds docker/Dockerfile.connect
make tf-apply R=connectors   # registers the three sinks from these files
make connect-status
```

`make connect-register` still exists and does the same thing with a curl loop.
It is no longer the owner of this config: since build step 5B Terraform is, and
`make tf-drift` exits 0 only while the live connectors match these files.

JSON has no comments, so the reasoning for each non-default setting in the
three configs lives here.

| Setting | Why |
|---|---|
| `${env:POSTGRES_PASSWORD}` | Resolved inside the worker by `EnvVarConfigProvider` (see `docker-compose.yml`). The committed file carries no credential, and neither does the config the REST API returns. |
| `auto.create=false` | The tables' shapes, partitioning and primary keys belong to `docker/initdb/03-sink.sql` and `05-flink-sinks.sql`. Auto-create built a plain, keyless, unpartitioned table where every column was nullable. |
| `insert.mode=upsert` on a natural key | Delivery is at-least-once and a restart replays from the last committed offset. Upsert on the natural key makes a replayed record overwrite itself. Same idempotency design as Phase 0's DDL (ADR 0004), and on these two topics it is doing real work rather than standing by: the alert topic held 1,033 duplicate alerts and the prediction topic 43,705 duplicate pairs, both measured, and the upsert is what collapses them. |
| `TimestampConverter` on the epoch fields | The payloads carry epoch-second integers. Converting them in the connector is what lets the table partition by day on a real `timestamptz`. `unix.precision=seconds` matters: the default is milliseconds, which would put every row in January 1970. |
| `unix.precision=seconds` needs an INTEGER field | Specifically it needs `INT32`/`INT64`, and it refuses a float: `Schema Schema{FLOAT64} does not correspond to a known timestamp type format`. Both JSON schemas originally declared these fields as `number`, the jobs emitted Python floats, and every task on both connectors failed having written zero rows. The schemas declare `integer` now, the jobs coerce at the point the timestamp enters the pipeline, and `tests/test_sink_framing.py` asserts payload types against the declared ones, because matching names is what failed to catch this. |
| `value.converter=JsonSchemaConverter` | The worker's default is the protobuf converter, which is right for `enriched.vehicle_positions` and rejects everything else. The registered schema is what supplies the sink's column list, so the converter, `schemas/json/*.json` and the registry subject are all part of the table's contract: `auto.evolve=false` means a schema the table disagreeing with is a failed task, not a widened table. |
| `tasks.max=3` / `tasks.max=1` | One task per topic partition. The prediction topic has three (ADR 0002); the alert topic has one, and more tasks would sit idle. |
| `table.types=PARTITIONED TABLE` | Postgres reports a partitioned table through JDBC metadata as type `PARTITIONED TABLE`, and the sink's existence check looks only for `TABLE` by default. Without this, every record failed with `Table "transit"."raw"."enriched_vehicle_positions" is missing and auto-creation is disabled`, on a table that existed. |
| `errors.tolerance=none` | Fail loudly, and this was measured rather than chosen on principle. The first version used `errors.tolerance=all` with a dead-letter topic. The `table.types` bug above then failed **every** record: 317,144 went to the DLQ, zero reached the table, and `connect-status` showed `RUNNING RUNNING RUNNING` for over ten minutes. JDBC sink 10.x sends database write errors to the DLQ too, not just converter errors, so tolerance turned a total outage into a green dashboard. All three topics are registry-validated now, so a bad individual record is close to impossible, and anything that fails here is systemic and should stop the task. |
| `auto.offset.reset=earliest` | The first registration backfills everything the topic retains (30 days on the two Flink topics). The DDL creates daily partitions covering that window first, because Postgres refuses to create a partition once the default partition holds rows for that day. |

## What is not sunk, and why

The raw topics (`raw.vehicle_positions`, `raw.trip_updates`,
`raw.service_alerts`) are schemaless JSON straight off the producer (ADR 0005),
so the three Phase 0 tables in `01-schema.sql` that were designed for them
cannot be filled by Connect. The Flink path is what reads them, and 4F drops
those tables rather than leaving three empty ones in the warehouse.

The two Flink topics were in the same position until build step 4B: schemaless
JSON that the sink rejected on the first record ("requires records with a
non-null Struct value and non-null Struct schema"). Framing them as registered
JSON is what changed, and because a connector reading `earliest` would now die
on the first unframed record still in the topic, `consumers/reframe_topic.py`
re-framed both topics in place. It dumps every record to a file before it
deletes anything, which is not ceremony: the first attempt at the 1.35M-record
prediction topic hit the producer's local queue limit and died mid-republish,
and the dump is what made that a 3-minute rerun instead of a data loss.
