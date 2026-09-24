# transit-stream

Real-time telemetry pipeline over King County Metro's GTFS-Realtime feeds:
continuous ingest into Redpanda, spatial enrichment against static schedule
data, stateful stream processing for bus bunching and prediction accuracy, and
a replayable raw archive.

> Transit scheduling, geographic, and real-time data provided by permission of
> King County.

**Status: Phases 0-3 complete and verified; Phase 4, the analytics layer, is
in progress.** Reconnaissance is measured and written up
([`docs/findings.md`](docs/findings.md)); the local stack runs and is verified
end to end, from the producer through Connect, dbt and Airflow. See
[Current state](#current-state).

---

## Why this project

It exists to demonstrate event streaming honestly. A companion project
(WA parcel reconciliation) covers batch orchestration, dbt, and PostGIS, and
it cannot demonstrate streaming, county assessor data refreshes quarterly.
This one covers the other half: continuous ingest, partitioning and ordering
semantics, schema evolution, stateful processing, and the streaming/batch
boundary.

Secondary goal: produce genuinely interesting transit findings. A pipeline
with no output is a demo.

## Architecture

```
3 GTFS-RT feeds   ──►  Producer (Python)  ──►  Redpanda
  vehiclepositions.pb    - conditional GET        raw.vehicle_positions
  tripupdates.pb         - decode protobuf        raw.trip_updates
  alerts_enhanced.json   - value-level dedupe     raw.service_alerts (compacted)
  (poll 10/10/30s)       - key per ADR 0002       enriched.vehicle_positions
                         - raw archive to MinIO   alerts.bunching
                                                  dlq.*
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            Enrichment        Stateful proc      Kafka Connect
            consumer          (headway/          ──► PostGIS
            - spatial join      bunching)            (partitioned by day)
              to route buffers                            │
            - schedule                                    ▼
              deviation                               dbt models
                                                          │
                                                          ▼
                                                   Airflow (batch side)
                                                   - static GTFS refresh
                                                   - dbt run
                                                   - partition maintenance
```

Airflow does **not** orchestrate the stream. It orchestrates the static feed
refresh, dbt runs, partition maintenance, and DLQ reporting. That boundary is
deliberate.

## Quickstart

Requires Docker, [uv](https://docs.astral.sh/uv/), and `make`.

```bash
uv sync
cp .env.example .env      # then edit: the passwords are literally 'change-me'
make dirs                 # needs sudo, once (see below)
make up
make topics
```

Then:

| Service | URL |
|---|---|
| Redpanda Console | http://localhost:8085 |
| MinIO Console | http://localhost:9001 |
| Kafka broker | `localhost:19092` |
| Schema Registry | http://localhost:18081 |
| PostGIS | `localhost:5434` |

`make check` reports container status, cluster health, topics, and disk.
`make help` lists every target.

### Starting the stream, and surviving a restart

`make up` is infrastructure. The producer and the enrichment consumer are
compose services in the `stream` profile so that they have a restart policy,
which is what stops a machine restart from becoming hours of silently missing
data:

```bash
make stream-up            # build the pipeline image, start producer + enrichment
make stream-logs          # follow them
make resume               # re-submit the Flink jobs after a JobManager restart
```

A profile governs **starting**, not restarting, so these come back on a boot
without the profile being named again. `make produce` and `make enrich` remain
for one-off and debug runs, and they do NOT survive a reboot -- that difference
is the point.

Two properties worth knowing:

- The pipeline image (`docker/Dockerfile.pipeline`) deliberately has no
  `ENTRYPOINT`, because one image runs three programs: the producer, the
  enrichment consumer, and the static loader that Airflow's `DockerOperator`
  calls in build step 4D. The protobuf bindings are generated inside it rather
  than copied, so it does not depend on a local `make schema-gen`.

  **A source change needs `make pipeline-image`.** The code is baked in, so the
  running containers and Airflow's task containers keep executing the old
  revision until the image is rebuilt -- and a container recreate for the
  stream side. This is not hypothetical: the loader gained its exit-99 code and
  the DAG was tested against an image built before the edit, so the task
  reported SUCCESS where it should have said SKIPPED. Nothing errored; the
  whole symptom was a green task doing the wrong thing.
- Every service in `docker-compose.yml` is memory-capped, with
  `memswap_limit` equal to `mem_limit` so it cannot swap. The point is not to
  save memory but to keep a runaway container dying inside its own cgroup
  rather than pushing the host into zram swap, which freezes a desktop instead
  of failing one process. The TaskManager's 7g is the one number with a hard
  floor: it must exceed `taskmanager.memory.process.size`.

> **Correction, 2026-09-20.** The 96%-full root filesystem described below is
> historical. Root is now a different device with **414 GB free at 36%**. The
> bind-mount design stays, a continuously-accumulating event store still does
> not belong on the OS partition, but the disk constraint was retired as an
> argument against new components in
> [ADR 0006](docs/decisions/0006-flink-over-faust.md).

### Why `make dirs` needs sudo

Every persistent volume is a **host bind mount** under `/mnt/F/docker-data`,
not a Docker named volume, and bind-mounted data directories must be owned by
the uid the container runs as, Docker creates the path as root and will not
fix ownership for you. `make dirs` creates the three directories and chowns
them to postgres (999) and redpanda (101).

The bind mounts are not a stylistic choice. Named volumes live under
`/var/lib/docker` on this machine's root partition, which is at 96% with
~15 GB free, and this stack's entire purpose is to accumulate event data
continuously. A 24-hour collection run pointed at the root partition would
fill it. `/mnt/F` is 3.6 TB.

### Ports

All host ports route around the parcel project, which holds **5433** and
**8080**. The convention is to prefix the conventional port with `1`
(9092 → 19092, 8081 → 18081). Change them in `.env`, not in
`docker-compose.yml`.

## Current state

**Done**

- Local stack: Redpanda v25.2.1 (with its built-in Schema Registry), Redpanda
  Console, PostGIS 16-3.4, MinIO. Verified end to end, cluster healthy,
  topics created with their intended configs, bucket created, warehouse schema
  applied, and a protobuf produce/consume round trip through the host listener.
- Phase 0 reconnaissance, measured rather than assumed:
  [`docs/findings.md`](docs/findings.md).
- Warehouse schema: partitioned raw tables, DLQ, partition-maintenance
  function ([`docker/initdb/01-schema.sql`](docker/initdb/01-schema.sql)).
- Topic layout with real retention and compaction settings (`make topics`).
- ADRs 0001-0008. Four carry dated corrections where measurement
  contradicted the original reasoning: 0001 on the disk constraint, 0003 on
  `block_id`'s real source, 0005 on what the Schema Registry actually
  enforces for protobuf, and 0006 on the PyFlink version pin and the
  gencode/runtime conflict its isolation created.
- **245 tests**, all enforced in CI, across seven suites: wire semantics
  (10), producer contract (46), enrichment contract (42), schema/semantic-gate
  (23), the bunching detector (63), the prediction-accuracy join (48), and the
  sink framing (13).
  The contract suites are the executable specs the implementations were
  written against, and each was written before the code it tests.

**Phase 1, complete.** 24 hours of continuous collection, 16.3M messages,
median gap 21 s, **zero gaps over 90 s**. 75 transient network faults, all
absorbed by per-feed isolation; no decode, delivery or archive failures.
Trip-update dedup sustained 81.9%. Results in
[findings §8](docs/findings.md).

**Phase 2, complete.** Static GTFS loaded into versioned PostGIS tables
(31,688 trips / 1.1M stop_times / 424 shapes / 350 neighborhood polygons),
enrichment consumer joining positions to schedule and geography, protobuf on
the Schema Registry with a v1→v2 evolution. Verified live: 100% trip join
rate, median schedule deviation **+105 s**, zero timezone or service-date
anchor errors. Results in [findings §10](docs/findings.md).

**Phase 3, complete.** Stateful stream processing on PyFlink 2.2.0
([ADR 0006](docs/decisions/0006-flink-over-faust.md)), two jobs running live
with checkpoints in MinIO. **Bunching**: 607 alerts over a spot-checked day,
40% of them in the 16:00-18:00 peak, keyed and gated per
[ADR 0007](docs/decisions/0007-bunching-key-and-gates.md). **Prediction
accuracy**: a hand-rolled two-stream join (PyFlink has no `interval_join`)
whose live output reproduced the offline error curve within ~10 s per lead
bucket overnight. Metro's estimates run pessimistic: buses arrive earlier than
the sign says, by 12 s at under two minutes and over 3 minutes an hour out.
Results in [findings](docs/findings.md).

**In progress (Phase 4), the analytics layer.** Kafka Connect sinks the
enriched topic into a partitioned PostGIS table, idempotent under a full
replay; dbt and Airflow each run in their own image
([ADR 0008](docs/decisions/0008-warehouse-sink.md)). Build order:

| step | what | state |
|---|---|---|
| 4A | Connect JDBC sink for `enriched.vehicle_positions` | **done**: 2.58M rows, replay-verified |
| 4B | Get the two Flink topics into the warehouse (JSON Schema + registry framing) | **done**: both topics re-framed in place, 1,186 alert rows and 1,302,521 prediction rows landed, every record accounted for |
| 4C | Marts: `mart_feed_health`, `mart_route_deviation_hourly` now; the prediction curve and bunching marts after 4B | **done**: 3,949 minute rows and 3,679 route-hours; `make dbt` 47 pass, 0 fail once the last two marts land |
| 4D | Airflow: `transit_health` (stall + DLQ alerts), `transit_static_refresh` | **done**: 4 of 4 DAGs, both verified end to end through Airflow |
| 4E | Warehouse retention: decide it, then add the drop task | **done**: 90 days, `raw.drop_partitions_before`, a daily drop task in `transit_partitions` |
| 4F | Drop the three unsinkable Phase 0 raw tables; the chart | **done**: all three dropped empty; the curve now comes from `mart_prediction_error_by_lead` |

Exit: marts populated on a schedule, and the prediction-error curve drawn
from the warehouse rather than from a script. The curve lands on the finding
Phase 3 measured offline: median \|error\| rises from 44 s in the 0-2m bucket to
187 s in 45-60m, and the mean error is positive at every forward-leaning lead,
so the sign runs optimistic.

```
make topics && make connect-up && make connect-register   # 4A, 4B
make schema-register-sinks && make migrate                # 4B DDL and subjects
make dbt                                                  # 47 pass, 0 fail
make airflow-check                                        # 4 of 4 DAGs complete
```

Phases 5-6 (CI/Terraform, replay demo) are unstarted.

## Phase 0 headlines

Full detail in [`docs/findings.md`](docs/findings.md). The findings that
changed design decisions:

- **Refresh cadence is 20.0 s**, not the estimated 15-30 s, and vehicle
  positions and trip updates turn over *in lockstep*, from one upstream job.
  Alerts are 60.0 s, not irregular.
- **`bearing` (2.1%) and `speed` (1.8%) are effectively unpopulated.** They
  must be derived from consecutive positions, which independently reinforces
  `vehicle_id` as the partition key.
- **An absent `current_status` means `IN_TRANSIT_TO`, not unknown.** The
  proto2 field carries `[default = IN_TRANSIT_TO]` and Metro omits it for 73%
  of vehicles. A decoder that tests presence and maps absence to `NULL`
  discards most of the column.
- **`occupancy_status` is populated (99.6%) in the basic protobuf**, which
  answers the proposal's open question about enhanced-JSON extension fields
  more cheaply than expected.
- **The prediction-accuracy join must key on `trip_id`, not `vehicle_id`.**
  Only 45.8% of trip updates carry a vehicle, the rest are trips that have
  not started, which is exactly the long-lead-time data the analysis needs.
- **~70% of every trip-updates poll is byte-identical to the previous one**,
  so value-level dedup removes about two thirds of write volume at no
  analytical cost.
- **The basic JSON feed mirrors do not exist** (403); only enhanced JSON does.

## Repo layout

```
docker-compose.yml       local stack; read the header before editing
docker/initdb/           warehouse schema, applied once on first start
recon/probe.py           Phase 0 instrument (snapshot + cadence)
producer/                Phase 1 -- the live collector, run via `make stream-up`
consumers/               Phase 2-3 -- enrichment, bunching
schemas/                 protobuf definitions (Phase 2)
dbt/                     Phase 4 -- staging views, marts, spec tests
airflow/dags/            Phase 4 -- partitions, dbt, health, static refresh
connect/                 Phase 4 -- sink configs, one file per connector
terraform/               Phase 5
tests/                   wire-semantics regression tests + fixtures
docs/findings.md         Phase 0 results
docs/decisions/          ADRs
```

## Data source and terms

King County Metro publishes GTFS static plus three GTFS-Realtime feeds as
public S3 objects. No API key, no registration.

- Static: `https://metro.kingcounty.gov/GTFS/google_transit.zip`
- Realtime: `https://s3.amazonaws.com/kcm-alerts-realtime-prod/{vehiclepositions,tripupdates,alerts}.pb`

Coverage includes Metro bus, Seattle Streetcar, King County Water Taxi, Sound
Transit Link light rail, and some Sound Transit Express routes. The mixed-mode
coverage is useful adversarially, a water taxi will not snap sensibly to a
road-oriented route buffer.

Redistribution and derivative works are explicitly permitted. **Attribution is
mandatory and must be prominently displayed**, see the top of this README;
it also belongs in any published chart. King County service marks and logos
may not be used.

Data is provided as-is with no uptime guarantee, and access may be modified or
discontinued without notice. That is a real argument for the MinIO raw
archive, not just a legal disclaimer: if the feed disappears mid-project, the
archive is the project.
