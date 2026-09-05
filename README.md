# transit-stream

Real-time telemetry pipeline over King County Metro's GTFS-Realtime feeds:
continuous ingest into Redpanda, spatial enrichment against static schedule
data, stateful stream processing for bus bunching and prediction accuracy, and
a replayable raw archive.

> Transit scheduling, geographic, and real-time data provided by permission of
> King County.

**Status: Phase 0 complete, Phase 1 scaffolded.** Reconnaissance is measured
and written up ([`docs/findings.md`](docs/findings.md)); the local stack runs
and is verified end to end; the producers are not written yet. See
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
- ADRs 0001-0004 for the decisions Phase 0 settled.
- Regression tests asserting the wire semantics the schema depends on
  (`make test`, 10 tests, no stack or network needed).

**Next (Phase 1)**

- The producer: conditional GET, decode, value-level dedup, keying, MinIO
  archive. `producer/` is scaffolded and empty.
- Kafka Connect sink config into the partitioned raw tables (`connect/`).
- Exit criterion: 24 hours of continuous uninterrupted collection across all
  three feeds.

Phases 2-6 (schema evolution, stateful processing, dbt/Airflow, CI/Terraform,
replay demo) are unstarted.

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
producer/                Phase 1 -- the live collector
consumers/               Phase 2-3 -- enrichment, bunching
connect/                 Kafka Connect sink configs
schemas/                 protobuf definitions (Phase 2)
dbt/                     Phase 4
airflow/dags/            Phase 4 -- static refresh, dbt, partitions, DLQ report
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
