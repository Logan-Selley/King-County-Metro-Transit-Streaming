# transit-stream

[![CI](https://github.com/Logan-Selley/King-County-Metro-Transit-Streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/Logan-Selley/King-County-Metro-Transit-Streaming/actions/workflows/ci.yml)

Real-time telemetry pipeline over King County Metro's GTFS-Realtime feeds. Three
public feeds go into Redpanda, get enriched against the static schedule and
county geography, are processed statefully in Flink for bus bunching and
prediction accuracy, and land in PostGIS, where dbt and Airflow take over. Every
raw payload is archived first, so the whole pipeline can be replayed from bytes
it has already seen.

> Transit scheduling, geographic, and real-time data provided by permission of
> King County.

## The findings site

`site/` is a static page that shows what the pipeline measured: where buses
bunch, the hours and routes they bunch on, how far off Metro's arrival
predictions run by lead time, whether a replay of the archive reproduces the
live pipeline, and how much of each day the feed actually delivered.

Every number on it is cut from the dbt marts by `publish/export.py`, so the page
cannot disagree with the tests that enforce those marts. Nothing is built at
deploy time: `site/data/` is committed, `.github/workflows/pages.yml` publishes
`site/` to GitHub Pages on push, and `make site-preview` serves it locally the
way Pages will.

```bash
make exports          # re-cut site/data/ from the marts
make og-card          # re-render the link preview from that data
make site-preview     # http://localhost:8765
```

The committed snapshot is the full study week, 2026-09-24 to 09-30 Pacific:
**8.61M vehicle positions, 3,694 bunching alerts, 23.3M checked predictions,
and the feed live in 97.4% of minutes**, with a median absolute prediction
error of 44 s at under two minutes out and 202 s at 45-60 minutes. The study
window is a stated choice rather than whatever happened to be in the warehouse:
[ADR 0011](docs/decisions/0011-the-findings-site.md) records the design, and
[findings section 14](docs/findings.md) is the page's own write-up.

Built and verified end to end: the stack starts from a clean clone, CI runs three
jobs on every push, and the numbers below are measured rather than estimated.
The measurements are written up in [`docs/findings.md`](docs/findings.md),
including the ones that contradicted the original design.

## What it demonstrates, and where to read it

- **Conditional GET as a staleness signal.** The feeds refresh on a tight 20.0 s
  period, so 73% of polls return 304 for a few hundred bytes, and a stalled ETag
  is unambiguous. ([findings §2](docs/findings.md), [ADR 0004](docs/decisions/0004-delivery-semantics-and-dedup.md))
- **Partition keys that survive being derived from.** Heading and speed are
  measured from consecutive positions, so ordering per vehicle is a
  requirement, not a preference. ([ADR 0002](docs/decisions/0002-partition-key.md))
- **Schema evolution, and the limit of a registry.** Protobuf moves v1 to v2
  under BACKWARD compatibility, and the gate still cannot see a latitude and
  longitude swap. ([findings §10](docs/findings.md), [ADR 0005](docs/decisions/0005-schema-compatibility.md))
- **Event time versus processing time.** A Python operator drops the record
  timestamp, which cost a day and is the reason the watermark sits where it does.
  ([findings §11](docs/findings.md), [ADR 0006](docs/decisions/0006-flink-over-faust.md))
- **The streaming and batch boundary, drawn on purpose.** Flink owns the stream;
  Airflow owns the static feed refresh, dbt runs, partition maintenance, and DLQ
  reporting, and never orchestrates the stream. ([ADR 0008](docs/decisions/0008-warehouse-sink.md))
- **Replay as a fidelity proof.** Archived payloads go back through the live
  producer, consumer, and Flink job into an isolated namespace, and the baseline
  has to reproduce what live wrote before the experiment on top of it means
  anything. ([findings §13](docs/findings.md), [ADR 0010](docs/decisions/0010-replay-from-the-archive.md))
- **A deliverable that stays honest under repetition.** The page reads the
  tested layer only, carries the window every file was cut from, and is
  byte-identical when it is re-cut. ([findings §14](docs/findings.md), [ADR 0011](docs/decisions/0011-the-findings-site.md))

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
              deviation                              dbt models
                                                          │
                                        ┌─────────────────┴───────────────┐
                                        ▼                                 ▼
                                 Airflow (batch side)          publish/export.py
                                 - static GTFS refresh         site/data/*.json
                                 - dbt run                             │
                                 - partition maintenance               ▼
                                 - feed health                      site/ (Pages)
```

The producer is a plain Python process, not a framework job, because the ordering
inside it is load-bearing: the 304 short-circuit runs before the archive write,
the archive write before the decode, the decode before the dedupe, and the dedupe
before the publish.

The site is the other end of that line, and the only consumer that reads nothing
but the marts. It has no server behind it, which is why its data is committed and
why re-cutting an unchanged window leaves git with nothing to show.

## Results

**One day of collection** ([findings §8](docs/findings.md)). 16.3M messages,
median gap between archived payloads 21 s, and zero gaps over 90 s. 75 polls
failed, all transient network faults, all absorbed by per-feed isolation; no
decode, delivery, or archive failures. Value-level dedup suppressed **81.9%** of
trip-update rows, so 83.5M published records became 15.2M. The same run produced
a free fleet-health signal: a small population of chronically broken GPS units
generated every out-of-bounds position, and one vehicle reported null island
2,172 times, which is a maintenance ticket rather than a data-quality nuisance.

**Enrichment against the static schedule** ([findings §10](docs/findings.md)).
100% of live positions joined to a same-day static load, median schedule
deviation **+105 s**, no timezone or service-date anchor errors. 12.6% of records
carry a distance-along-shape of exactly zero, and sampling them in PostGIS shows
they are buses waiting at the first stop of a trip that has not started.

**Bus bunching** ([findings §11](docs/findings.md)). 607 alerts over a 19-hour
corpus, **40% of them in the 16:00-18:00 peak**, and the routes at the top are
the ones that should be (G Line, E Line, 7, all high-frequency). Route 255
ranked fourth on the first run, and chasing that produced the terminal gate: its
alerts came from coaches staging 1,600 ft along the shape, not from bunching
rider-facing, and the gate removes 87% of them. Parallelism 3 turned out to be
required rather than a tuning choice: at parallelism 2 the job silently lost 59%
of alerts with a healthy-looking dashboard.

**Prediction accuracy** ([findings §11 and §12](docs/findings.md)). A two-stream
join keyed on `trip_id` (45.8% of trip updates carry a vehicle, and those are
the trips that have not started, which is exactly the long-lead-time half the
analysis needs). Metro's arrival estimates run **pessimistic**: median absolute
error rises from 45 s at under two minutes out to 196 s at 45-60m, and the mean
error is positive in every forward bucket, so buses arrive earlier than the sign
says. The live job reproduced the offline curve within about 10 s per bucket, and
the warehouse mart draws the same curve again.

**Replay from the archive** ([findings §13](docs/findings.md)). One calendar day
re-run through the live code: **1,359,657 enriched rows reproduced with every
live row matched**, and 607 of 626 live alerts matched with zero field
differences. The differences that remain sit inside three windows where live was
measurably misbehaving: the replay holds 1,143 enriched rows that live never
landed, 1,138 of them in a single two-minute window. The gate experiment on top reproduces Phase 3's result on a
second day, from archived bytes.

**The page** ([findings §14](docs/findings.md)). Over the study week: 3,694
alerts, 39.3% of them between 16:00 and 18:59, about 667 a weekday against 178
a weekend day, with both peaking in the 17:00 hour. G Line leads (596), then E
Line (553) and route 7 (324), and G Line's worst stops are all on Madison St
between 5th and 12th Ave. The map places every alert, and `kpis.json` carries
that share rather than leaving it implied.

## Quickstart

Requires Docker, [uv](https://docs.astral.sh/uv/), and `make`.

```bash
uv sync
cp .env.example .env      # then edit: the passwords are literally 'change-me'
make dirs                 # needs sudo, once (see below)
make up                   # broker, warehouse, MinIO, Connect
make platform             # Terraform: topics, bucket + lifecycle, connectors, roles
make migrate              # warehouse DDL, then ownership of staging/marts to dbt_transform
make schema-register-sinks # the JSON Schema subjects the two Flink sinks frame against
make stream-up            # build the pipeline image, start producer + enrichment
```

`make check` reports container status, cluster health, topics, and disk.
`make help` lists every target. The site targets need the warehouse and nothing
else, since they read marts: on a fresh clone those are empty until `make dbt`
has run.

The order matters after the Terraform step: `make platform` creates the roles
every client logs in as, and `make migrate` is what hands `staging` and `marts`
to `dbt_transform`. On a fresh clone the containers come up before any role
exists, so the producer and the enrichment consumer log connection failures
until `make platform` has run, then reconnect on their own.

| Service | URL |
|---|---|
| Findings site (`make site-preview`) | http://localhost:8765 |
| Redpanda Console | http://localhost:8085 |
| MinIO Console | http://localhost:9001 |
| Kafka broker | `localhost:19092` |
| Schema Registry | http://localhost:18081 |
| PostGIS | `localhost:5434` |

All host ports route around a sibling project that holds **5433** and **8080**.
The convention is to prefix the conventional port with `1` (9092 to 19092, 8081
to 18081). Change them in `.env`, not in `docker-compose.yml`.

### Starting the stream, and surviving a restart

`make up` is infrastructure. The producer and the enrichment consumer are compose
services in the `stream` profile so that they carry a restart policy, which is
what stops a machine reboot from becoming hours of silently missing data:

```bash
make stream-up            # build the pipeline image, start producer + enrichment
make stream-logs          # follow them
make resume               # re-submit the Flink jobs after a JobManager restart
```

A profile governs starting, not restarting, so these come back on a boot without
the profile being named again. `make produce` and `make enrich` remain for one-off
and debug runs, and they do not survive a reboot; that difference is the point.

The two Flink jobs are submitted by `make bunching` and `make prediction`, and
`make flink-jobs` lists what is running. The TaskManager has nine slots and the
prediction join needs six of them, so a cluster that is already carrying the
detector will refuse the second job rather than queue it.

Two properties worth knowing before you edit anything:

- **A source change needs `make pipeline-image`.** The code is baked into the
  image, and Airflow's task containers run that image too, so a running stack
  keeps executing the old revision until it is rebuilt. This is not
  hypothetical: the static loader gained an exit code and a DAG was tested
  against an image built before the edit, so the task reported SUCCESS where it
  should have said SKIPPED. Nothing errored.
- **Every service is memory-capped with `memswap_limit` equal to `mem_limit`.**
  The point is not to save memory but to keep a runaway container dying inside
  its own cgroup instead of pushing the host into swap, which freezes a desktop,
  instead of failing one process. The TaskManager's 7g is the one number with a
  hard floor: it has to exceed `taskmanager.memory.process.size`.

### Why `make dirs` needs sudo

Every persistent volume is a host bind mount under `/mnt/F/docker-data`, not a
Docker named volume. Bind-mounted data directories have to be owned by the uid
the container runs as, Docker creates the path as root, and it will not fix
ownership for you, so `make dirs` creates the directories and chowns them to
postgres (999) and redpanda (101).

Named volumes were the alternative and they lose on one point: they live under
`/var/lib/docker`, and this stack exists to accumulate event data continuously. A
24-hour collection run would have filled that partition. `/mnt/F` is 3.6 TB.

## Repo layout

```
docker-compose.yml       local stack; read the header before editing
docker/initdb/           warehouse DDL, applied in order on first start
docker/Dockerfile.*      one image per runtime: pipeline, flink, connect, dbt
producer/                the collector: conditional GET, decode, dedupe, archive, publish
consumers/               enrichment, bunching, prediction accuracy
static/                  GTFS static load and the manifest that keeps it honest
schemas/                 protobuf source plus the JSON Schemas for the Flink sinks
recon/                   the Phase 0 instrument (snapshot + cadence probes)
replay/                  the replay comparison and the report behind the site's replay panel
publish/                 marts -> site/data, and the link preview rendered from it
site/                    the static page and its committed data; no server
connect/                 Kafka Connect sink configs, one file per connector
dbt/                     staging views, marts, and the singular tests over them
airflow/dags/            static refresh, dbt build, partitions, feed health
terraform/               core/ (topics, bucket, roles) and connectors/
tests/                   the contract suites, plus wire-semantics fixtures
docs/findings.md         every measurement, phase by phase
docs/decisions/          eleven ADRs, four of them carrying dated corrections
```

## Design notes worth reading

The traps below each cost real time, and each one is documented where it bit:

- **A schema registry checks the wire, not the meaning.** `int32` to `bool`
  registers as COMPATIBLE and turns every nonzero deviation into `true`; a
  latitude/longitude swap registers as COMPATIBLE and transposes every
  coordinate. The registry cannot catch either, so the project carries a value
  bound and a three-way semantic diff alongside it ([findings §10](docs/findings.md)).
- **A Python map operator does not carry its input record's timestamp.** Assign
  the watermark on the source and the job reads everything, drops nothing, and
  emits nothing, with `numLateRecordsDropped` reading 0 ([findings §11](docs/findings.md)).
- **PyFlink's `PICKLED_BYTE_ARRAY` pickles whatever you give it**, including raw
  bytes, so a Kafka sink framed with Confluent's five-byte header still wrote
  pickle opcodes. 235 contract tests passed while it was broken ([findings §12](docs/findings.md)).
- **A job's checkpoint settings come from the client that submits it**, not from
  the JobManager's properties, so retention and failure tolerance live in the job
  graph ([`consumers/checkpointing.py`](consumers/checkpointing.py)).
- **Two topics can disagree about a date format and both be right.** The raw
  topic carries ISO dates and the enriched topic carries GTFS form, which is
  invisible until a join key built from both never matches ([findings §11](docs/findings.md)).
- **An index probe and a counted join look alike and differ by sevenfold.** The
  map's mart asks each vehicle for one position, which the partition's key
  serves; counting the window's positions instead does not use that index
  ([findings §14](docs/findings.md)).
- **A committed data file should not carry a timestamp.** The site's JSON holds
  the window it was cut from and nothing about when it was written, so re-cutting
  an unchanged window is byte-identical and a diff proves the files came from the
  code ([ADR 0011](docs/decisions/0011-the-findings-site.md)).

## Data source and terms

King County Metro publishes GTFS static plus three GTFS-Realtime feeds as public
S3 objects. No API key, no registration.

- Static: `https://metro.kingcounty.gov/GTFS/google_transit.zip`
- Realtime: `https://s3.amazonaws.com/kcm-alerts-realtime-prod/{vehiclepositions,tripupdates,alerts}.pb`

Coverage includes Metro bus, Seattle Streetcar, King County Water Taxi, Sound
Transit Link light rail, and some Sound Transit Express routes. The mixed-mode
coverage is useful adversarially, since a water taxi will not snap sensibly to a
road-oriented route buffer.

Redistribution and derivative works are explicitly permitted. **Attribution is
mandatory and must be prominently displayed**, which is what the blockquote at
the top of this file is for; it belongs on any published chart as well, the
site's map included. King County service marks and logos may not be used.

Data is provided as-is with no uptime guarantee, and access may be modified or
discontinued without notice. That is a real argument for the MinIO raw archive
rather than a footnote: if the feed disappears, the archive is the project.

## Status

- **The pipeline is complete and runs locally**, from the producer through
  Connect, dbt, Airflow, and the findings site. Terraform manages the local
  stack's topics, bucket, connectors, and roles; there is no cloud deployment,
  and [ADR 0009](docs/decisions/0009-terraform-local-platform.md) records why.
- **446 contract tests** across twelve suites, plus 10 wire-semantics tests, all
  enforced in CI; each was written before the code it tests. CI is green on
  `main`.
- **The site is a snapshot by design.** Its data is committed and re-cut by
  `make exports`, so publishing an update is a commit rather than a deploy step
  that can fail halfway, and the link preview is rendered from the same JSON.
- **Known limits, stated rather than implied.** The gate experiment covers two
  days, so it shows the gate working rather than measuring it, and the route
  findings rest on the one study week the snapshot holds. The prediction
  job is not replayable, because its state expires on processing time. 35 of
  31,688 trips visit the same stop twice and join their second visit against the
  first, which is left in at 0.1% of trips.
- MIT licensed, see [LICENSE](LICENSE).
