# ADR 0009: Terraform manages the local platform, not a cloud account

**Status:** Accepted
**Date:** 2026-09-24

## Context

The proposal's Phase 5 asks for "Terraform for whatever cloud surface is worth
standing up" and leaves the question open (Q3): is a cloud surface worth the
spend, or is local-only plus a strong README enough?

Two things about the stack at the end of Phase 4 answered it.

**The platform's configuration had no owner.** Topics came from
`make topics`, which runs `rpk topic create`. That is a no-op on a topic that
exists and does not apply config to it either. When 4B's re-framing migration
recreated two topics, they silently lost their 30-day retention, and the fix
was a second block of `rpk topic alter-config` calls in the Makefile. The
bucket came from a one-shot `mc mb --ignore-existing`, connectors from a curl
loop. All three create things and none of them can say whether what is
running still matches what was written down.

**Every client used the superuser.** Connect, dbt, Airflow, the enrichment
consumer and the static loader all log in to the warehouse as `transit`, which
owns every schema; the producer and both Flink jobs use MinIO's root user.

Neither problem needs a cloud account. Both need declarative configuration
with drift detection, which is what Terraform is for.

## Decision

**Terraform manages the local stack's logical resources**, against the running
services, with no cloud account involved:

| root | owns | provider |
|---|---|---|
| `terraform/core` | topics, the MinIO bucket and its lifecycle, Postgres roles and grants, MinIO users and policies | `Mongey/kafka` 0.13.1, `aminueza/minio` 3.43.0, `cyrilgdn/postgresql` 1.27.0 |
| `terraform/connectors` | the three JDBC sinks, read from `connect/*.json` | `Mongey/kafka-connect` 0.5.0 |

Terraform itself runs from `hashicorp/terraform:1.15.9` on the compose network,
the way dbt does, so nothing is installed on the host and CI runs the same
binary.

**Q3's answer: local-only, and no weaker for it.** The cost argument is ADR
0001's: a continuously running ingest against metered services is an
open-ended bill. The stronger argument is that the problems above are real on
this stack today, while a cloud module would provision an S3 bucket that
nothing in the pipeline reads. What the project demonstrates is adopting
live, data-bearing infrastructure into code without recreating it, which is
the harder and more common job.

## What stays out, and why

- **Schema Registry subjects.** Registering a schema version is an
  append-only event behind a compatibility gate (`register.py --check`,
  `sink_schemas.py --check`), not state to converge on. A Terraform resource
  would make `destroy` delete a subject that consumers still need to decode
  every record already on the topic. I also did not find a provider for a
  self-hosted, Confluent-compatible registry that I would pin.
- **Table DDL.** `docker/initdb/*.sql` stays SQL. Tables hold data, and
  changing one is a migration with an order and a history, not a converge.
  Grants are the exception: they describe roles, so they live with the roles.
- **Flink job submission and dbt models.** They are deployments of code, and
  each already has an idempotent owner (`flink-submit.sh`, `dbt build`).

## Measured while prototyping (2026-09-24)

All against the live stack, on throwaway resources wherever anything was
applied.

| check | result |
|---|---|
| import two real topics (one compacted) | 0 to add, 0 to change |
| import all three connectors via `jsondecode(file())` | 0 changes; `${env:...}` passes through unescaped because file contents are data, not templates |
| lower `partitions` on the real `raw.vehicle_positions` (plan only) | `forces replacement`: delete and recreate, so seven days of data and every group's offsets |
| raise partitions / change retention on a scratch topic | both `update in-place` |
| change retention behind Terraform's back | next plan shows `"60000" -> "7200000"` |
| role with `password_wo` from an ephemeral variable | logs in; the password occurs 0 times in the state |
| `import` of an object that does not exist | `Error: Cannot import non-existent remote object` |

These shaped five rules:

1. **`prevent_destroy` on every topic and the bucket.** Kafka cannot shrink a
   topic, so a lower partition count can only be a replacement.
2. **Partition counts are pinned to Flink parallelism by a test.** Raising a
   count applies in place, and bunching's parallelism must equal its source's
   partition count or it drops late data (the Phase 3 29.75% bug). Terraform
   cannot know that, so `tests/test_platform_contract.py` does.
3. **Adoption is gated on `adopt_existing`**, true once on this machine and
   false on a fresh stack. Unconditional imports work here and break the
   clean-clone path that Phase 5's exit criterion is about.
4. **Credentials are ephemeral variables and write-only arguments**, so local
   state holds none. That makes a local, gitignored state file acceptable.
   The connectors root holds none by construction, because 4A put
   `${env:...}` placeholders in the configs instead of passwords.
5. **Two roots, not one.** The Connect worker cannot start until core has
   created its compacted internal topics, and connectors are managed through
   that worker.

## Alternatives considered

- **A small AWS module** (an S3 copy of the raw archive, a GitHub OIDC role).
  Real cloud, and cheap. Rejected as the main deliverable because nothing in
  the pipeline would use it, and a bucket nobody reads is the cloud version of
  the empty Phase 0 tables 4F dropped. It stays a reasonable addition if a
  target role asks for AWS specifically.
- **Keep the make targets and add a drift script.** Drift detection is the
  part a script would have to reimplement per resource type. It would also
  lose `plan`, the reviewable diff before a change.
- **Remote state in MinIO.** More operators or machines would justify it. This
  stack has one of each, and MinIO is one of the things this root creates, so
  the backend would have to exist before the code that makes it.

## Consequences

- `make tf-drift` answers "does the running stack match the repository" for
  topics, the bucket, connectors and roles, which nothing could answer before.
- Topic config has one home. `make topics` and `minio-init` are retired in 5B,
  and a test fails while either still creates resources.
- A partition change is now visibly a two-file change (topic and job), or the
  suite is red.
- Bootstrapping a fresh stack is `make up` then `make platform`, not
  `docker compose up` alone. 5E proves that sequence in CI on every push.
