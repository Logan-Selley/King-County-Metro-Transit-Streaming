# transit-stream -- task runner
#
# Recipes run under bash regardless of your login shell, so the bash-only
# `set -a` idiom works even though the project shell is fish.
#
# The one thing worth reading before using this file: `make dirs` needs sudo,
# once, before the first `make up`. Bind-mounted data directories have to be
# owned by the uid each container runs as, and Docker will not fix that for
# you -- it creates the path as root and postgres then refuses to initialize
# into a directory it does not own. See the target for the specific uids.

SHELL := /bin/bash
ROOT  := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
PY    := $(ROOT)/.venv/bin/python
DC    := docker compose -f $(ROOT)/docker-compose.yml

ENV := set -a; . $(ROOT)/.env; set +a;

DATA := /mnt/F/docker-data

.DEFAULT_GOAL := help

help:  ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- environment -------------------------------------------------------------

$(ROOT)/.env:
	@test -f $(ROOT)/.env || { \
	  echo "No .env found. Copying the template:"; \
	  cp $(ROOT)/.env.example $(ROOT)/.env; \
	  echo "  cp .env.example .env"; \
	  echo "EDIT IT -- the passwords are literally 'change-me'."; \
	  exit 1; }

dirs:  ## Create + chown the bind-mounted data directories (needs sudo; safe to re-run)
	@# postgres runs as uid 999 and checks PGDATA ownership against its own
	@# uid before initdb; redpanda runs as uid 101. Getting either wrong shows
	@# up as a container that starts, logs a permission error, and exits 1.
	@# Measured for redpanda on 2026-09-25 with a directory Docker created as
	@# root, which is what a fresh CI runner gets:
	@#
	@#   Failure during startup: ... mkdir failed: Permission denied
	@#
	@# chown -R, not a plain chown. The directory alone is not enough once a
	@# container has written into it: any content created under a different
	@# uid (a stack brought up with a `user:` override, a volume moved between
	@# machines) stays unreadable and postgres fails AFTER initdb rather than
	@# before, which is a much more confusing failure.
	@#
	@# BUT ONLY WHEN THE TOP-LEVEL OWNER IS WRONG. The first version walked every
	@# tree on every run, and a re-run beside a live stack failed: MinIO creates
	@# and deletes temp files under .minio.sys/tmp continuously, one vanished
	@# between the directory listing and the chown, and chown exits 1 on
	@# "No such file or directory". A directory already owned by the right uid
	@# is left alone, which makes the target idempotent and keeps it off live
	@# data trees.
	@#
	@# minio runs as uid 65532 since the image moved to Chainguard's build
	@# (docker-compose.yml says why). The old minio/minio image ran as root and
	@# needed no chown; this one fails to write a root-owned /data. An existing
	@# root-owned MinIO directory is therefore chowned ONCE, and that walk must
	@# not race a running server: `docker compose stop minio` first.
	@#
	@# Airflow's task logs are the one bind mount INSIDE the repo rather than
	@# under $(DATA) (docker-compose.airflow.yml). The container runs as 50000:0,
	@# and a root-owned 755 directory makes it die at startup with
	@#
	@#   ValueError: Unable to configure handler 'processor'
	@#
	@# before it can even migrate its own database, which is a log-permission
	@# error wearing a logging-config error's clothes. Added 2026-09-23, when
	@# `make airflow-up` was found to have never produced a running Airflow on
	@# this machine.
	@sudo mkdir -p $(DATA)/transit-warehouse $(DATA)/transit-redpanda \
	  $(DATA)/transit-minio $(DATA)/transit-airflow-db $(ROOT)/airflow/logs
	@own() { \
	  if [ "$$(sudo stat -c %u:%g "$$1")" != "$$2" ]; then \
	    sudo chown -R "$$2" "$$1" && echo "  chowned $$1 -> $$2"; \
	  fi; }; \
	own $(DATA)/transit-warehouse 999:999 && \
	own $(DATA)/transit-airflow-db 999:999 && \
	own $(DATA)/transit-redpanda 101:101 && \
	own $(DATA)/transit-minio 65532:65532 && \
	own $(ROOT)/airflow/logs 50000:0
	@sudo chmod 700 $(DATA)/transit-warehouse
	@echo "data directories ready under $(DATA)"

up: $(ROOT)/.env  ## Start the core stack (redpanda, console, warehouse, minio)
	@$(DC) up -d
	@echo
	@echo "  console    http://localhost:$${CONSOLE_PORT:-8085}"
	@echo "  minio      http://localhost:$${MINIO_CONSOLE_PORT:-9001}"
	@echo "  kafka      localhost:$${REDPANDA_KAFKA_PORT:-19092}"
	@echo "  registry   http://localhost:$${REDPANDA_REGISTRY_PORT:-18081}"
	@echo "  warehouse  localhost:$${WAREHOUSE_PORT:-5434}"

connect-up: $(ROOT)/.env  ## Build + start Kafka Connect, and wait until its REST API answers
	@# --wait: return only once the worker's healthcheck passes. Without it this
	@# returned the moment the container started, while the worker needs up to
	@# two minutes to open port 8083 (its image's own healthcheck allows a 120s
	@# start period). The next step in `make platform` and in CI, the connectors
	@# apply, then failed on every connector with
	@#
	@#   Error: Post "http://connect:8083/connectors": ... connection refused
	@#
	@# Found by rehearsing the `platform` CI job on an empty daemon, 2026-09-25.
	@# On this machine the worker had always been up long before anyone applied.
	@$(DC) --profile connect up -d --build --wait connect

# PUT, not POST: PUT /connectors/<name>/config creates or updates, so this is
# safe to re-run after editing a config. Name comes from the file name.
connect-register:  ## Register/update every sink in connect/*.json
	@for f in $(ROOT)/connect/*.json; do n=$$(basename $$f .json); \
	  echo "registering $$n"; \
	  curl -sf -X PUT -H 'Content-Type: application/json' --data @$$f \
	    localhost:$${CONNECT_PORT:-8083}/connectors/$$n/config >/dev/null || exit 1; done

connect-status:  ## Show every connector's state and task states
	@curl -s "localhost:$${CONNECT_PORT:-8083}/connectors?expand=status" | python3 -c \
	  "import json,sys; [print(f\"{n:<32} {v['status']['connector']['state']:<8} tasks: {' '.join(t['state'] for t in v['status']['tasks'])}\") for n,v in json.load(sys.stdin).items()]"

down:  ## Stop the stack (data directories are preserved)
	@# Every profile a start target can bring up, or a container outside them
	@# survives `make down` and keeps writing to a stopped broker. flink is
	@# absent on purpose: it has its own target pair, and leaving the cluster
	@# running against a stopped broker is the pre-existing behaviour rather
	@# than one this change introduces.
	@$(DC) --profile connect --profile stream down

ps:  ## Show container status
	@$(DC) ps

logs:  ## Tail logs (SVC=redpanda to narrow)
	@$(DC) logs -f $(SVC)

# --- topics ------------------------------------------------------------------
# MOVED TO TERRAFORM, build step 5B. Topic config lives in
# terraform/core/topics.tf, and `make platform` is what applies it.
#
# WHY IT MOVED. This target ran a broker-CLI topic create per topic, which is a
# no-op on a topic that already exists and applies no config either. Two topics
# were recreated by 4B's re-framing migration and silently lost their 30-day
# retention; the fix was a second block of `rpk topic alter-config` calls right
# here. Two sources for one setting is how that happened, and neither of them
# could say whether the running stack still matched what was written down.
#
# tests/test_platform_contract.py now fails while any raw topic-create call
# appears in this file -- a blunt substring check, so this comment cannot quote
# the command either. `make tf-drift` answers the question the target could not:
# does the live broker match the repository?
#
# The reasoning that used to live below (partition counts are for rebalancing
# rather than throughput; retention is grounded in the Phase 0 measurements) is
# next to the values in topics.tf.

topic-describe:  ## Show config for one topic (T=raw.service_alerts)
	@test -n "$(T)" || { echo "usage: make topic-describe T=raw.service_alerts"; exit 2; }
	@$(DC) exec -T redpanda rpk topic describe $(T) -c

# --- reconnaissance (Phase 0) ------------------------------------------------

recon:  ## Phase 0 snapshot -- sizes, entity counts, field population census
	@$(ENV) $(PY) $(ROOT)/recon/probe.py snapshot

# --- producer (Phase 1) ------------------------------------------------------

feeds:  ## Show the feed manifest (poll intervals, formats, keys, topics)
	@$(ENV) $(PY) -m producer.run --list

produce-dry:  ## One tick per feed, fetch+archive+decode, publish nothing
	@$(ENV) $(PY) -m producer.run --once --dry-run -v

produce:  ## Run the producer (D=24h to bound it; default runs until stopped)
	@$(ENV) $(PY) -m producer.run $(if $(D),--duration $(D))

cadence:  ## Phase 0 cadence -- measure real refresh interval (M=minutes)
	@$(ENV) $(PY) $(ROOT)/recon/probe.py cadence -m $(or $(M),10)

# --- static reference + enrichment (Phase 2) ---------------------------------

static-status:  ## Show loaded static GTFS versions
	@$(ENV) $(PY) -m static.run --status

static-load:  ## Load the GTFS zip into PostGIS (skips if the ETag is unchanged)
	@# 99 is static.run's "the ETag has not moved": not a failure, and the one
	@# exit code the Airflow DAG turns into SKIPPED. Passed through unchanged
	@# it makes `make` print "*** Error 99" and reads as a broken load, so it
	@# is caught here and only a real non-zero code escapes.
	@$(ENV) $(PY) -m static.run --load; rc=$$?; \
	  if [ $$rc -eq 99 ]; then echo "  (ETag unchanged: nothing to load)"; \
	  else exit $$rc; fi

static-hoods:  ## Load the King County neighborhood polygons
	@$(ENV) $(PY) -m static.run --neighborhoods

# Registration is DELIBERATELY a separate step from running a producer.
# auto.register.schemas is off (ADR 0005) so a process cannot mutate a shared
# subject as a side effect of starting.
schema-gen:  ## Generate _pb2.py + .desc from the .proto (build output, gitignored)
	@# --descriptor_set_out is for the Flink job, which CANNOT import the
	@# _pb2.py: the image runs protobuf 5.29.6 (apache-beam pins <6) and the
	@# gencode is 7.35.1, which protobuf refuses at import because a runtime
	@# may not be older than its gencode. A serialized FileDescriptorSet is
	@# protobuf's own version-agnostic format, so the job builds the message
	@# class from it at startup. See consumers/bunching/decode.py.
	@#
	@# ONE protoc invocation emits both, deliberately. Two commands is how
	@# the descriptor ends up describing a different schema than the bindings.
	@$(PY) -m grpc_tools.protoc -I$(ROOT)/schemas \
	  --python_out=$(ROOT)/schemas --pyi_out=$(ROOT)/schemas \
	  --descriptor_set_out=$(ROOT)/schemas/enriched_vehicle_position.desc \
	  $(ROOT)/schemas/enriched_vehicle_position.proto
	@echo "schemas/enriched_vehicle_position_pb2.py"
	@echo "schemas/enriched_vehicle_position.desc"

schema-register:  ## Register the enriched protobuf schema
	@$(ENV) $(PY) -m consumers.enrichment.register

# The two Flink output topics, which are JSON rather than protobuf and need a
# schema of their own before the JDBC sink will read them (ADR 0008, step 4B).
# Separate from `schema-register` because they are a different registry type
# against a different producer; `--check` and `--status` work the same way.
schema-register-sinks:  ## Register the JSON Schemas for alerts.bunching and analytics.prediction_accuracy
	@$(ENV) $(PY) -m consumers.sink_schemas $(ARGS)

schema-status:  ## Show registered subjects, versions and compatibility
	@$(ENV) $(PY) -m consumers.enrichment.run --status

enrich-dry:  ## Consume + enrich, publish nothing
	@$(ENV) $(PY) -m consumers.enrichment.run --dry-run -v

enrich:  ## Run the enrichment consumer (V=2 for the spatial pass)
	@$(ENV) $(PY) -m consumers.enrichment.run --schema-version $(or $(V),1)

# --- supervised stream (run continuously, survive a restart) -----------------
# The compose services in the `stream` profile. `make produce` and `make
# enrich` above remain for one-off and debug runs; these are the ones with a
# restart policy, and they are what stop a machine restart from becoming
# hours of silently missing data.

pipeline-image:  ## Build the pipeline image (producer, enrichment, static loader)
	@docker build -q -t transit-pipeline:0.1.0 -f $(ROOT)/docker/Dockerfile.pipeline $(ROOT)

stream-up: $(ROOT)/.env  ## Build + start the supervised producer and enrichment consumer
	@$(DC) --profile stream up -d --build
	@$(DC) --profile stream ps

stream-logs:  ## Tail the supervised stream's logs
	@$(DC) --profile stream logs -f --tail 20 producer enrichment

contract-p2:  ## Run the Phase 2 contract tests (the spec)
	@$(PY) -m pytest $(ROOT)/tests/test_enrichment_contract.py -m contract \
	  $(if $(K),-k $(K)) -v

# --- stateful processing (Phase 3) -------------------------------------------
# Flink runs behind a compose profile, like connect. The job does NOT use the
# project venv -- apache-flink pins protobuf<6 and would silently downgrade
# the 7.36.1 the decoders need (ADR 0006).

flink-up: $(ROOT)/.env  ## Build the PyFlink image and start the cluster
	@$(DC) --profile flink up -d --build
	@echo "Flink UI -> http://localhost:$${FLINK_UI_PORT:-8086}"

flink-down:  ## Stop the Flink cluster
	@$(DC) --profile flink stop flink-jobmanager flink-taskmanager flink-submit

resume:  ## Re-submit the Flink jobs (idempotent; see docker/flink-submit.sh)
	@$(DC) --profile flink up -d flink-jobmanager flink-taskmanager flink-submit
	@$(DC) --profile flink restart flink-submit

flink-ui:  ## Print the Flink web UI URL
	@$(ENV) echo "http://localhost:$${FLINK_UI_PORT:-8086}"

# --pyFiles /opt/jobs is not optional, and the failure it prevents is
# confusing: `flink run -py <script>` puts the SCRIPT'S OWN directory on the
# python path, not the mount root, so `from consumers.bunching.decode import
# decode` dies with "No module named 'consumers'" -- a plain ImportError in a
# job whose imports are fine, from a client that never tells you which path it
# used. --pyFiles adds the root for both the client and the UDF workers.
FLINK_RUN = flink run --pyFiles /opt/jobs -py
# Streaming jobs submit DETACHED. Attached, `flink run` blocks until the job
# ends -- which for an unbounded source is never -- so `make bunching` hangs
# the terminal, and interrupting it leaves you unsure whether the job is still
# up (it is: without -sae, killing the client does not cancel the job).
# flink-smoke stays attached because it is bounded and waiting is the point.
FLINK_RUN_D = flink run -d --pyFiles /opt/jobs -py

flink-smoke:  ## Prove the Flink->Kafka path works and records DECODE (bounded)
	@$(DC) exec -T flink-jobmanager $(FLINK_RUN) /opt/jobs/consumers/bunching/smoke.py

bunching:  ## Submit the bunching detection job to the cluster
	@$(DC) exec -T flink-jobmanager $(FLINK_RUN_D) /opt/jobs/consumers/bunching/job.py

prediction:  ## Submit the prediction-accuracy join to the cluster
	@# Needs 6 slots (raw.trip_updates has 6 partitions); `make flink-jobs`
	@# first, because a second job on a full cluster fails with
	@# NoResourceAvailableException rather than queueing.
	@$(DC) exec -T flink-jobmanager $(FLINK_RUN_D) /opt/jobs/consumers/prediction/job.py

flink-jobs:  ## List running Flink jobs
	@$(DC) exec -T flink-jobmanager flink list

contract-p3f:  ## Run the Phase 3F prediction-accuracy spec (no cluster needed)
	@$(PY) -m pytest $(ROOT)/tests/test_prediction_contract.py -m contract \
	  $(if $(K),-k $(K)) -v

contract-p3:  ## Run the Phase 3 detector spec (no cluster needed)
	@# Runs in the PROJECT venv, not the Flink image, which is the whole
	@# reason consumers/bunching/detect.py holds no pyflink import. K=proximity
	@# to narrow.
	@$(PY) -m pytest $(ROOT)/tests/test_bunching_contract.py -m contract \
	  $(if $(K),-k $(K)) -v

flink-logs:  ## Tail TaskManager logs (where Python job errors surface)
	@$(DC) logs -f flink-taskmanager

# --- tests -------------------------------------------------------------------

test:  ## Run the wire-semantics tests (no stack, no network needed)
	@$(PY) -m pytest $(ROOT)/tests -q

# The producer spec. Fails until the stubs in producer/ are implemented --
# that is the point. Excluded from `make test` and CI so work in progress
# does not show up as a broken build. K=dedupe to narrow.
contract:  ## Run the producer contract tests (the Phase 1 spec)
	@$(PY) -m pytest $(ROOT)/tests/test_producer_contract.py -m contract \
	  $(if $(K),-k $(K)) -v

contract-schema:  ## Assert what the registry actually enforces (needs it running)
	@$(ENV) $(PY) -m pytest $(ROOT)/tests/test_schema_compatibility.py -m contract -v

smoke:  ## Produce + consume a protobuf round trip against the running stack
	@$(ENV) $(PY) $(ROOT)/tests/smoke_roundtrip.py

sink-roundtrip:  ## Produce one framed record to alerts.bunching; wait for the row (ARGS="--clean")
	@$(ENV) $(PY) $(ROOT)/tests/sink_roundtrip.py $(ARGS)

# --- analytics layer (Phase 4) -----------------------------------------------
# dbt and Airflow each run in their own image; neither is in the project venv
# and they never share one with each other (docker/Dockerfile.dbt,
# docker-compose.airflow.yml). dbt runs as a container on the compose network,
# the same way Airflow's DockerOperator runs it, so a model that works under
# `make dbt` works under the scheduler.

DBT_IMAGE := transit-dbt:1.11.0
# dbt_transform, not the superuser, since build step 5C. The password comes from
# .env as DBT_TRANSFORM_PASSWORD and is passed straight through; the user name is
# the role name and has no second spelling.
DBT_RUN = $(ENV) docker run --rm --network transit-stream_default \
	  -v $(ROOT)/dbt:/dbt -e DBT_PROFILES_DIR=/dbt -e DBT_USE_COLORS=false \
	  -e DBT_HOST -e DBT_PORT -e DBT_USER=dbt_transform \
	  -e DBT_PASSWORD=$$DBT_TRANSFORM_PASSWORD -e POSTGRES_DB $(DBT_IMAGE)

dbt-image:  ## Build the dbt image (docker/Dockerfile.dbt)
	@docker build -q -t $(DBT_IMAGE) -f $(ROOT)/docker/Dockerfile.dbt $(ROOT)

dbt:  ## Run a dbt command in its container (ARGS="build --select stg_vehicle_positions")
	@$(DBT_RUN) $(or $(ARGS),build)

dbt-freshness:  ## dbt source freshness: is the sink still receiving data?
	@$(DBT_RUN) source freshness

AIRFLOW := $(ENV) PWD=$(ROOT) docker compose -f $(ROOT)/docker-compose.airflow.yml

airflow-up: dbt-image  ## Start Airflow (http://localhost:8088, admin/admin)
	@$(AIRFLOW) up -d
	@echo "Airflow -> http://localhost:$${AIRFLOW_PORT:-8088}"

airflow-down:  ## Stop Airflow (metadata is preserved on the bind mount)
	@$(AIRFLOW) down

airflow-logs:  ## Tail the Airflow scheduler/webserver logs
	@$(AIRFLOW) logs -f airflow

# The DAG spec. Imports every DAG inside the Airflow image and checks each has
# the tasks airflow/check_dags.py requires -- and fails on any import error,
# which is how a DAG vanishes from the UI without a word.
#
# THE ENVIRONMENT HERE MATTERS. The DAGs read their task containers' credentials
# at import time, so every variable they name has to exist or the import fails and
# the DAG reports as missing. Since 5C those are the per-role passwords rather
# than POSTGRES_USER/POSTGRES_PASSWORD: x is fine for all of them, because this
# only imports, it never connects.
airflow-check:  ## Import every DAG and check required tasks (runs in the Airflow image)
	@docker run --rm --entrypoint python \
	  -e TRANSIT_HOST_ROOT=$(ROOT) -e POSTGRES_DB=transit \
	  -e DBT_TRANSFORM_PASSWORD=x -e STATIC_LOADER_PASSWORD=x \
	  -e AIRFLOW__CORE__LOAD_EXAMPLES=false \
	  -v $(ROOT)/airflow/dags:/opt/airflow/dags:ro \
	  -v $(ROOT)/airflow/check_dags.py:/opt/airflow/check_dags.py:ro \
	  apache/airflow:2.10.5 /opt/airflow/check_dags.py

# --- diagnostics -------------------------------------------------------------

# docker-entrypoint-initdb.d runs ONCE, on first creation of the data
# directory. A .sql file added later never executes, which is how a schema
# ends up in git and not in the database. Every file there is written to be
# re-runnable (CREATE ... IF NOT EXISTS, CREATE OR REPLACE), so applying them
# to a live warehouse is safe and is the non-destructive alternative to
# `make nuke-warehouse`.
migrate:  ## Apply docker/initdb/*.sql to the RUNNING warehouse (idempotent)
	@for f in $(ROOT)/docker/initdb/*.sql; do \
	  echo "applying $$(basename $$f)"; \
	  $(ENV) docker exec -i transit_warehouse psql -q -v ON_ERROR_STOP=1 \
	    -U $$POSTGRES_USER -d $$POSTGRES_DB < $$f || exit 1; \
	done
	@echo "schemas now present:"
	@$(ENV) docker exec transit_warehouse psql -U $$POSTGRES_USER -d $$POSTGRES_DB \
	  -tAc "select schema_name from information_schema.schemata where schema_name in ('raw','staging','marts','static') order by 1" | sed 's/^/  /'

psql:  ## Open psql against the warehouse
	@$(ENV) docker exec -it transit_warehouse psql -U $$POSTGRES_USER -d $$POSTGRES_DB

check:  ## Verify the environment before blaming a consumer
	@echo "containers:"
	@for c in transit_redpanda transit_warehouse transit_minio transit_console; do \
	  printf '  %-20s %s\n' "$$c" "$$(docker inspect -f '{{.State.Status}}' $$c 2>/dev/null || echo 'not running')"; \
	done
	@echo "cluster:"
	@$(DC) exec -T redpanda rpk cluster health 2>/dev/null | sed 's/^/  /' || echo "  unreachable"
	@echo "topics:"
	@$(DC) exec -T redpanda rpk topic list 2>/dev/null | sed 's/^/  /' || echo "  unreachable"
	@echo "disk:"
	@printf '  %-20s %s\n' "root (images)" "$$(df -h / | tail -1 | awk '{print $$4" free ("$$5" used)"}')"
	@printf '  %-20s %s\n' "/mnt/F (volumes)" "$$(df -h /mnt/F | tail -1 | awk '{print $$4" free ("$$5" used)"}')"
	@du -sh $(DATA)/transit-* 2>/dev/null | sed 's|^|  |' || true

lag:  ## Show consumer group lag (the Phase 1 "did it stall overnight" check)
	@$(DC) exec -T redpanda rpk group list 2>/dev/null || echo "no groups yet"

# --- terraform (Phase 5) -----------------------------------------------------
#
# Two roots, applied in order (terraform/connectors/versions.tf says why):
#
#     make tf-apply R=core         topics, bucket, (5C) roles
#     make connect-up              the worker needs core's _connect_* topics
#     make tf-apply R=connectors   the sinks
#
# `make platform` runs the three. On a machine whose resources predate
# Terraform, run `make tf-adopt R=core` and `make tf-adopt R=connectors` ONCE
# instead of the applies; after that, plain tf-apply.
#
# Terraform runs from its pinned image on the compose network, the way dbt
# does, so the endpoints are service names and nothing needs installing on the
# host. -u keeps the files it writes (.terraform/, terraform.tfstate) owned
# by you rather than by root, on a bind mount that would otherwise leave
# root-owned state in the repo.
#
# CREDENTIALS reach it as TF_VAR_* from .env and never as arguments, so they
# stay out of `ps` and shell history. The variables that carry them are
# ephemeral (terraform/core/variables.tf), so they stay out of the state too.
# `-e NAME` with no value passes the variable through from this shell.
# 5C adds its role passwords to TF_PASS.

R ?= core
TF_IMAGE := hashicorp/terraform:1.15.9
# APPLY IS INTERACTIVE BY DEFAULT, and AUTO=1 is for the places that cannot be:
# CI's platform job, and any scripted run. `terraform apply` prints the plan and
# waits for a typed "yes", which is the right default for a change to live
# infrastructure and the wrong one for a runner with no stdin -- there it fails
# with "Apply cancelled" rather than doing anything useful.
#
# tf-drift exists as the review step this skips: run it first, and AUTO=1 applies
# a plan you have already seen. `-auto-approve` does NOT skip the plan, only the
# confirmation.
TF_APPROVE := $(if $(AUTO),-auto-approve,)

# The core root applies ONE RESOURCE AT A TIME. Its Postgres grants collide when
# run concurrently: every table grant rewrites that table's pg_class row, and
# connect_sink and dbt_transform are both granted on the same raw tables, so
# two parallel revoke-then-grant statements hit one row and Postgres aborts one:
#
#   Error: could not execute revoke query: pq: tuple concurrently updated
#   STATEMENT: REVOKE UPDATE,SELECT,INSERT ON TABLE "raw"."prediction_accuracy",...
#
# Found by rehearsing the `platform` CI job on an empty daemon (2026-09-25); on
# this machine the grants were adopted or added a few at a time and never raced.
# The provider's own max_connections = 1 was tried first and did NOT fix it:
# the next run still showed two backends (pids 139 and 145) failing in the same
# second. -parallelism=1 removes the concurrency instead of trusting a setting
# that measurably did not. The cost is the twelve topics applying in sequence
# on a fresh stack; plan and tf-drift only read, and stay parallel.
TF_SERIAL = $(if $(filter core,$(1)),-parallelism=1,)
TF_PASS := TF_VAR_warehouse_db TF_VAR_warehouse_admin_user TF_VAR_warehouse_admin_password \
           TF_VAR_minio_root_user TF_VAR_minio_root_password TF_VAR_raw_bucket \
           TF_VAR_connect_sink_password TF_VAR_static_loader_password \
           TF_VAR_enrichment_password TF_VAR_dbt_transform_password \
           TF_VAR_airflow_ops_password TF_VAR_archive_writer_secret \
           TF_VAR_flink_state_secret
TF_ENV := $(ENV) export TF_VAR_warehouse_db=$$POSTGRES_DB \
  TF_VAR_warehouse_admin_user=$$POSTGRES_USER \
  TF_VAR_warehouse_admin_password=$$POSTGRES_PASSWORD \
  TF_VAR_minio_root_user=$$MINIO_ROOT_USER \
  TF_VAR_minio_root_password=$$MINIO_ROOT_PASSWORD \
  TF_VAR_raw_bucket=$${RAW_BUCKET:-transit-raw} \
  TF_VAR_connect_sink_password=$$CONNECT_SINK_PASSWORD \
  TF_VAR_static_loader_password=$$STATIC_LOADER_PASSWORD \
  TF_VAR_enrichment_password=$$ENRICHMENT_PASSWORD \
  TF_VAR_dbt_transform_password=$$DBT_TRANSFORM_PASSWORD \
  TF_VAR_airflow_ops_password=$$AIRFLOW_OPS_PASSWORD \
  TF_VAR_archive_writer_secret=$$ARCHIVE_WRITER_SECRET \
  TF_VAR_flink_state_secret=$$FLINK_STATE_SECRET;
TF = docker run --rm --network transit-stream_default \
  -u $$(id -u):$$(id -g) -e HOME=/tmp $(foreach v,$(TF_PASS),-e $(v)) \
  -v $(ROOT):/repo -w /repo/terraform/$(1) $(TF_IMAGE)

tf-init:  ## terraform init for one root (R=core|connectors)
	@$(TF_ENV) $(call TF,$(R)) init -input=false -no-color

tf-plan: tf-init  ## terraform plan for one root (R=core|connectors)
	@$(TF_ENV) $(call TF,$(R)) plan -input=false

tf-validate: tf-init  ## terraform validate for one root (R=core|connectors)
	@$(TF_ENV) $(call TF,$(R)) validate -no-color

tf-apply: tf-init  ## terraform apply for one root, after showing the plan (R=...)
	@$(TF_ENV) $(call TF,$(R)) apply -input=false $(call TF_SERIAL,$(R)) $(TF_APPROVE)

tf-adopt: tf-init  ## ONCE per machine: import resources that predate Terraform (R=...)
	@$(TF_ENV) $(call TF,$(R)) apply -input=false -var adopt_existing=true $(call TF_SERIAL,$(R)) $(TF_APPROVE)

# -detailed-exitcode: 0 no changes, 1 error, 2 changes. The 2 is the point, so
# it is turned into a failure with a message rather than passed through as a
# bare exit status. -lock=false because this only reads, and a stale lock from
# an interrupted apply should not stop the drift check from reporting.
tf-drift:  ## Fail if the live stack differs from terraform/ (both roots)
	@for r in core connectors; do \
	  $(MAKE) -s tf-init R=$$r >/dev/null || exit 1; \
	  $(TF_ENV) $(call TF,$$r) plan -input=false -lock=false -detailed-exitcode -no-color >/dev/null; \
	  s=$$?; \
	  if [ $$s -eq 0 ]; then echo "$$r: no drift"; \
	  elif [ $$s -eq 2 ]; then echo "$$r: DRIFT, run 'make tf-plan R=$$r' to see it"; exit 2; \
	  else echo "$$r: plan failed"; exit 1; fi; \
	done

tf-fmt:  ## terraform fmt -check across both roots (what CI runs)
	@docker run --rm -u $$(id -u):$$(id -g) -v $(ROOT):/repo -w /repo/terraform \
	  $(TF_IMAGE) fmt -check -recursive -diff

# fmt -check plus validate for BOTH roots, needing no credentials and no running
# service. `init -backend=false` still downloads the providers named in the lock
# file -- which is what makes it honest for CI, since a lock file that disagrees
# with the declarations fails here -- but it never touches state, so none of
# TF_ENV's variables are needed. That is why this is the one part of the
# terraform lifecycle a validator-only CI job can run.
tf-check:  ## terraform fmt -check + validate for both roots, with no credentials
	@$(MAKE) -s tf-fmt
	@for r in core connectors; do \
	  echo "terraform/$$r:"; \
	  docker run --rm -u $$(id -u):$$(id -g) -e HOME=/tmp -v $(ROOT):/repo \
	    -w /repo/terraform/$$r $(TF_IMAGE) init -backend=false -input=false -no-color >/dev/null || exit 1; \
	  docker run --rm -u $$(id -u):$$(id -g) -e HOME=/tmp -v $(ROOT):/repo \
	    -w /repo/terraform/$$r $(TF_IMAGE) validate -no-color | tail -3; \
	done

platform: $(ROOT)/.env  ## Create everything Terraform owns, in order (core, worker, connectors)
	@$(MAKE) -s tf-apply R=core
	@$(MAKE) -s connect-up
	@$(MAKE) -s tf-apply R=connectors

# --- CI fixture (Phase 5A) ---------------------------------------------------

ci-fixture:  ## Re-export the real-data slice CI's dbt build runs against
	@$(ENV) bash $(ROOT)/tests/fixtures/warehouse/export.sh

# --- CI: the database-backed steps, as targets --------------------------------
#
# These exist so the runner executes the same code this machine does. A step that
# lives only inside ci.yml cannot be run before it is pushed, so its first
# execution is on a runner where debugging costs another push. The contract suites
# check the declarations; these run them.
#
# SPLIT UP because the dbt job needs the same database the validate job only
# checks: `ci-ddl` is up + ddl + down, and `ci-dbt` reuses the middle pieces and
# adds the fixture load and the build.
#
# A THROWAWAY NETWORK rather than a published port. The dbt container has to
# reach this server, and every other endpoint in this project is reached by
# service name, so `ci-pg` on `transit-ci` is the same idiom. A published port
# would additionally make the run depend on a host port being free.

CI_NET  := transit-ci
CI_PG   := ci-pg
CI_USER := ci
CI_PASS := ci
CI_DB   := ci

ci-net:
	@docker network inspect $(CI_NET) >/dev/null 2>&1 || \
	  docker network create $(CI_NET) >/dev/null

ci-pg-up: ci-net  ## CI: start a throwaway PostGIS and wait until it is really ready
	@docker rm -f $(CI_PG) >/dev/null 2>&1 || true
	@docker run -d --name $(CI_PG) --network $(CI_NET) \
	  -e POSTGRES_USER=$(CI_USER) -e POSTGRES_PASSWORD=$(CI_PASS) \
	  -e POSTGRES_DB=$(CI_DB) postgis/postgis:16-3.4 >/dev/null
	@# WAIT FOR THE IMAGE'S OWN INIT TO FINISH, which a socket probe cannot see.
	@# postgis/postgis runs its docker-entrypoint-initdb.d scripts against a
	@# TEMPORARY server that listens on the Unix socket only, then stops it and
	@# starts the real one. Two races came out of that, one after the other:
	@#
	@#  1. pg_isready over the socket answers during the temporary phase, so the
	@#     apply raced the image's own CREATE EXTENSION postgis and lost:
	@#       duplicate key value violates unique constraint "pg_extension_name_index"
	@#     Measured on this machine.
	@#  2. The fix for (1) waited until postgis was VISIBLE, on the assumption that
	@#     the image had then finished. It had not: the extension is created on
	@#     the temporary server, which is then shut down. On a cold runner
	@#     (rehearsed on an empty daemon, 2026-09-25) the DDL started in that gap
	@#     and died mid-file with
	@#       FATAL:  terminating connection due to administrator command
	@#     This machine had passed only by being fast enough to win.
	@#
	@# -h 127.0.0.1 on every probe closes both: the temporary server has no TCP
	@# listener, so a TCP answer can only come from the final server, and every
	@# socket connection after that talks to it too.
	@for i in $$(seq 1 60); do \
	  docker exec $(CI_PG) pg_isready -h 127.0.0.1 -U $(CI_USER) -d $(CI_DB) >/dev/null 2>&1 && break; \
	  sleep 2; \
	done
	@for i in $$(seq 1 30); do \
	  docker exec -e PGPASSWORD=$(CI_PASS) $(CI_PG) psql -h 127.0.0.1 -U $(CI_USER) -d $(CI_DB) -tAc \
	    "select 1 from pg_extension where extname = 'postgis'" 2>/dev/null \
	    | grep -q 1 && break; \
	  sleep 2; \
	done
	@docker exec -e PGPASSWORD=$(CI_PASS) $(CI_PG) psql -h 127.0.0.1 -U $(CI_USER) -d $(CI_DB) -tAc \
	  "select 1 from pg_extension where extname = 'postgis'" | grep -q 1 || \
	  { echo "the final server never answered over TCP with postgis loaded" >&2; exit 1; }
	@echo "$(CI_PG) ready on $(CI_NET)"

ci-pg-ddl:  ## CI: apply every initdb file, then exercise both maintenance functions
	@# ALL SIX, in order, which is the whole point: `psql -f` against a real
	@# server is the only honest syntax check for DDL. There is no offline parser
	@# that agrees with Postgres about partitioning and plpgsql.
	@for f in $(ROOT)/docker/initdb/*.sql; do \
	  echo "applying $$(basename $$f)"; \
	  docker exec -i $(CI_PG) psql -U $(CI_USER) -d $(CI_DB) -v ON_ERROR_STOP=1 -q < $$f || exit 1; \
	done
	@# The functions have to be exercised ON TABLES THAT SURVIVE 06, which drops
	@# three of the Phase 0 raw tables. The scaffold's version called them on
	@# raw.vehicle_positions, which no longer exists by this point.
	@#
	@# ensure_partition runs twice on purpose: Airflow calls it on a schedule and
	@# a second call must not error.
	@docker exec $(CI_PG) psql -U $(CI_USER) -d $(CI_DB) -v ON_ERROR_STOP=1 -tAc \
	  "select raw.ensure_partition('raw.bunching_alerts', current_date);"
	@docker exec $(CI_PG) psql -U $(CI_USER) -d $(CI_DB) -v ON_ERROR_STOP=1 -tAc \
	  "select raw.ensure_partition('raw.bunching_alerts', current_date);"
	@docker exec $(CI_PG) psql -U $(CI_USER) -d $(CI_DB) -v ON_ERROR_STOP=1 -tAc \
	  "select raw.drop_partitions_before('raw.prediction_accuracy', '1999-01-01');"

ci-pg-load:  ## CI: load the committed fixtures, then check them against MANIFEST
	@# gunzip on the HOST side of the pipe. The fixtures are gzipped CSV, one
	@# file per table, columns in table order with a header -- so this is one
	@# \copy per table. Decompressing here means the database image does not have
	@# to carry gunzip, and the load stops at the first error.
	@for f in $(ROOT)/tests/fixtures/warehouse/*.csv.gz; do \
	  t=$$(basename $$f .csv.gz); \
	  echo "loading raw.$$t"; \
	  gunzip -c $$f | docker exec -i $(CI_PG) psql -U $(CI_USER) -d $(CI_DB) \
	    -v ON_ERROR_STOP=1 -q -c "\copy raw.$$t FROM STDIN CSV HEADER" || exit 1; \
	done
	@# AND THEN COUNT WHAT ARRIVED. This is the part that makes the rest mean
	@# something: `dbt build` against an EMPTY warehouse passes almost every test
	@# in this project, because the singular tests return failing rows and an
	@# empty table has none. A truncated load would therefore be a green build
	@# that asserted nothing. MANIFEST is written by the same export that wrote
	@# the files, so the two cannot drift apart silently.
	@while read -r t want; do \
	  got=$$(docker exec $(CI_PG) psql -U $(CI_USER) -d $(CI_DB) -tAc \
	    "select count(*) from raw.$$t"); \
	  if [ "$$got" != "$$want" ]; then \
	    echo "raw.$$t: loaded $$got rows, MANIFEST says $$want" >&2; exit 1; \
	  fi; \
	  printf '  raw.%-28s %8s rows, matches MANIFEST\n' "$$t" "$$got"; \
	done < $(ROOT)/tests/fixtures/warehouse/MANIFEST

ci-pg-down:
	@docker rm -f $(CI_PG) >/dev/null 2>&1 || true
	@echo "$(CI_PG) removed"

# The two entry points, in the sequence the database has to be built up in.
# Written as explicit $(MAKE) calls rather than prerequisites, because the dbt
# build has to happen BETWEEN load and down, and that ordering cannot be a
# dependency. `platform` above does the same thing for the same reason.
#
# A RED dbt BUILD LEAVES THE CONTAINER UP ON PURPOSE, so its loaded fixtures can
# be inspected with psql. `make ci-pg-down` removes it, and the next ci-pg-up
# removes it anyway.
ci-ddl:  ## CI: throwaway Postgres, all six initdb files, both functions, cleaned up
	@$(MAKE) -s ci-pg-up
	@$(MAKE) -s ci-pg-ddl
	@$(MAKE) -s ci-pg-down
	@echo "six initdb files applied; both maintenance functions callable"

# The dbt container as CI must run it: on the throwaway network, reaching the
# throwaway database by name, with credentials passed inline. That last part is
# why this is not DBT_RUN: `make dbt` deliberately sources .env, and a CI
# checkout has no .env and should not need one. The credentials are DBT_USER and
# DBT_PASSWORD, which is what profiles.yml reads since 5C; as the CI SUPERUSER,
# deliberately, because this warehouse is a fresh PostGIS with no roles at all:
# nothing in this target runs Terraform, so there is no dbt_transform to be.
CI_DBT_RUN = docker run --rm --network $(CI_NET) \
	  -v $(ROOT)/dbt:/dbt -e DBT_PROFILES_DIR=/dbt -e DBT_USE_COLORS=false \
	  -e DBT_HOST=$(CI_PG) -e DBT_PORT=5432 \
	  -e DBT_USER=$(CI_USER) -e DBT_PASSWORD=$(CI_PASS) \
	  -e POSTGRES_DB=$(CI_DB) $(DBT_IMAGE)

ci-dbt: dbt-image  ## CI: dbt build against a throwaway warehouse, with real fixture data
	@$(MAKE) -s ci-pg-up
	@$(MAKE) -s ci-pg-ddl
	@$(MAKE) -s ci-pg-load
	@# require_fixture:true turns on dbt/tests/fixture_is_loaded.sql, the test
	@# about the test data: it asserts the 2026-09-23 night is loaded, that a
	@# stale-timestamp row exists to exercise the stale filter, and that the
	@# stall minute is empty. Without the var that guard is disabled, which is
	@# what makes an unloaded warehouse look green.
	@# stall_mart_readers:[] skips mart_feed_health's GRANT. That model grants
	@# SELECT to airflow_ops so the hourly table swap cannot drop the stall
	@# check's read access, and this warehouse has no roles, so the grant would
	@# fail the build with "role airflow_ops does not exist".
	@# THE TEARDOWN RUNS EVEN WHEN THE BUILD FAILS, and the exit status is
	@# carried through: without that, a failed dbt run left a throwaway PostGIS
	@# (and its port) running, which is the state that makes the NEXT run fail.
	@status=0; $(CI_DBT_RUN) build \
	  --vars '{require_fixture: true, stall_mart_readers: []}' || status=$$?; \
	$(MAKE) -s ci-pg-down; \
	exit $$status

ci-platform: $(ROOT)/.env  ## CI: the clean-clone path end to end (fresh stack to one row)
	@# THE CLEAN-CLONE EXIT CRITERION as one target rather than seven steps in a
	@# workflow, for the same reason ci-ddl and ci-dbt are targets: it can be
	@# rehearsed, and a failure on a runner is reproducible here.
	@#
	@# NOT RUNNABLE BESIDE A RUNNING STACK. Every service in docker-compose.yml
	@# declares a fixed container_name, so a second stack would collide on names
	@# and on the published ports. CI's runner is empty, which is where this runs.
	@#
	@# adopt_existing stays at its default of false: on a fresh stack an import
	@# block for a topic that does not exist is a hard error, so the fresh path
	@# must not adopt. `make tf-adopt R=core` is the once-per-machine other half.
	@#
	@# up -d --wait brings up the core services only (connect and flink are behind
	@# profiles) and blocks until their healthchecks pass, because the next step
	@# talks to the broker and the bucket, and "started" is not "ready".
	@$(DC) up -d --wait
	@$(MAKE) -s tf-apply R=core AUTO=1
	@$(MAKE) -s migrate
	@$(MAKE) -s schema-register-sinks
	@$(MAKE) -s connect-up
	@$(MAKE) -s tf-apply R=connectors AUTO=1
	@# Immediately after the apply, not later: an apply that does not converge in
	@# one pass is a bug in terraform/, not flakiness, and this is the step that
	@# says so.
	@$(MAKE) -s tf-drift
	@# The only step that proves the whole sink path rather than each piece of it:
	@# registry framing, JsonSchemaConverter, TimestampConverter, the upsert and
	@# partition routing, on a stack that did not exist a minute earlier.
	@$(MAKE) -s sink-roundtrip ARGS="--clean"

# --- destructive -------------------------------------------------------------

nuke-warehouse:  ## Drop the warehouse data dir so initdb re-runs (DESTRUCTIVE)
	@echo "This deletes $(DATA)/transit-warehouse entirely."
	@read -p "type 'yes' to continue: " a; [ "$$a" = yes ] || exit 1
	@$(DC) rm -sf warehouse
	@sudo rm -rf $(DATA)/transit-warehouse
	@$(MAKE) dirs

.PHONY: help dirs up connect-up connect-register connect-status down ps logs topic-describe recon cadence \
        tf-init tf-plan tf-apply tf-adopt tf-validate tf-drift tf-fmt tf-check platform ci-fixture ci-ddl ci-dbt ci-platform \
        ci-net ci-pg-up ci-pg-ddl ci-pg-load ci-pg-down \
        feeds produce-dry produce static-status static-load static-hoods \
        schema-gen schema-register schema-status enrich-dry enrich \
        schema-register-sinks \
        pipeline-image stream-up stream-logs \
        flink-up flink-down flink-ui flink-smoke bunching flink-jobs flink-logs resume \
        migrate test contract contract-p2 contract-p3 contract-p3f contract-schema \
        prediction smoke sink-roundtrip psql \
        check lag nuke-warehouse dbt-image dbt dbt-freshness airflow-up airflow-down \
        airflow-logs airflow-check
