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

dirs:  ## Create + chown the bind-mounted data directories (needs sudo, once)
	@# postgres runs as uid 999 and checks PGDATA ownership against its own
	@# uid before initdb; redpanda runs as uid 101. Getting either wrong shows
	@# up as a container that starts, logs a permission error, and exits 1.
	@# minio runs as root in its image, so its directory needs no chown.
	@# chown -R, not a plain chown. The directory alone is not enough once a
	@# container has written into it: any content created under a different
	@# uid (a stack brought up with a `user:` override, a volume moved between
	@# machines) stays unreadable and postgres fails AFTER initdb rather than
	@# before, which is a much more confusing failure.
	@sudo mkdir -p $(DATA)/transit-warehouse $(DATA)/transit-redpanda $(DATA)/transit-minio $(DATA)/transit-airflow-db
	@sudo chown -R 999:999 $(DATA)/transit-warehouse && sudo chmod 700 $(DATA)/transit-warehouse
	@sudo chown -R 999:999 $(DATA)/transit-airflow-db
	@sudo chown -R 101:101 $(DATA)/transit-redpanda
	@sudo chown -R $$(id -u):$$(id -g) $(DATA)/transit-minio
	@# Airflow's task logs. The only bind mount that lives INSIDE the repo rather
	@# than under $(DATA) (docker-compose.airflow.yml), and it needs a chown like
	@# the rest: the container runs as 50000:0, and a root-owned 755 directory
	@# makes it die at startup with
	@#
	@#   ValueError: Unable to configure handler 'processor'
	@#
	@# before it can even migrate its own database, which is a log-permission
	@# error wearing a logging-config error's clothes. Added 2026-09-23, when
	@# `make airflow-up` was found to have never produced a running Airflow on
	@# this machine.
	@sudo mkdir -p $(ROOT)/airflow/logs
	@sudo chown -R 50000:0 $(ROOT)/airflow/logs
	@echo "data directories ready under $(DATA)"

up: $(ROOT)/.env  ## Start the core stack (redpanda, console, warehouse, minio)
	@$(DC) up -d
	@echo
	@echo "  console    http://localhost:$${CONSOLE_PORT:-8085}"
	@echo "  minio      http://localhost:$${MINIO_CONSOLE_PORT:-9001}"
	@echo "  kafka      localhost:$${REDPANDA_KAFKA_PORT:-19092}"
	@echo "  registry   http://localhost:$${REDPANDA_REGISTRY_PORT:-18081}"
	@echo "  warehouse  localhost:$${WAREHOUSE_PORT:-5434}"

connect-up: $(ROOT)/.env  ## Build + start Kafka Connect (see docker/Dockerfile.connect)
	@$(DC) --profile connect up -d --build connect

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
# Created explicitly rather than by auto-creation, because auto-created topics
# get the broker default partition count and cleanup policy -- which would
# silently give the alerts topic delete-retention instead of compaction, and
# the compaction design is one of the things this project exists to show.
#
# Partition counts are not throughput-driven; at these volumes one partition
# would keep up. They are there so consumer-group rebalancing and per-key
# ordering are demonstrable at all, which needs more than one.
#
# Retention is grounded in the Phase 0 measurements in docs/findings.md.

topics: ## Create the Kafka topics with their intended configs
	@$(DC) exec -T redpanda rpk topic create raw.vehicle_positions \
	  -p 3 -r 1 -c retention.ms=604800000 -c cleanup.policy=delete || true
	@$(DC) exec -T redpanda rpk topic create raw.trip_updates \
	  -p 6 -r 1 -c retention.ms=259200000 -c cleanup.policy=delete || true
	@# COMPACTED, keyed by alert_id: the topic holds current alert state, not a
	@# history of every poll restating the same 53 alerts.
	@$(DC) exec -T redpanda rpk topic create raw.service_alerts \
	  -p 1 -r 1 -c cleanup.policy=compact -c min.cleanable.dirty.ratio=0.1 || true
	@$(DC) exec -T redpanda rpk topic create enriched.vehicle_positions \
	  -p 3 -r 1 -c retention.ms=604800000 || true
	@$(DC) exec -T redpanda rpk topic create alerts.bunching \
	  -p 1 -r 1 -c retention.ms=2592000000 || true
	@# Phase 3F output: one record per (prediction, observed arrival) pair.
	@# 3 partitions because the analysis groups by lead-time bucket and route
	@# rather than reading it in order, so per-key ordering buys nothing here.
	@$(DC) exec -T redpanda rpk topic create analytics.prediction_accuracy \
	  -p 3 -r 1 -c retention.ms=2592000000 || true
	@# Kafka Connect's own state. COMPACTED, and created here rather than by
	@# Connect: Redpanda auto-creates a topic the moment any client touches it,
	@# with the broker default cleanup.policy=delete, and if that happens first
	@# the worker refuses to start ("offset.storage.topic ... is required to
	@# have 'cleanup.policy=compact'"). Measured, not hypothetical.
	@for t in _connect_configs _connect_offsets _connect_status; do \
	  $(DC) exec -T redpanda rpk topic create $$t -p 1 -r 1 -c cleanup.policy=compact || true; done
	@# One DLQ topic per feed: run.py routes with f"dlq.{spec.name}", so every
	@# feed in feeds.py needs its topic here, or the first DLQ produce would
	@# auto-create it with broker defaults.
	@$(DC) exec -T redpanda rpk topic create dlq.vehicle_positions -p 1 -r 1 || true
	@$(DC) exec -T redpanda rpk topic create dlq.trip_updates -p 1 -r 1 || true
	@$(DC) exec -T redpanda rpk topic create dlq.service_alerts -p 1 -r 1 || true
	@echo
	@# `rpk topic create` is a NO-OP on a topic that already exists, and it does
	@# not apply config either -- so a topic recreated by the 4B framing
	@# migration (consumers/reframe_topic.py) would silently lose its retention
	@# window. These re-apply the ones that matter, which also makes this target
	@# the single place topic config lives.
	@$(DC) exec -T redpanda rpk topic alter-config alerts.bunching \
	  --set retention.ms=2592000000 || true
	@$(DC) exec -T redpanda rpk topic alter-config analytics.prediction_accuracy \
	  --set retention.ms=2592000000 || true
	@$(DC) exec -T redpanda rpk topic list

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

# --- analytics layer (Phase 4) -----------------------------------------------
# dbt and Airflow each run in their own image; neither is in the project venv
# and they never share one with each other (docker/Dockerfile.dbt,
# docker-compose.airflow.yml). dbt runs as a container on the compose network,
# the same way Airflow's DockerOperator runs it, so a model that works under
# `make dbt` works under the scheduler.

DBT_IMAGE := transit-dbt:1.11.0
DBT_RUN = $(ENV) docker run --rm --network transit-stream_default \
	  -v $(ROOT)/dbt:/dbt -e DBT_PROFILES_DIR=/dbt -e DBT_USE_COLORS=false \
	  -e POSTGRES_USER -e POSTGRES_PASSWORD -e POSTGRES_DB $(DBT_IMAGE)

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
airflow-check:  ## Import every DAG and check required tasks (runs in the Airflow image)
	@docker run --rm --entrypoint python \
	  -e TRANSIT_HOST_ROOT=$(ROOT) -e POSTGRES_USER=x -e POSTGRES_PASSWORD=x -e POSTGRES_DB=x \
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

# --- destructive -------------------------------------------------------------

nuke-warehouse:  ## Drop the warehouse data dir so initdb re-runs (DESTRUCTIVE)
	@echo "This deletes $(DATA)/transit-warehouse entirely."
	@read -p "type 'yes' to continue: " a; [ "$$a" = yes ] || exit 1
	@$(DC) rm -sf warehouse
	@sudo rm -rf $(DATA)/transit-warehouse
	@$(MAKE) dirs

.PHONY: help dirs up connect-up connect-register connect-status down ps logs topics topic-describe recon cadence \
        feeds produce-dry produce static-status static-load static-hoods \
        schema-gen schema-register schema-status enrich-dry enrich \
        schema-register-sinks \
        pipeline-image stream-up stream-logs \
        flink-up flink-down flink-ui flink-smoke bunching flink-jobs flink-logs resume \
        migrate test contract contract-p2 contract-p3 contract-p3f contract-schema \
        prediction smoke psql \
        check lag nuke-warehouse dbt-image dbt dbt-freshness airflow-up airflow-down \
        airflow-logs airflow-check
