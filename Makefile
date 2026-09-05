# transit-stream — task runner
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
	@sudo mkdir -p $(DATA)/transit-warehouse $(DATA)/transit-redpanda $(DATA)/transit-minio
	@sudo chown -R 999:999 $(DATA)/transit-warehouse && sudo chmod 700 $(DATA)/transit-warehouse
	@sudo chown -R 101:101 $(DATA)/transit-redpanda
	@sudo chown -R $$(id -u):$$(id -g) $(DATA)/transit-minio
	@echo "data directories ready under $(DATA)"

up: $(ROOT)/.env  ## Start the core stack (redpanda, console, warehouse, minio)
	@$(DC) up -d
	@echo
	@echo "  console    http://localhost:$${CONSOLE_PORT:-8085}"
	@echo "  minio      http://localhost:$${MINIO_CONSOLE_PORT:-9001}"
	@echo "  kafka      localhost:$${REDPANDA_KAFKA_PORT:-19092}"
	@echo "  registry   http://localhost:$${REDPANDA_REGISTRY_PORT:-18081}"
	@echo "  warehouse  localhost:$${WAREHOUSE_PORT:-5434}"

connect-up: $(ROOT)/.env  ## Start Kafka Connect too (~1.5GB image, see compose)
	@$(DC) --profile connect up -d

down:  ## Stop the stack (data directories are preserved)
	@$(DC) --profile connect down

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
	@# One DLQ topic per feed: run.py routes with f"dlq.{spec.name}", so every
	@# feed in feeds.py needs its topic here, or the first DLQ produce would
	@# auto-create it with broker defaults.
	@$(DC) exec -T redpanda rpk topic create dlq.vehicle_positions -p 1 -r 1 || true
	@$(DC) exec -T redpanda rpk topic create dlq.trip_updates -p 1 -r 1 || true
	@$(DC) exec -T redpanda rpk topic create dlq.service_alerts -p 1 -r 1 || true
	@echo
	@$(DC) exec -T redpanda rpk topic list

topic-describe:  ## Show config for one topic (T=raw.service_alerts)
	@test -n "$(T)" || { echo "usage: make topic-describe T=raw.service_alerts"; exit 2; }
	@$(DC) exec -T redpanda rpk topic describe $(T) -c

# --- reconnaissance (Phase 0) ------------------------------------------------

recon:  ## Phase 0 snapshot — sizes, entity counts, field population census
	@$(ENV) $(PY) $(ROOT)/recon/probe.py snapshot

# --- producer (Phase 1) ------------------------------------------------------

feeds:  ## Show the feed manifest (poll intervals, formats, keys, topics)
	@$(ENV) $(PY) -m producer.run --list

produce-dry:  ## One tick per feed, fetch+archive+decode, publish nothing
	@$(ENV) $(PY) -m producer.run --once --dry-run -v

produce:  ## Run the producer (D=24h to bound it; default runs until stopped)
	@$(ENV) $(PY) -m producer.run $(if $(D),--duration $(D))

cadence:  ## Phase 0 cadence — measure real refresh interval (M=minutes)
	@$(ENV) $(PY) $(ROOT)/recon/probe.py cadence -m $(or $(M),10)

# --- tests -------------------------------------------------------------------

test:  ## Run the wire-semantics tests (no stack, no network needed)
	@$(PY) -m pytest $(ROOT)/tests -q

# The producer spec. Fails until the stubs in producer/ are implemented --
# that is the point. Excluded from `make test` and CI so work in progress
# does not show up as a broken build. K=dedupe to narrow.
contract:  ## Run the producer contract tests (the Phase 1 spec)
	@$(PY) -m pytest $(ROOT)/tests/test_producer_contract.py -m contract \
	  $(if $(K),-k $(K)) -v

smoke:  ## Produce + consume a protobuf round trip against the running stack
	@$(ENV) $(PY) $(ROOT)/tests/smoke_roundtrip.py

# --- diagnostics -------------------------------------------------------------

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

.PHONY: help dirs up connect-up down ps logs topics topic-describe recon cadence \
        feeds produce-dry produce test contract smoke psql check lag nuke-warehouse
