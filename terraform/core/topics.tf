# Topics. Build step 5B. All twelve, each with the reason for its numbers
# next to them; the contract is in tests/test_platform_contract.py.
#
# WHY TERRAFORM OWNS THESE NOW. `make topics` ran `rpk topic create`, which is
# a no-op on a topic that already exists and does not apply config either.
# The Makefile had to grow a second block of `rpk topic alter-config` calls
# after 4B, because the re-framing migration recreated two topics and they
# silently lost their 30-day retention. Nothing reported the drift. A plan
# does: measured on a throwaway topic, a retention changed behind
# Terraform's back showed up as `~ "retention.ms" = "60000" -> "7200000"`.
#
# THE ONE THING TO KNOW BEFORE EDITING A PARTITION COUNT. Measured, plan only,
# against the real raw.vehicle_positions:
#
#     ~ partitions = 3 -> 2 # forces replacement
#
# Replacement is DELETE the topic and create it again: seven days of data,
# and every consumer group's offsets, gone. Kafka cannot shrink a topic, so
# the provider can only destroy it. `prevent_destroy` turns that plan into an
# error instead of an apply.
#
# Raising the count is the quieter trap. It applies IN PLACE (measured: a
# scratch topic went 1 -> 2 with `update in-place`), and nothing in Terraform
# knows that the Flink jobs' parallelism is coupled to it. bunching/job.py
# sets parallelism equal to enriched.vehicle_positions' partition count,
# because a subtask reading two interleaved partitions shares one watermark
# and drops late data; that was the 29.75% late-drop bug in Phase 3. So a
# partition increase here is a correctness change to a job in another
# directory. The contract test pins the two together so the edit cannot be
# made in one place only.

# --- worked example ---------------------------------------------------------

import {
  # See the adopt_existing variable. Every topic that exists on this machine
  # already needs one of these; on a clean stack none of them runs.
  for_each = var.adopt_existing ? toset(["raw.vehicle_positions"]) : toset([])
  to       = kafka_topic.raw_vehicle_positions
  id       = each.value
}

resource "kafka_topic" "raw_vehicle_positions" {
  name = "raw.vehicle_positions"

  # Keyed by vehicle_id (ADR 0002). Three partitions carry ~1.1M positions a
  # day with room to spare, and three is what the consumers were built for.
  partitions         = 3
  replication_factor = 1

  # EVERY key the live topic reports as DYNAMIC_TOPIC_CONFIG must appear here,
  # with the live value, or the first plan after adoption shows a change. That
  # is the adoption test: `make tf-drift` exits 0. Measured live config for
  # every topic is in tests/test_platform_contract.py.
  config = {
    "retention.ms"   = "604800000" # 7 days, the replay window (ADR 0004)
    "cleanup.policy" = "delete"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# --- the other eleven ---------------------------------------------------------
#
# Same shape as the example for each: an import block gated on adopt_existing,
# then the resource with the reason for its numbers sitting next to them. Those
# reasons used to live in the Makefile's `make topics`; this file replaces it.
#
# PARTITION COUNTS ARE NOT THROUGHPUT-DRIVEN. At these volumes one partition
# would keep up. They exist so consumer-group rebalancing and per-key ordering
# are demonstrable at all, which needs more than one, and so each Flink job's
# parallelism has a partition count to equal (see the coupling note above).
#
# RETENTION IS GROUNDED IN THE PHASE 0 MEASUREMENTS (docs/findings.md) rather
# than picked for tidiness: 7 days is the replay window the raw topics were sized
# for, 3 days matches trip updates' ~30x row volume, and 30 days covers the two
# analysis outputs whose consumers rebuild state from a longer history.

import {
  for_each = var.adopt_existing ? toset(["raw.trip_updates"]) : toset([])
  to       = kafka_topic.raw_trip_updates
  id       = each.value
}

resource "kafka_topic" "raw_trip_updates" {
  name = "raw.trip_updates"

  # Six, because this feed carries ~30x the row volume of positions (measured:
  # 18,623 trip updates against 280 positions in one poll). Three days of
  # retention for the same reason: it is the topic that would actually fill a
  # disk, and nothing downstream reads it a week later.
  partitions         = 6
  replication_factor = 1

  config = {
    "retention.ms"   = "259200000"
    "cleanup.policy" = "delete"
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset(["raw.service_alerts"]) : toset([])
  to       = kafka_topic.raw_service_alerts
  id       = each.value
}

resource "kafka_topic" "raw_service_alerts" {
  name = "raw.service_alerts"

  # COMPACTED, keyed by alert_id: this topic holds current alert state, not a
  # history of every poll restating the same 53 alerts. One partition because
  # the key is what orders it and there are 53 of them.
  #
  # min.cleanable.dirty.ratio is pinned low on purpose: the default 0.5 means a
  # log with mostly-unchanged keys goes a long time between compactions, and
  # this topic is small enough that compacting eagerly costs nothing.
  partitions         = 1
  replication_factor = 1

  config = {
    "cleanup.policy"            = "compact"
    "min.cleanable.dirty.ratio" = "0.1"
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset(["enriched.vehicle_positions"]) : toset([])
  to       = kafka_topic.enriched_vehicle_positions
  id       = each.value
}

resource "kafka_topic" "enriched_vehicle_positions" {
  name = "enriched.vehicle_positions"

  # The boundary between the producer and everything downstream, and the source
  # both Flink jobs read. Three partitions to match raw.vehicle_positions:
  # bunching's parallelism is pinned equal to this count by the contract test,
  # because a subtask spanning two interleaved partitions drops late data.
  #
  # 7 days is the replay window (ADR 0004): long enough that a consumer down for
  # a weekend can catch up from the topic rather than from the archive.
  partitions         = 3
  replication_factor = 1

  config = {
    "retention.ms" = "604800000"
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset(["alerts.bunching"]) : toset([])
  to       = kafka_topic.alerts_bunching
  id       = each.value
}

resource "kafka_topic" "alerts_bunching" {
  name = "alerts.bunching"

  # The detector's output, and the 4B lesson lives here: this topic's retention
  # was silently lost once when a migration recreated it, because `rpk topic
  # create` is a no-op on an existing topic and applies no config. A plan would
  # have shown it.
  #
  # One partition, keyed by route: alerts for one route stay ordered, and there
  # are a few thousand of them a day.
  #
  # 30 days, matching the prediction topic: the two analysis outputs are the
  # ones whose consumers rebuild state from a longer history than the raw feeds.
  partitions         = 1
  replication_factor = 1

  config = {
    "retention.ms" = "2592000000"
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset(["analytics.prediction_accuracy"]) : toset([])
  to       = kafka_topic.analytics_prediction_accuracy
  id       = each.value
}

resource "kafka_topic" "analytics_prediction_accuracy" {
  name = "analytics.prediction_accuracy"

  # Phase 3F's output: one record per (prediction, observed arrival) pair, ~1.3M
  # of them from a day of the live stack. Three partitions because the analysis
  # groups by lead-time bucket and route rather than reading in order, so per-key
  # ordering buys nothing here and the consumer's parallelism can use all three.
  partitions         = 3
  replication_factor = 1

  config = {
    "retention.ms" = "2592000000"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# Kafka Connect's own state: config, offsets and status storage.
#
# CREATED HERE RATHER THAN BY CONNECT, and that ordering is load-bearing.
# Redpanda auto-creates a topic the moment any client touches it, with the
# broker default cleanup.policy=delete. If that happens before Connect starts,
# the worker refuses to start with "offset.storage.topic ... is required to have
# 'cleanup.policy=compact'". Measured, not hypothetical, and it is why these
# three are in this root: the connectors root cannot be applied until the worker
# is up, and the worker cannot start until these exist.
#
# This is also the reason there are two roots at all (ADR 0009, rule 5).
import {
  for_each = var.adopt_existing ? toset(["_connect_configs"]) : toset([])
  to       = kafka_topic.connect_configs
  id       = each.value
}

resource "kafka_topic" "connect_configs" {
  name               = "_connect_configs"
  partitions         = 1
  replication_factor = 1

  config = {
    "cleanup.policy" = "compact"
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset(["_connect_offsets"]) : toset([])
  to       = kafka_topic.connect_offsets
  id       = each.value
}

resource "kafka_topic" "connect_offsets" {
  name               = "_connect_offsets"
  partitions         = 1
  replication_factor = 1

  config = {
    "cleanup.policy" = "compact"
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset(["_connect_status"]) : toset([])
  to       = kafka_topic.connect_status
  id       = each.value
}

resource "kafka_topic" "connect_status" {
  name               = "_connect_status"
  partitions         = 1
  replication_factor = 1

  config = {
    "cleanup.policy" = "compact"
  }

  lifecycle {
    prevent_destroy = true
  }
}

# One DLQ topic per feed. producer/run.py routes with f"dlq.{spec.name}", so
# every feed in feeds.py needs its topic here, or the first DLQ produce
# auto-creates one with broker defaults (the same auto-create race the Connect
# topics above exist to lose).
#
# NO `config` ARGUMENT, deliberately. These carry no overrides live, and an empty
# map may not read back the same way an absent one does -- which would show as a
# phantom change on every plan. The contract test compares declared against live
# and would catch it either way.
#
# prevent_destroy even though they hold nothing today: a DLQ topic recreated
# empty looks exactly like a feed with no errors, which is the one signal this
# project relies on being real (transit_health's dlq_report).
import {
  for_each = var.adopt_existing ? toset(["dlq.vehicle_positions"]) : toset([])
  to       = kafka_topic.dlq_vehicle_positions
  id       = each.value
}

resource "kafka_topic" "dlq_vehicle_positions" {
  name               = "dlq.vehicle_positions"
  partitions         = 1
  replication_factor = 1

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset(["dlq.trip_updates"]) : toset([])
  to       = kafka_topic.dlq_trip_updates
  id       = each.value
}

resource "kafka_topic" "dlq_trip_updates" {
  name               = "dlq.trip_updates"
  partitions         = 1
  replication_factor = 1

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = var.adopt_existing ? toset(["dlq.service_alerts"]) : toset([])
  to       = kafka_topic.dlq_service_alerts
  id       = each.value
}

resource "kafka_topic" "dlq_service_alerts" {
  name               = "dlq.service_alerts"
  partitions         = 1
  replication_factor = 1

  lifecycle {
    prevent_destroy = true
  }
}
