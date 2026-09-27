# The MinIO bucket.
#
# WHY TERRAFORM OWNS IT. `mc mb --ignore-existing` creates a bucket if absent
# and says nothing about configuration, which is the same shape as `rpk topic
# create`. The bucket has configuration worth managing (the lifecycle below),
# and one owner for it.

import {
  for_each = var.adopt_existing ? toset([var.raw_bucket]) : toset([])
  to       = minio_s3_bucket.raw
  id       = each.value
}

resource "minio_s3_bucket" "raw" {
  bucket = var.raw_bucket

  # Two prefixes live here, and both matter:
  #   raw/                every feed payload the producer fetched, the replay's
  #                       source
  #   flink-checkpoints/  both jobs' state
  # Destroying the bucket destroys both. force_destroy stays at its default
  # (false), so even without prevent_destroy the provider refuses a non-empty
  # bucket; the lifecycle block makes the plan refuse before that.
  lifecycle {
    prevent_destroy = true
  }
}

# Abandoned checkpoint directories expire.
#
# THE PROBLEM, measured 2026-09-24: flink-checkpoints/ held 272 MiB across SIX
# job-id directories while exactly two jobs were running. The other four were
# left by earlier submissions. Every resubmission gets a new job id, and
# RETAIN_ON_CANCELLATION (consumers/checkpointing.py) deliberately keeps the old
# directory so a cancelled job can be resumed by hand (flink-submit.sh shows
# how). Nothing ever removed them, and the machine restarted four times on
# 2026-09-23 alone.
#
# SEVEN DAYS is the answer to "how long does a cancelled job stay hand-resumable
# by hand", and it is also the raw topics' replay window (ADR 0004). Long enough
# that a job cancelled over a weekend is still resumable, short enough that a
# day of restarts does not accumulate.
#
# SAFE BY AGE ONLY BECAUSE OF THE STATE BACKEND, and this note is the point of
# the rule: state.backend is `hashmap` (docker-compose.yml), so every checkpoint
# is a FULL snapshot, only the newest is retained, and a running job's files are
# therefore never older than the 30-second checkpoint interval. With RocksDB
# INCREMENTAL checkpoints that stops being true -- an incremental checkpoint
# references shared files that can have been written days earlier, so an
# age-based rule would delete state a running job still needs. If the backend
# changes, this rule has to change with it.
#
# NO RULE TOUCHES raw/. It is Phase 6's replay source, and it is affordable to
# keep: the whole MinIO directory was 14 GB after ~20 days of archiving
# (~0.7 GB/day) against 2.8 T free on /mnt/F. If that changes it is an ADR, not
# a lifecycle default. tests/test_platform_contract.py asserts both halves of
# this: that flink-checkpoints/ appears here, and that raw/ does not.
resource "minio_ilm_policy" "raw" {
  bucket = minio_s3_bucket.raw.bucket

  rule {
    id     = "expire-abandoned-flink-checkpoints"
    status = "Enabled"
    # The prefix, so the rule cannot see raw/ even by accident.
    filter     = "flink-checkpoints/"
    expiration = "7d"
  }
}
