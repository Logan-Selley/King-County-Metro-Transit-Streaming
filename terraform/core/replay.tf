# The replay namespace. ADR 0010 has the design; this is the part of it
# that is infrastructure.
#
# WHY A NAMESPACE AND NOT A RUN-ID PER REPLAY. A replay has to be unable to touch
# the live pipeline, and "unable" means the names it writes to are not the live
# names, checked before anything is produced (producer/replay.py refuses any
# topic outside `replay.`). Topics created per run would need creating and
# deleting around every run, outside Terraform, and every forgotten one is a
# topic nobody owns. A fixed namespace is declared once, its config is under the
# same drift check as everything else, and it is RESET between runs
# (`make replay-reset` trims each topic to its high watermark and deletes the
# replay consumer groups) rather than recreated.
#
# PARTITION COUNTS MIRROR THE LIVE TOPICS, for the correctness reason topics.tf
# gives: the bunching job's parallelism must equal its source's partition
# count, and the replay runs the same job with the same parallelism. A replay
# topic with a different count would make the replay drop late data the live
# job does not, and the fidelity check would blame the logic. The contract test
# pins each replay topic to its live counterpart.
#
# ONLY THE VEHICLE-POSITION PATH. Bunching reads enriched positions, enrichment
# reads raw positions, and neither reads trip updates or alerts, so those feeds
# are not replayed. The prediction join is deliberately not replayed at all:
# its state TTL is processing time (consumers/prediction/job.py), so a replay
# running days of data through in minutes would expire nothing and match
# nothing like the live run. That is a scope decision, not a gap.
#
# 7-DAY RETENTION, the raw topics' own window: long enough to rerun a comparison
# the next day, short enough that a forgotten replay ages out by itself.

locals {
  # replay topic -> (partitions, the live topic it mirrors)
  replay_topics = {
    "replay.raw.vehicle_positions"      = { partitions = 3, mirrors = "raw.vehicle_positions" }
    "replay.enriched.vehicle_positions" = { partitions = 3, mirrors = "enriched.vehicle_positions" }
    "replay.dlq.vehicle_positions"      = { partitions = 1, mirrors = "dlq.vehicle_positions" }
    # Two detector outputs, one per logic under comparison. `baseline` is the
    # live logic and exists to prove the replay reproduces the live alerts;
    # `variant` is the changed logic. Generic names, so a later experiment
    # reuses them instead of adding topics.
    "replay.alerts.bunching.baseline" = { partitions = 1, mirrors = "alerts.bunching" }
    "replay.alerts.bunching.variant"  = { partitions = 1, mirrors = "alerts.bunching" }
  }
}

# for_each here, unlike topics.tf's one-block-per-topic: these hold nothing that
# cannot be regenerated from the archive in minutes, so the per-topic WHY
# comments that justified explicit blocks there have nothing to say here, and
# prevent_destroy is deliberately absent for the same reason.
resource "kafka_topic" "replay" {
  for_each = local.replay_topics

  name               = each.key
  partitions         = each.value.partitions
  replication_factor = 1

  config = {
    "retention.ms"   = "604800000"
    "cleanup.policy" = "delete"
  }
}

# --- read-only access to the archive ------------------------------------------
#
# archive_writer (access.tf) can PutObject under raw/ and nothing else, so it
# cannot read the archive back, which is correct for the producer and is why the
# replay needs its own user. The inverse policy: list and get under raw/, no
# put, no delete. That makes the replay physically unable to write into the
# archive it reads, even by mistake: a replay pipeline built with an archive
# attached would fail on its first put rather than duplicate history.

variable "archive_reader_secret" {
  description = "ARCHIVE_READER_SECRET from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}

resource "minio_iam_policy" "archive_reader" {
  name = "archive-reader"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = ["arn:aws:s3:::${var.raw_bucket}"]
        Condition = { StringLike = { "s3:prefix" = ["raw/*", "raw/"] } }
      },
      {
        # A SEPARATE STATEMENT: MinIO rejects s3:prefix on this action, so a
        # single statement carrying both actions fails the apply with
        #   unable to create policy (archive-reader): unsupported condition keys
        #   '[s3:prefix]' used for action 's3:GetBucketLocation'
        # Getting the region is bucket-wide and not prefix-scoped anyway.
        # archive_writer needs the same action for the same reason: minio's
        # bucket_exists() resolves the bucket's region before anything else.
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = ["arn:aws:s3:::${var.raw_bucket}"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["arn:aws:s3:::${var.raw_bucket}/raw/*"]
      },
    ]
  })
}

resource "minio_iam_user" "archive_reader" {
  name              = "archive_reader"
  secret_wo         = var.archive_reader_secret
  secret_wo_version = tonumber(var.credential_version)
}

resource "minio_iam_user_policy_attachment" "archive_reader" {
  user_name   = minio_iam_user.archive_reader.name
  policy_name = minio_iam_policy.archive_reader.name
}
