# Least privilege. The executable half is
# tests/test_privileges_contract.py, which logs in as each role against the
# running warehouse and checks what it can and cannot do.
#
# WHY. Every client of the warehouse logged in as `transit`, the superuser initdb
# created: Connect, dbt, Airflow, the enrichment consumer and the static loader.
# Measured on 2026-09-24: `transit` was the only non-system role and owned raw,
# static, staging and marts. So a typo in a dbt model, a bad connector config or a
# compromised Airflow container could DROP the landing tables that 2.58M rows of
# replay-verified sink output live in. The 4A upsert makes the sink idempotent; it
# does nothing about the sink's credentials being able to drop the table.
#
# WHERE THE VALUES COME FROM. .env, named <ROLE>_PASSWORD and <USER>_SECRET, the
# way every other credential in this project is. The Makefile hands them to the
# container as TF_VAR_* (TF_ENV and TF_PASS), so there is still one place a
# credential is written down.
#
# EVERY CREDENTIAL VARIABLE IS EPHEMERAL. `sensitive` alone only hides a value
# from the terminal; it still lands in terraform.tfstate in plain text. An
# ephemeral variable can be read by a provider block or a write-only argument and
# is never written to the plan or the state, which is what makes the local state
# file acceptable (ADR 0009, rule 4). tests/test_platform_contract.py fails on a
# credential variable that is merely sensitive.
#
# ONE ROTATION VERSION FOR ALL OF THEM. password_wo and secret_wo are only sent
# when their *_wo_version changes, which is the whole point (Terraform cannot
# diff a value it never stored). Bumping var.credential_version re-applies every
# password at once, which is the direction that fails safe: rotating by hand
# without bumping leaves the role's real password and Terraform's view of it
# disagreeing, and nothing would report that.

variable "credential_version" {
  description = <<-EOT
    Bump to force every role password and MinIO secret to be re-applied. A
    write-only value is only sent when its version changes.
  EOT
  type        = string
  default     = "1"
}

variable "connect_sink_password" {
  description = "CONNECT_SINK_PASSWORD from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "static_loader_password" {
  description = "STATIC_LOADER_PASSWORD from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "enrichment_password" {
  description = "ENRICHMENT_PASSWORD from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "dbt_transform_password" {
  description = "DBT_TRANSFORM_PASSWORD from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "airflow_ops_password" {
  description = "AIRFLOW_OPS_PASSWORD from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "archive_writer_secret" {
  description = "ARCHIVE_WRITER_SECRET from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "flink_state_secret" {
  description = "FLINK_STATE_SECRET from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}
#
# THE ROLES, one per client, named for what they do, not for the tool:
#
#   connect_sink    upserts into raw.enriched_vehicle_positions,
#                   raw.bunching_alerts, raw.prediction_accuracy. Nothing else.
#   static_loader   writes static.* (static/load.py: COPY, INSERT, UPDATE and
#                   DELETE on the GTFS tables, and TRUNCATE on neighborhoods,
#                   which is its own privilege, separate from DELETE)
#   enrichment      reads static.*. Writes nothing to the warehouse.
#   dbt_transform   reads raw.* and static.*, owns what it builds in staging
#                   and marts.
#   airflow_ops     runs partition maintenance and reads marts (the stall
#                   check). Nothing else.
#
# And two MinIO users, for the same reason one level down: the producer and both
# Flink jobs used MINIO_ROOT_USER, and neither needs it. The producer writes one
# prefix and Flink reads and writes another, so each gets a user whose policy
# names only its own prefix.
#
#   archive_writer  PutObject under raw/        (the producer)
#   flink_state     read/write under flink-checkpoints/ (JobManager, TaskManager)

# --- the roles ---------------------------------------------------------------
#
# Explicit blocks rather than for_each over a map, the same call topics.tf makes:
# each role's reason sits next to its values. It also sidesteps a real hazard,
# since these carry ephemeral values and a collection is the wrong place for one.
#
# login = true and nothing else. No superuser, no createdb, no createrole, and no
# role memberships, so the only power any of these has is what the grants below
# give it. That is the whole point: the sink could not DROP the table it writes to
# even if the connector's config were wrong.

resource "postgresql_role" "connect_sink" {
  name                = "connect_sink"
  login               = true
  password_wo         = var.connect_sink_password
  password_wo_version = var.credential_version
}

resource "postgresql_role" "static_loader" {
  name                = "static_loader"
  login               = true
  password_wo         = var.static_loader_password
  password_wo_version = var.credential_version
}

resource "postgresql_role" "enrichment" {
  name                = "enrichment"
  login               = true
  password_wo         = var.enrichment_password
  password_wo_version = var.credential_version
}

resource "postgresql_role" "dbt_transform" {
  name                = "dbt_transform"
  login               = true
  password_wo         = var.dbt_transform_password
  password_wo_version = var.credential_version
}

resource "postgresql_role" "airflow_ops" {
  name                = "airflow_ops"
  login               = true
  password_wo         = var.airflow_ops_password
  password_wo_version = var.credential_version
}

# --- grants -------------------------------------------------------------------
#
# TWO SHAPES, and the difference is deliberate. A `postgresql_grant` with
# object_type = "table" and no `objects` means every table in the schema, which is
# right for the read-only roles. The writer gets named objects instead, because
# "INSERT on raw.*" would let a misconfigured connector write into raw.dlq.
#
# WHAT UPSERT ACTUALLY NEEDS. INSERT alone is not enough. The JDBC sink emits
# INSERT ... ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col, and Postgres
# requires SELECT on the columns the statement reads (the conflict target and
# anything named in the update) as well as UPDATE on the table. That is why
# connect_sink's grant is SELECT, INSERT, UPDATE and not just INSERT.
#
# DELETE AND TRUNCATE ARE NEVER GRANTED to any sink. Nothing in this project
# deletes a landed row, and both are privileges a compromised connector should not
# have. tests/test_privileges_contract.py asserts all three absences.
#
# A GAP WORTH KNOWING: a grant with no `objects` covers the tables that exist when
# it is applied. Add a table to raw/ and dbt_transform cannot read it until this
# file is re-applied. That fails loudly (a dbt build stops), not silently, which is
# why it is a note rather than a default-privileges resource.
#
# AND A RACE WHEN REFACTORING THESE. Destroying a broad grant REVOKES its
# privileges, and Terraform sees no dependency between that destroy and the
# narrower creates replacing it, so the two can run in either order. Measured:
# nine tables got their privileges and `static.stops` did not, because its create
# landed before the old grant's revoke. A second apply converged, and the end
# state is stable. On a fresh stack this cannot happen, because everything is a
# create.

resource "postgresql_grant" "connect_sink_usage" {
  database    = var.warehouse_db
  role        = postgresql_role.connect_sink.name
  schema      = "raw"
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "postgresql_grant" "connect_sink_sink_tables" {
  database    = var.warehouse_db
  role        = postgresql_role.connect_sink.name
  schema      = "raw"
  object_type = "table"
  objects     = ["enriched_vehicle_positions", "bunching_alerts", "prediction_accuracy"]
  privileges  = ["SELECT", "INSERT", "UPDATE"]
}

# static_loader writes the GTFS tables, and only those. TRUNCATE is a separate
# privilege from DELETE, and static/load.py needs it in exactly one place: the
# neighborhood layer is replaced wholesale, measured at load.py:325.
resource "postgresql_grant" "static_loader_usage" {
  database    = var.warehouse_db
  role        = postgresql_role.static_loader.name
  schema      = "static"
  object_type = "schema"
  privileges  = ["USAGE"]
}

# PER TABLE, not "all tables in static", and the reason is measured: a
# postgresql_grant is NOT additive. It reads the role's whole ACL on the object and
# reconciles it to exactly the list declared, so a second resource touching the
# same table fights the first forever. "All tables" granting
# SELECT/INSERT/UPDATE/DELETE plus a second resource granting TRUNCATE on
# neighborhoods produced permanent drift, verbatim from the plan:
#
#   ~ privileges = [
#       - "DELETE", - "INSERT", - "SELECT", - "UPDATE", + "TRUNCATE",
#     ]
#
# One resource per table with that table's full list has nothing to fight over,
# and it is tighter at the same time: TRUNCATE lands on the one table
# static/load.py truncates (load.py:325) and nowhere else.
locals {
  # The ten tables 02-static.sql creates, and what static/load.py does to each.
  static_loader_privileges = merge(
    {
      for name in [
        "feed_version", "routes", "trips", "stops", "stop_times",
        "shapes", "shape_lines", "calendar", "calendar_dates",
      ] : name => ["SELECT", "INSERT", "UPDATE", "DELETE"]
    },
    {
      # Replaced wholesale on every reload rather than updated in place.
      neighborhoods = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
    },
  )
}

resource "postgresql_grant" "static_loader_tables" {
  for_each = local.static_loader_privileges

  database    = var.warehouse_db
  role        = postgresql_role.static_loader.name
  schema      = "static"
  object_type = "table"
  objects     = [each.key]
  privileges  = each.value
}

# enrichment reads static.* and writes nothing. It never reads raw.* from Postgres
# at all: its source is the raw topic, over Kafka, which is why the contract test
# pins "cannot select raw.enriched_vehicle_positions" and means it.
resource "postgresql_grant" "enrichment_usage" {
  database    = var.warehouse_db
  role        = postgresql_role.enrichment.name
  schema      = "static"
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "postgresql_grant" "enrichment_static_tables" {
  database    = var.warehouse_db
  role        = postgresql_role.enrichment.name
  schema      = "static"
  object_type = "table"
  privileges  = ["SELECT"]
}

# dbt_transform reads both landing schemas. SELECT on raw.* covers dbt source
# freshness as well as the models, because freshness reads the raw tables rather
# than a staging view (access.tf 3e). It owns staging and marts outright, which is
# a migration rather than a grant: docker/initdb/07-privileges.sql.
resource "postgresql_grant" "dbt_transform_raw" {
  database    = var.warehouse_db
  role        = postgresql_role.dbt_transform.name
  schema      = "raw"
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "postgresql_grant" "dbt_transform_raw_tables" {
  database    = var.warehouse_db
  role        = postgresql_role.dbt_transform.name
  schema      = "raw"
  object_type = "table"
  # NAMED PARENTS, not "all tables in raw". The obvious version grants on every
  # table in the schema, which includes every daily PARTITION, and
  # transit_partitions adds one a day: the recorded set would go stale daily and
  # `tf-drift` would report drift at midnight for a change nothing made.
  #
  # Coverage is not lost by naming parents: privileges are checked on the parent
  # when a row is routed into a partition, which
  # tests/test_privileges_contract.py proves for connect_sink by inserting into a
  # partition created after the grant.
  objects    = ["enriched_vehicle_positions", "bunching_alerts", "prediction_accuracy", "dlq"]
  privileges = ["SELECT"]
}

resource "postgresql_grant" "dbt_transform_static" {
  database    = var.warehouse_db
  role        = postgresql_role.dbt_transform.name
  schema      = "static"
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "postgresql_grant" "dbt_transform_static_tables" {
  database    = var.warehouse_db
  role        = postgresql_role.dbt_transform.name
  schema      = "static"
  object_type = "table"
  privileges  = ["SELECT"]
}

# airflow_ops runs partition maintenance and reads the one mart the stall check
# needs. The TABLE grant for that mart is deliberately NOT here.
#
# mart_feed_health is a dbt table materialization, which builds a new relation and
# swaps it in, so a grant declared here is revoked out from under the mart by the
# next dbt build, every hour at :15. Measured on 2026-09-25: the relation's relacl
# was empty, airflow_ops could not read it, and tf-drift reported the missing
# SELECT. Applying again restores it only until the next :15.
#
# So dbt owns that grant, because dbt owns the table: the model declares
# grants={'select': ...}, which dbt re-applies after every build. One owner per
# grant is the rule. What stays here is the schema-level fact, which survives the
# swaps.
resource "postgresql_grant" "airflow_ops_marts" {
  database    = var.warehouse_db
  role        = postgresql_role.airflow_ops.name
  schema      = "marts"
  object_type = "schema"
  privileges  = ["USAGE"]
}

# USAGE on raw, and this one is easy to miss: EXECUTE on a function in a schema is
# unusable without USAGE on that schema, because resolving `raw.ensure_partition`
# is itself a schema privilege. Without this the functions are granted and every
# call fails with "permission denied for schema raw".
# airflow_ops still gets no table privilege in raw: USAGE lets it name objects
# there, not read them.
resource "postgresql_grant" "airflow_ops_raw_usage" {
  database    = var.warehouse_db
  role        = postgresql_role.airflow_ops.name
  schema      = "raw"
  object_type = "schema"
  privileges  = ["USAGE"]
}

# EXECUTE on the two maintenance functions, which are SECURITY DEFINER with EXECUTE
# revoked from PUBLIC in docker/initdb/01-schema.sql and 04-retention.sql. This is
# the grant that lets a role owning nothing in raw create and drop partitions
# there, and the reason those functions run with definer rights at all (3c).
resource "postgresql_grant" "airflow_ops_maintenance" {
  database    = var.warehouse_db
  role        = postgresql_role.airflow_ops.name
  schema      = "raw"
  object_type = "function"
  objects     = ["ensure_partition(text,date)", "drop_partitions_before(text,date)"]
  privileges  = ["EXECUTE"]
}

# --- MinIO users --------------------------------------------------------------
#
# Same argument as the roles, one level down. The producer needs to write objects
# under raw/ and to know whether the bucket exists (bucket_exists is a HEAD on the
# bucket, so ListBucket covers it); it never reads an object back. Flink needs
# read, write, delete and list under flink-checkpoints/, because a checkpoint is
# written, read for recovery, and deleted when it is superseded.
#
# Policies are jsonencode'd rather than written as heredoc JSON, so the prefix in
# the rule and the bucket in the ARN cannot drift apart.

# The bucket-level actions, and GetBucketLocation is not optional: the minio
# client's bucket_exists() resolves the bucket's region first, so a policy with
# ListBucket alone fails there with
#   AccessDenied ... resource: /transit-raw
# before put_object is ever reached. Measured on the producer's first run as
# archive_writer, 2026-09-25.
resource "minio_iam_policy" "archive_writer" {
  name = "archive-writer"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = ["arn:aws:s3:::${var.raw_bucket}"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.raw_bucket}/raw/*"]
      },
    ]
  })
}

resource "minio_iam_user" "archive_writer" {
  name              = "archive_writer"
  secret_wo         = var.archive_writer_secret
  secret_wo_version = tonumber(var.credential_version)
}

resource "minio_iam_user_policy_attachment" "archive_writer" {
  user_name   = minio_iam_user.archive_writer.name
  policy_name = minio_iam_policy.archive_writer.name
}

resource "minio_iam_policy" "flink_state" {
  name = "flink-state"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = ["arn:aws:s3:::${var.raw_bucket}"]
      },
      {
        # Flink's S3 filesystem lists, reads, writes and deletes checkpoints, and
        # uploads in parts, so the multipart actions come with it.
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
        ]
        Resource = ["arn:aws:s3:::${var.raw_bucket}/flink-checkpoints/*"]
      },
    ]
  })
}

resource "minio_iam_user" "flink_state" {
  name              = "flink_state"
  secret_wo         = var.flink_state_secret
  secret_wo_version = tonumber(var.credential_version)
}

resource "minio_iam_user_policy_attachment" "flink_state" {
  user_name   = minio_iam_user.flink_state.name
  policy_name = minio_iam_policy.flink_state.name
}
#
