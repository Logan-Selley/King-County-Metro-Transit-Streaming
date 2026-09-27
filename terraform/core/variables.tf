# The endpoint, name and credential variables for the core root. The per-role
# passwords live in access.tf with the roles that use them, so a machine that
# has not created the roles is not asked for passwords it has no use for.
#
# WHERE VALUES COME FROM. Nothing here has a tfvars file. The Makefile reads
# .env and hands each value to the container as TF_VAR_<name>, so .env stays
# the one place a credential is written down, exactly as it is for compose,
# Connect (EnvVarConfigProvider) and dbt (profiles.yml). *.tfvars is
# gitignored anyway, but not having one at all removes the temptation.
#
# EPHEMERAL on every credential. An ephemeral variable can be used by a
# provider block or a write-only argument and is never written to the plan or
# the state. A merely `sensitive` one is only hidden from the terminal: it
# still lands in terraform.tfstate in plain text.

# --- endpoints (compose service names; see providers.tf) ---------------------

variable "kafka_bootstrap" {
  description = "Redpanda's internal Kafka listener"
  type        = string
  default     = "redpanda:9092"
}

variable "minio_endpoint" {
  description = "MinIO's S3 API, host:port, no scheme"
  type        = string
  default     = "minio:9000"
}

variable "warehouse_host" {
  type    = string
  default = "warehouse"
}

variable "warehouse_port" {
  type    = number
  default = 5432
}

# --- names (not secret) ------------------------------------------------------

variable "warehouse_db" {
  description = "POSTGRES_DB from .env"
  type        = string
}

variable "warehouse_admin_user" {
  description = "POSTGRES_USER from .env: the superuser initdb created"
  type        = string
}

variable "minio_root_user" {
  description = "MINIO_ROOT_USER from .env"
  type        = string
}

variable "raw_bucket" {
  description = "RAW_BUCKET from .env; compose defaults it to transit-raw"
  type        = string
  default     = "transit-raw"
}

# --- credentials -------------------------------------------------------------

variable "warehouse_admin_password" {
  description = "POSTGRES_PASSWORD from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "minio_root_password" {
  description = "MINIO_ROOT_PASSWORD from .env"
  type        = string
  sensitive   = true
  ephemeral   = true
}

# --- adoption ----------------------------------------------------------------

variable "adopt_existing" {
  description = <<-EOT
    true ONCE, on a machine whose topics and bucket predate Terraform
    (`make tf-adopt R=core`). false on a fresh stack, where an import block
    for an object that does not exist is a hard error. connectors/connectors.tf
    has the measurement.
  EOT
  type        = bool
  default     = false
}
