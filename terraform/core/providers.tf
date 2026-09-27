# Every endpoint is a COMPOSE SERVICE NAME, because Terraform runs in its own
# container on transit-stream_default (see the Makefile's TF variable), the
# same way dbt and the Flink submitter do. localhost would be the Terraform
# container itself, the same trap as BOOTSTRAP in the Flink jobs, DBT_HOST,
# and the submitter's -m flag.

provider "kafka" {
  bootstrap_servers = [var.kafka_bootstrap]
  # The internal listener is plaintext. Without this the provider attempts a
  # TLS handshake against it and times out rather than failing clearly.
  tls_enabled = false
}

provider "minio" {
  minio_server   = var.minio_endpoint
  minio_user     = var.minio_root_user
  minio_password = var.minio_root_password
  minio_ssl      = false
}

provider "postgresql" {
  host     = var.warehouse_host
  port     = var.warehouse_port
  database = var.warehouse_db
  username = var.warehouse_admin_user
  password = var.warehouse_admin_password
  # The warehouse listens without TLS on the compose network. The provider's
  # default is "require", which fails the first connection.
  sslmode = "disable"
  # connect_timeout stays at the provider default. A warehouse that is still
  # running initdb refuses connections outright, and that failure is
  # clearer than a long wait.
}
