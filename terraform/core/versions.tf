# The core root: topics, the MinIO bucket, and the least-privilege roles. Everything the rest of the stack needs to exist
# before it starts. ADR 0009 has the scope and what was left out of it.
#
# PINNED EXACTLY, not `~>`. Terraform's own version matches the image the
# Makefile runs (hashicorp/terraform:1.15.9), and every provider is pinned
# to the version the ADR's prototype was measured against. A provider minor
# release can change how an imported object reads back, which shows up as a
# plan full of phantom changes against resources nobody touched. The lock
# file (.terraform.lock.hcl) is committed for the same reason.
#
# 1.15 IS NOT JUST "LATEST". Two features this root depends on are recent:
# ephemeral variables (1.10), which keep the admin passwords out of the plan
# and state, and write-only arguments (1.11), which let role passwords be set
# without Terraform ever storing them. Measured on the prototype: a role
# created with password_wo logged in with its password, and a grep of the
# state file found the password zero times.

terraform {
  required_version = "= 1.15.9"

  required_providers {
    # Redpanda speaks the Kafka admin API, and this is the provider that uses
    # it. Redpanda's own provider manages Redpanda CLOUD clusters, not a
    # broker you run yourself.
    kafka = {
      source  = "Mongey/kafka"
      version = "0.13.1"
    }
    minio = {
      source  = "aminueza/minio"
      version = "3.43.0"
    }
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "1.27.0"
    }
  }

  # LOCAL STATE, gitignored, and that is a decision rather than a default.
  # Remote state earns its keep with more than one operator or more than one
  # machine, and this stack has one of each. It also has an awkward
  # bootstrap: the obvious remote backend here is MinIO, which is one of the
  # things this root creates. Local state holds no credentials (the
  # variables that carry them are ephemeral, and the role passwords are
  # write-only), so the file is no more sensitive than the topic list.
  backend "local" {}
}
