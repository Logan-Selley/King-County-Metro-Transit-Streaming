# The connectors root: Kafka Connect sinks, and nothing else.
#
# WHY A SECOND ROOT instead of one more file in core/. Connectors are managed
# over the Connect worker's REST API, and the worker cannot start until
# core/ has created its three compacted internal topics (_connect_configs,
# _connect_offsets, _connect_status; see core/topics.tf for the measured
# failure). One root would need the worker to come up halfway through an apply.
# Two roots make the order a fact of the layout:
#
#     make tf-apply R=core         topics, bucket, roles
#     make connect-up              the worker, which now finds its topics
#     make tf-apply R=connectors   the sinks
#
# The versions are pinned exactly for the reason core/versions.tf gives.

terraform {
  required_version = "= 1.15.9"

  required_providers {
    kafka-connect = {
      source  = "Mongey/kafka-connect"
      version = "0.5.0"
    }
  }

  # Local state, gitignored. See core/versions.tf for why that is safe here,
  # which for this root is simpler still: every connector config carries
  # ${env:...} placeholders, never a credential, so the state has none to hold.
  backend "local" {}
}
