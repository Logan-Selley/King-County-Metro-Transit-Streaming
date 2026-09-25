# The worked example for Phase 5B: adopting live resources into Terraform
# without recreating them, with the configuration staying where it already
# lives.
#
# THE JSON FILES STAY THE SOURCE OF TRUTH. connect/*.json and connect/README.md
# explain every setting, and 4A-4B measured each one into place. Rewriting them
# as HCL would split that explanation from the values. So this reads them:
# one connector per file, named after the file, which is what
# `make connect-register` already did with a curl loop. What Terraform adds is
# what the loop could not do: `plan` shows a config edited behind its back, and
# a connector deleted by hand comes back on the next apply.
#
# ${env:...} NEEDS NO ESCAPING HERE, and that is not an accident. The configs
# carry ${env:POSTGRES_PASSWORD} for Connect's EnvVarConfigProvider (4A). Typed
# into HCL, `${` starts an interpolation and would have to be written `$${`.
# Read through file() + jsondecode(), it is data, never a template, so the
# placeholder reaches the worker verbatim. Measured: all three connectors
# imported with 0 changes.
#
# NO CREDENTIAL REACHES THE STATE, for the same reason. The password in the
# config is the placeholder, not the value, and the worker's REST API returns
# the placeholder too (4A verified that). The 4A decision to use a config
# provider is what makes a local state file safe to keep for this root.
# config_sensitive is deliberately unused: moving the placeholder into it
# showed an in-place update on import and bought nothing, since the value is
# not a secret.

variable "connect_url" {
  description = "Connect worker REST API, as seen from the Terraform container"
  type        = string
  default     = "http://connect:8083"
}

variable "adopt_existing" {
  description = <<-EOT
    true ONCE, on a machine whose connectors were registered before Terraform
    managed them (`make tf-adopt R=connectors`). false everywhere else,
    including CI's clean stack. See the import block below.
  EOT
  type        = bool
  default     = false
}

provider "kafka-connect" {
  url = var.connect_url
}

locals {
  # "enriched_vehicle_positions" => { connector.class = ..., topics = ..., ... }
  connectors = {
    for f in fileset("${path.module}/../../connect", "*.json") :
    trimsuffix(f, ".json") => jsondecode(file("${path.module}/../../connect/${f}"))
  }
}

# ADOPTION IS GATED, because an import block is not a no-op on a stack that
# lacks the object. Measured against a topic that did not exist:
#
#     Error: Cannot import non-existent remote object
#
# So an unconditional import works on this machine and breaks the clean-clone
# stack that Phase 5's exit criterion is about. With the gate, this machine
# adopts once and a fresh stack creates. Leaving the blocks in after adoption
# costs nothing (an import of something already in state is skipped), and
# they document which resources predate Terraform.
import {
  for_each = var.adopt_existing ? local.connectors : {}
  to       = kafka-connect_connector.sink[each.key]
  id       = each.key
}

resource "kafka-connect_connector" "sink" {
  for_each = local.connectors

  name = each.key
  # The worker stores the name inside the config as well, so the imported
  # object has it there. Leaving it out of this map shows a diff on every plan.
  config = merge(each.value, { name = each.key })
}

output "connectors" {
  description = "What Terraform manages, by name"
  value       = sort(keys(local.connectors))
}
