"""Register the JSON Schemas for the two Flink output topics.

    python -m consumers.sink_schemas              # register both
    python -m consumers.sink_schemas --check      # compatibility, no writes
    python -m consumers.sink_schemas --status     # what the registry holds

This is the counterpart of consumers/enrichment/register.py for the topics the
Flink jobs write. It exists for the same reason: `auto.register.schemas` is off
(ADR 0005), so a job whose schema is not registered fails at submit rather than
mutating a shared subject as a side effect of starting.

WHY THE FLINK TOPICS NEED SCHEMAS AT ALL, when they are plain JSON: the JDBC
sink needs a schema for every record and rejects a schemaless one. ADR 0008
holds this and the connector configs in connect/ are the other half.

Run this in the project venv, not the Flink image: it needs the registry
client, which the job image deliberately does not have (ADR 0006). The jobs
only need the resulting schema ID, which they fetch over REST at submit time
(consumers/framing.py).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

log = logging.getLogger("sink_schemas")

SCHEMAS = Path(__file__).resolve().parents[1] / "schemas" / "json"

# Topic -> schema document. The subject name is Confluent's convention,
# `<topic>-value`, which is what a connector configured with
# `value.converter.schema.registry.url` looks up.
SUBJECTS = {
    "alerts.bunching": SCHEMAS / "bunching_alert.json",
    "analytics.prediction_accuracy": SCHEMAS / "prediction_accuracy.json",
}


def registry_client():
    """A SchemaRegistryClient pointed at .env's SCHEMA_REGISTRY_URL."""
    import os

    from confluent_kafka.schema_registry import SchemaRegistryClient

    return SchemaRegistryClient(
        {"url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081")}
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="consumers.sink_schemas", description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="test compatibility without registering")
    ap.add_argument("--status", action="store_true",
                    help="print what the registry holds for these subjects")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    load_dotenv()

    from confluent_kafka.schema_registry import Schema

    client = registry_client()
    failures = 0

    for topic, path in SUBJECTS.items():
        subject = f"{topic}-value"
        schema = Schema(path.read_text(), schema_type="JSON")

        if args.status:
            try:
                latest = client.get_latest_version(subject)
                print(f"{subject:<40} id {latest.schema_id}, version "
                      f"{latest.version}, {latest.schema.schema_type}")
            except Exception as exc:  # noqa: BLE001 -- not registered yet is normal
                print(f"{subject:<40} not registered ({type(exc).__name__})")
            continue

        if args.check:
            try:
                ok = client.test_compatibility(subject_name=subject, schema=schema)
                print(f"{subject:<40} {'COMPATIBLE' if ok else 'INCOMPATIBLE'}")
                failures += 0 if ok else 1
            except Exception as exc:  # noqa: BLE001
                print(f"{subject:<40} cannot test: {exc}")
                failures += 1
            continue

        try:
            schema_id = client.register_schema(subject, schema)
        except Exception as exc:  # noqa: BLE001
            # The registry's message names the field and the rule, which is
            # more useful than anything this script could add.
            print(f"REJECTED {subject}:\n  {exc}", file=sys.stderr)
            failures += 1
            continue
        print(f"registered {subject} -> id {schema_id}")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
