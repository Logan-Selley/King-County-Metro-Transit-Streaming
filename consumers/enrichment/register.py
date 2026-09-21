"""Register the enriched schema against the registry. The deploy step.

    python -m consumers.enrichment.register            # register
    python -m consumers.enrichment.register --check    # test compatibility, do not write
    python -m consumers.enrichment.register --set-compat BACKWARD

Separate from every process that produces, because auto.register.schemas is
off (ADR 0005). A producer whose schema is not registered fails loudly at
startup, which correctly identifies a skipped deploy step instead of letting
a developer's local .proto mutate a shared subject as a side effect.

`--check` is the one worth knowing about: it asks the registry whether a
schema WOULD be accepted without writing anything. That is how you find out a
change is breaking before you have made it, rather than after.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from consumers.enrichment.schema import SUBJECT, describe, registry_client
from consumers.enrichment.semantic_diff import diff, summarise

log = logging.getLogger("enrichment.register")

PROTO = Path(__file__).resolve().parents[2] / "schemas" / "enriched_vehicle_position.proto"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="consumers.enrichment.register", description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="test compatibility without registering")
    ap.add_argument("--set-compat", metavar="LEVEL",
                    choices=("BACKWARD", "FORWARD", "FULL", "NONE",
                             "BACKWARD_TRANSITIVE", "FORWARD_TRANSITIVE", "FULL_TRANSITIVE"),
                    help="set the subject's compatibility level")
    ap.add_argument("--proto", type=Path, default=PROTO)
    ap.add_argument("--subject", default=SUBJECT)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    load_dotenv()

    from confluent_kafka.schema_registry import Schema
    client = registry_client()

    if args.set_compat:
        client.set_compatibility(subject_name=args.subject, level=args.set_compat)
        print(f"{args.subject} compatibility -> {args.set_compat}")
        return 0

    if not args.proto.exists():
        print(f"no such proto: {args.proto}", file=sys.stderr)
        return 2

    schema = Schema(args.proto.read_text(), schema_type="PROTOBUF")

    if args.check:
        try:
            ok = client.test_compatibility(subject_name=args.subject, schema=schema)
        except Exception as exc:  # noqa: BLE001 -- subject may not exist yet
            print(f"cannot test compatibility: {exc}")
            return 1
        print(f"registry (wire contract) : {'COMPATIBLE' if ok else 'INCOMPATIBLE'}")

        # The second gate. The registry answers "can an old reader parse these
        # bytes"; this answers "do the values still mean the same thing".
        # ADR 0005 measured why both are needed: a lat/lon number swap, a
        # dropped `optional`, and int32->uint32 are all wire-compatible and
        # all destroy data.
        hazards = 0
        try:
            registered = client.get_latest_version(args.subject).schema.schema_str
            findings = diff(args.proto.read_text(), registered)
            wire, hazards, benign = summarise(findings)
            print(f"semantic diff            : {hazards} hazard(s), {benign} benign")
            for f in findings:
                print(f)
        except Exception as exc:  # noqa: BLE001 -- differ must not mask the registry verdict
            print(f"semantic diff            : unavailable ({exc})")

        # Nonzero if EITHER gate objects, so this works as a CI gate.
        return 0 if (ok and not hazards) else 1

    before = describe(client, args.subject)
    try:
        schema_id = client.register_schema(args.subject, schema)
    except Exception as exc:  # noqa: BLE001
        # The registry's own message names the offending field and the rule it
        # broke, which is more useful than anything this script could add.
        print(f"REJECTED by the registry:\n  {exc}", file=sys.stderr)
        print("\nUnder BACKWARD (ADR 0005) this means the change removes, renumbers,\n"
              "or narrows a field. Adding OPTIONAL fields is the compatible move.",
              file=sys.stderr)
        return 1

    after = describe(registry_client(), args.subject)
    # A FRESH client for the after-read: the client that just mutated the
    # subject can answer get_latest_version from its own cached view, which
    # made this line print "version 1 -> 1" on the run that created version 2
    # while the registry already listed [1, 2].
    print(f"registered {args.subject} -> id {schema_id}")
    if before.get("registered"):
        print(f"  version {before['latest_version']} -> {after['latest_version']}")
    else:
        print(f"  first registration, version {after['latest_version']}")
    print(f"  compatibility: {after['compatibility']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
