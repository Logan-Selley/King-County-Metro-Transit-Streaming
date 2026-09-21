"""Schema Registry wiring for the enriched topic.

The registration CLI lives in register.py. The enrichment logic this schema
serves lives in reference.py and enrich.py.

--- what the registry actually buys ---

Not validation. A producer can serialize anything it likes. What the registry
provides is a CONTRACT with a compatibility rule attached, checked at
registration time rather than at read time:

  * The schema id travels in the message, in a 5-byte prefix ahead of the
    payload (magic byte 0x00, then a 4-byte big-endian id). A consumer reads
    the id, fetches that exact schema, and decodes against it -- so a consumer
    written against v1 keeps working after the producer moves to v2, because
    it decodes v1 records with v1 and v2 records with v2.

  * Registering an INCOMPATIBLE schema fails at deploy time, in the producer
    that is trying to register it, instead of at 3am in whatever downstream
    job first hits an unreadable record.

That second property is the whole point, and it is why ADR 0005 sets
BACKWARD rather than NONE. A registry in NONE mode is a filing cabinet.

--- the subject naming decision ---

TopicNameStrategy: the subject is `<topic>-value`, so one topic carries one
message type. The alternative (RecordNameStrategy) allows several types on a
topic, which this project has no use for and which makes the compatibility
check per-type rather than per-topic. Named here because it is a default that
looks like a non-decision.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from google.protobuf.json_format import ParseDict

log = logging.getLogger("enrichment.schema")

SUBJECT = "enriched.vehicle_positions-value"


def registry_client(url: str | None = None) -> SchemaRegistryClient:
    """Client for Redpanda's built-in registry.

    Redpanda serves the Confluent Schema Registry API on the same binary as
    the broker (ADR 0001), so this is the ordinary confluent-kafka client
    against a non-Confluent server -- the API-compatibility claim tested
    rather than asserted.
    """
    return SchemaRegistryClient(
        {"url": url or os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081")}
    )


def build_serializer(
    client: SchemaRegistryClient,
    message_type,
    auto_register: bool = False,
) -> ProtobufSerializer:
    """Serializer for EnrichedVehiclePosition.

    `auto_register=False` on purpose, and it is the setting worth
    understanding. With auto-registration on, a producer silently registers
    whatever schema its generated class happens to carry -- so a developer
    running a modified .proto locally mutates the shared subject as a side
    effect of starting their process, and the first anyone knows is a
    compatibility error in someone else's consumer.

    Registration is therefore an explicit step (`make schema-register`), and
    a producer whose schema is not already registered fails loudly at startup.
    That is the correct failure: it means the deploy skipped a step.

    `use.deprecated.format=False` selects the current protobuf wire format for
    the message-index prefix. The deprecated format is not interoperable with
    it, and defaulting wrong here produces records that decode as garbage
    rather than failing cleanly.
    """
    return ProtobufSerializer(
        message_type,
        client,
        {
            "auto.register.schemas": auto_register,
            "use.deprecated.format": False,
            # MUST be False against Redpanda's registry, and this is not a
            # style choice. The client looks a protobuf schema up as a
            # base64-encoded FileDescriptorProto; Redpanda resolves that form
            # on the NON-normalizing lookup path and answers 404 on the
            # normalizing one (its canonicalisation is Avro-shaped, so a
            # protobuf schema never matches through it). With this True, every
            # produce fails with "Schema not found (40403)" even though the
            # subject is registered and `make schema-status` lists it.
            "normalize.schemas": False,
        },
    )


def dict_to_message(record: dict, message_type=None):
    """Enriched record dict -> protobuf message.

    Three conversions, each because the JSON record and the .proto disagree on
    purpose:

      * **`None` is dropped, never set.** The optional fields need explicit
        absence: `schedule_deviation_seconds = 0` is a bus exactly on time,
        which is the interesting case, and a null must stay distinguishable
        from it. Setting None would raise; omitting it is what `optional`
        means.
      * **`position_timestamp`**: ISO 8601 string -> epoch seconds (`int64`).
      * **`start_date`**: ISO `2026-09-05` -> `20260905`, as the agency
        publishes it. The realtime wire form is already GTFS-formatted; it
        became ISO only because the producer decoded it to a `date`.

    `ParseDict` is strict about unknown keys, and that strictness is worth
    keeping: it turns "the record grew a field the schema does not have" into
    a loud failure at the boundary, instead of a value that silently never
    reaches the topic. That is the same failure mode as a renamed field
    mapping to an unset optional.

    `message_type` defaults to the generated class, imported lazily because
    `_pb2.py` is build output (`make schema-gen`, gitignored) -- so this
    module stays importable on a fresh clone.
    """
    if message_type is None:
        from schemas.enriched_vehicle_position_pb2 import EnrichedVehiclePosition

        message_type = EnrichedVehiclePosition

    fields = {name: value for name, value in record.items() if value is not None}
    fields["position_timestamp"] = int(
        datetime.fromisoformat(fields["position_timestamp"]).timestamp()
    )
    fields["start_date"] = fields["start_date"].replace("-", "")

    message = message_type()
    ParseDict(fields, message)
    return message


def register(client: SchemaRegistryClient, proto_path: str, subject: str = SUBJECT) -> int:
    """Register a .proto against a subject. Returns the schema id.

    Raises SchemaRegistryError if the registry rejects it -- which, under
    BACKWARD compatibility, is exactly what should happen when someone
    removes or renumbers a field. Do not catch it here: the caller is a CLI
    that should print the registry's own explanation and exit nonzero, since
    the message the registry returns is more useful than anything this
    function could say about it.
    """
    from confluent_kafka.schema_registry import Schema

    with open(proto_path) as fh:
        schema = Schema(fh.read(), schema_type="PROTOBUF")

    schema_id = client.register_schema(subject, schema)
    log.info("registered %s -> schema id %s", subject, schema_id)
    return schema_id


def compatibility(client: SchemaRegistryClient, subject: str = SUBJECT) -> str:
    """The subject's compatibility level, falling back to the global default.

    get_compatibility raises when a subject has no explicit override, which
    is a normal state rather than an error -- the subject simply inherits.
    """
    try:
        return client.get_compatibility(subject_name=subject)
    except Exception:  # noqa: BLE001 -- any registry error means "inherits"
        return client.get_compatibility()


def describe(client: SchemaRegistryClient, subject: str = SUBJECT) -> dict:
    """Everything worth printing about a subject. Used by `make schema-status`."""
    try:
        versions = client.get_versions(subject)
    except Exception:  # noqa: BLE001 -- subject not registered yet
        return {"subject": subject, "registered": False}

    latest = client.get_latest_version(subject)
    return {
        "subject": subject,
        "registered": True,
        "versions": versions,
        "latest_version": latest.version,
        "latest_id": latest.schema_id,
        "compatibility": compatibility(client, subject),
    }
