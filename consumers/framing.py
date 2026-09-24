"""Confluent wire framing for the Flink output topics, with no Kafka client.

WHY THIS EXISTS. The JDBC sink needs a schema for every record, and the Flink
jobs write plain JSON, so the connector rejects them on the first one:

    requires records with a non-null Struct value and non-null Struct schema,
    but found record at (topic='alerts.bunching', ...) with a HashMap value and
    null value schema.

ADR 0008's answer is to register a JSON Schema and have the jobs write
Confluent framing, which is five bytes in front of the JSON:

    0x00 | schema id (uint32, big endian) | UTF-8 JSON

Five bytes and no library. The Flink image deliberately has no Kafka client
(ADR 0006), so this uses `urllib` and `struct` from the standard library and
nothing else. That is also why the helper lives here rather than in the project
venv, where `confluent_kafka` would be the obvious tool and would drag a
dependency into the job image.

THE ID IS RESOLVED ONCE, at job submission, not per record. That is what the
Confluent serializers do too (a serializer looks its id up when it is
constructed), and a record carries the id of the schema it was written
against, so a later re-registration does not retroactively change existing
records. The consequence to know: a job submitted before a schema change keeps
writing the OLD id until it is resubmitted, and the registry keeps every
version, so the connector still reads those records correctly.
"""

from __future__ import annotations

import json
import struct
import urllib.request

# The Confluent "this is a framed payload" marker. A Confluent deserializer
# checks this byte and fails cleanly on anything else, which is how a plain
# JSON record is rejected rather than mis-read.
MAGIC = 0x00


def schema_id(registry_url: str, subject: str) -> int:
    """Latest registered schema id for a subject, from the registry REST API.

    Raises rather than returning a sentinel: a job whose schema is not
    registered cannot write a correct record, and the registry's own 404 is the
    clearest way to say so at submit time instead of five bytes of zero at
    runtime.
    """
    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest"
    with urllib.request.urlopen(url, timeout=10) as response:
        return int(json.load(response)["id"])


def frame(payload, sid: int) -> bytearray:
    """Dict or JSON string -> the five-byte header plus UTF-8 JSON.

    Accepts a dict so callers can hand over the record directly, which is one
    fewer place to forget `json.dumps`.

    RETURNS A bytearray, NOT bytes, and that is not a style preference. The
    jobs' streams declare Types.PICKLED_BYTE_ARRAY before the Kafka sink, and
    PyFlink writes a `bytearray` as its raw bytes while it PICKLES a `bytes`
    object. Returning bytes therefore put a pickle protocol 5 header in front of
    every record. Measured on the live topic:

        8005951101000000000000420a01000000000000067b226469726563...

    where 80 05 is PROTO, 95 is FRAME, and the next eight bytes are the length
    of the rest (0x111 = 273 of 284 minus the 11-byte header). The connector
    found no magic byte and failed every record, with errors.tolerance=none
    turning that into a dead task rather than a quiet drop.
    """
    if isinstance(payload, dict):
        payload = json.dumps(payload)
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return bytearray(struct.pack(">bI", MAGIC, sid) + payload)


def unframe(raw: bytes) -> tuple[int, bytes]:
    """The inverse, for tests and for reading these topics by hand.

    Not used by the jobs: they only ever write. Returns bytes even when handed a
    bytearray, so a caller is not left holding whichever of the two the producer
    happened to deliver.
    """
    if len(raw) < 5 or raw[0] != MAGIC:
        raise ValueError(f"not Confluent-framed: first bytes {raw[:5]!r}")
    (sid,) = struct.unpack(">I", raw[1:5])
    return sid, bytes(raw[5:])
