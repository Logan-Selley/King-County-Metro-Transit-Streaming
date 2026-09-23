"""Confluent-framed protobuf bytes -> plain dict, in both protobuf majors.

`enriched.vehicle_positions` carries protobuf, not JSON. It carried JSON
briefly during the Phase 2 placeholder era, and the Phase 3 scaffold was
written against that: `kafka_source()` used SimpleStringSchema, parse_record
took a `str`, and the timestamp assigner called json.loads. All three were
stale by the time the topic was recreated. This module is the replacement
layer.

--- why not just import schemas/enriched_vehicle_position_pb2.py ---

Because the job cannot. ADR 0006 isolates the Flink image because
apache-flink pins protobuf<6 through apache-beam; the second-order cost is
that the image's runtime is OLDER than the gencode the project generates:

    Flink image protobuf   5.29.6
    project bindings       gencode 7.35.1

    VersionError: Detected incompatible Protobuf Gencode/Runtime versions
    ... Runtime version cannot be older than the linked gencode version.

That is a hard refusal at import, by design: protobuf guarantees forward
compatibility of gencode against newer runtimes, not older ones.

--- what this does instead ---

Builds the message class at runtime from a serialized FileDescriptorSet,
which `make schema-gen` emits next to the bindings from the same protoc
invocation. A FileDescriptorSet is protobuf's own wire format for "here is a
schema", so it is version-agnostic in a way generated Python is not.
Measured: a descriptor emitted by protoc 7.x builds a working class under
runtime 5.29.6, explicit presence intact.

Same source file, same command, so the descriptor cannot drift from the
bindings the enrichment consumer produces with. That was the deciding
argument over generating a second set of bindings inside the image with a
protobuf-5 protoc, which would have added a fourth version to keep in step
(Flink runtime, PyFlink, Kafka connector, and now protoc).

--- why the framing is parsed here rather than by ProtobufDeserializer ---

confluent-kafka is installed in the image, and its ProtobufDeserializer does
exactly this. It is also unimportable there:

    ModuleNotFoundError: No module named 'httpx'       # then authlib, ...

The registry client is behind the `[schemaregistry]` extra and pulls httpx
plus authlib's OAuth machinery. Adding all of that to a job whose actual need
is "skip six bytes" is the wrong trade. The framing below is Confluent's
published wire format and is asserted against live bytes in the contract
suite, so it is pinned by a test rather than by trust.
"""

from __future__ import annotations

import os
import struct

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

# Two homes for the same file: docker-compose mounts ./schemas read-only at
# /opt/jobs/schemas for the job, and it sits in the repo for the venv and the
# contract tests. Resolved in order rather than hardcoded to the image path,
# because a module that only works inside the container is a module no test
# can reach -- which is the whole failure mode consumers/bunching/detect.py
# was split out to avoid.
_REPO_SCHEMAS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "schemas",
    "enriched_vehicle_position.desc",
)
DESCRIPTOR_PATH = os.environ.get("ENRICHED_DESCRIPTOR") or next(
    (p for p in ("/opt/jobs/schemas/enriched_vehicle_position.desc", _REPO_SCHEMAS)
     if os.path.exists(p)),
    _REPO_SCHEMAS,
)
MESSAGE_NAME = "transit.EnrichedVehiclePosition"

_message_class = None


def message_class(path: str | None = None):
    """The EnrichedVehiclePosition class, built from the descriptor set.

    Cached, because a TaskManager builds this once per slot and then decodes
    tens of thousands of records through it. Rebuilding per record also
    re-registers the type in a fresh pool, which is slow and leaks.
    """
    global _message_class
    if _message_class is not None and path is None:
        return _message_class

    with open(path or DESCRIPTOR_PATH, "rb") as fh:
        fds = descriptor_pb2.FileDescriptorSet.FromString(fh.read())

    # A private pool, not descriptor_pool.Default(). The default pool is
    # process-global and raises on a duplicate file name, so a job that ever
    # loads two descriptors -- or reloads after a restart in the same JVM --
    # fails with "A file with this name is already in the pool" rather than
    # anything that points at the cause.
    pool = descriptor_pool.DescriptorPool()
    for f in fds.file:
        pool.Add(f)

    cls = message_factory.GetMessageClass(pool.FindMessageTypeByName(MESSAGE_NAME))
    if path is None:
        _message_class = cls
    return cls


def strip_framing(raw: bytes) -> bytes:
    """Confluent protobuf framing -> the protobuf payload.

    Layout, verified against 3,000 live records on the topic:

        byte  0      magic, always 0
        bytes 1-4    schema id, big-endian uint32   (3, for this subject)
        byte  5+     message-index array, varint-encoded
        rest         the serialized message

    The message-index array is the part that is easy to hardcode and wrong.
    It addresses a nested message inside the .proto, and the spec gives the
    single-message case a one-byte optimisation: a literal 0x00 rather than
    the length-1 array [1, 0]. All 3,000 sampled records use that form,
    because the schema has exactly one top-level message -- but reading the
    general form costs four lines and means a nested message added later
    decodes instead of silently shifting every field by a byte.

    Raises ValueError on anything that is not framed protobuf, including the
    JSON this topic briefly carried, so the caller can route it rather than
    decode garbage.
    """
    if len(raw) < 6:
        raise ValueError(f"too short to be framed protobuf: {len(raw)} bytes")
    if raw[0] != 0:
        raise ValueError(f"bad magic byte {raw[0]:#04x} (expected 0x00)")

    pos = 5
    count, pos = _varint(raw, pos)
    if count != 0:
        # Length-prefixed array: `count` indices follow. count == 0 is the
        # single-message shorthand and consumes nothing further.
        for _ in range(count):
            _, pos = _varint(raw, pos)
    return raw[pos:]


def _varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Read one base-128 varint. Returns (value, new position)."""
    result = shift = 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint in message-index array")
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def to_dict(msg) -> dict:
    """Protobuf message -> dict, with absence preserved.

    Explicit field access rather than json_format.MessageToDict, for two
    reasons that both corrupt this schema specifically:

      * MessageToDict renders int64 as a STRING, because JSON numbers cannot
        hold the full range. `position_timestamp` would arrive as
        "1789935300" and every downstream comparison against a float window
        boundary would fail -- or worse, compare as a string and sort
        lexically. `shape_dist_traveled` is a double and stays a float, so
        the corruption would be partial and easy to miss.

      * Unset optional fields would need `including_default_value_fields` to
        appear at all, and that setting fills them with 0 rather than None.
        ADR 0005 spends a section on why `schedule_deviation_seconds` must
        distinguish "could not compute" from "exactly on time"; collapsing
        them here would undo that at the last step.

    So: fields with explicit presence become None when unset, everything else
    takes its value. That is the same rule publish.serialize() applies in the
    other direction.
    """
    out = {}
    for field in msg.DESCRIPTOR.fields:
        if field.has_presence and not msg.HasField(field.name):
            out[field.name] = None
        else:
            out[field.name] = getattr(msg, field.name)
    return out


def decode(raw: bytes, cls=None) -> dict | None:
    """Framed protobuf bytes -> dict, or None if they are not decodable.

    None rather than an exception. This runs inside a Flink operator, where a
    raised exception kills the TaskManager slot and restarts the job from the
    last checkpoint -- so one malformed record on a topic becomes a crash
    loop. The caller filters Nones; counting them is Phase 4's problem.
    """
    try:
        msg = (cls or message_class())()
        msg.ParseFromString(strip_framing(raw))
        return to_dict(msg)
    except Exception:  # noqa: BLE001 -- see the docstring; this is the point
        return None
