"""The check for the class of change the Schema Registry structurally cannot see.

ADR 0005 measured the boundary: Redpanda implements protobuf's own
wire-compatibility table, which answers *"can an old reader parse these bytes
without error?"* It does not and cannot answer *"do the values still mean the
same thing?"* Protobuf declares int32/uint32/int64/uint64/bool interchangeable
and string/bytes interchangeable, so a change inside one of those families
passes cleanly and can still destroy the data.

This module is the complement. It compares a candidate .proto against the
registered one and classifies every change three ways:

    WIRE_INCOMPATIBLE   the registry already rejects this; reported for
                        completeness so the two gates agree
    SEMANTIC_HAZARD     wire-compatible and meaning-changing -- the gap
    BENIGN              wire-compatible and meaning-preserving

--- why THREE outcomes and not two ---

A gate that fails on any change to an existing field is a gate people bypass.
`int32 -> int64` is genuine safe widening and `string -> bytes` is harmless;
blocking them trains everyone to pass `--no-verify` within a month, and then
it catches nothing at all. The classification is the product.

--- why narrow, and not a general schema differ ---

The dangerous set here is enumerable: presence drops on the 18 `optional`
fields, same-kind retypes, and number reuse. A general differ would mostly
restate protobuf's compatibility table back at you. This checks the hazards
that actually apply to this schema and says so.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from google.protobuf import descriptor_pb2

# protobuf FieldDescriptorProto.Type -> the wire "kind" a value is encoded as.
#
# Grouped exactly as protobuf's compatibility table groups them, which is what
# the registry enforces. Note sint32/sint64 are split out: they share the
# varint wire type but use zigzag encoding, so reinterpreting one as the other
# silently changes values -- and the registry does catch that one.
_KIND = {
    1: "fixed64",   2: "fixed32",  3: "varint",   4: "varint",
    5: "varint",    6: "fixed64",  7: "fixed32",  8: "varint",
    9: "length",   11: "length",  12: "length",  13: "varint",
    14: "varint",  15: "fixed32", 16: "fixed64", 17: "zigzag",
    18: "zigzag",
}
_TYPE_NAME = {
    1: "double",  2: "float",   3: "int64",   4: "uint64",  5: "int32",
    6: "fixed64", 7: "fixed32", 8: "bool",    9: "string", 11: "message",
    12: "bytes", 13: "uint32", 14: "enum",   15: "sfixed32", 16: "sfixed64",
    17: "sint32", 18: "sint64",
}

# Same varint kind, so wire-compatible -- and each destroys data.
_UNSIGNED = {"uint32", "uint64"}
_SIGNED = {"int32", "int64"}
# Ordered by width so a widening can be told from a narrowing.
_WIDTH = {"int32": 32, "uint32": 32, "int64": 64, "uint64": 64, "bool": 1}


@dataclass(frozen=True)
class FieldSpec:
    number: int
    name: str
    type_name: str
    kind: str
    has_presence: bool


@dataclass(frozen=True)
class Finding:
    severity: str          # WIRE_INCOMPATIBLE | SEMANTIC_HAZARD | BENIGN
    number: int
    summary: str
    detail: str = ""

    def __str__(self) -> str:
        line = f"  [{self.severity:17}] field {self.number:>3}  {self.summary}"
        return f"{line}\n{' ' * 26}{self.detail}" if self.detail else line


def compile_proto(text: str, message: str = "EnrichedVehiclePosition") -> dict[int, FieldSpec]:
    """.proto text -> {field_number: FieldSpec}.

    Goes through protoc rather than parsing the text, because the registry
    returns a CANONICALISED form -- comments stripped, whitespace normalised --
    and a text diff would report those as changes. Descriptors compare the
    structure, which is the only thing that matters.
    """
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "candidate.proto"
        out = Path(tmp) / "candidate.desc"
        src.write_text(text)
        proc = subprocess.run(
            # sys.executable, not "python" -- the bare name resolves to the
            # system interpreter, which has no grpc_tools and fails with a
            # ModuleNotFoundError that reads like a missing dependency rather
            # than a wrong interpreter.
            [sys.executable, "-m", "grpc_tools.protoc", f"-I{tmp}",
             f"--descriptor_set_out={out}", str(src)],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            raise ValueError(f"proto did not compile:\n{proc.stderr.strip()}")
        fds = descriptor_pb2.FileDescriptorSet()
        fds.ParseFromString(out.read_bytes())

    for f in fds.file:
        for m in f.message_type:
            if m.name != message:
                continue
            return {
                fd.number: FieldSpec(
                    number=fd.number,
                    name=fd.name,
                    type_name=_TYPE_NAME.get(fd.type, str(fd.type)),
                    kind=_KIND.get(fd.type, "unknown"),
                    # proto3_optional marks explicit presence. Without it an
                    # absent scalar is indistinguishable from zero -- the
                    # distinction six fields in this schema depend on.
                    has_presence=fd.proto3_optional,
                )
                for fd in m.field
            }
    raise ValueError(f"message {message!r} not found in schema")


def diff(candidate: str, registered: str,
         message: str = "EnrichedVehiclePosition") -> list[Finding]:
    """Classify every structural change between two schemas."""
    new = compile_proto(candidate, message)
    old = compile_proto(registered, message)
    findings: list[Finding] = []

    old_by_name = {f.name: n for n, f in old.items()}
    new_by_name = {f.name: n for n, f in new.items()}

    # --- numbers present in both -------------------------------------------
    for number in sorted(set(old) & set(new)):
        a, b = old[number], new[number]

        if a.kind != b.kind:
            findings.append(Finding(
                "WIRE_INCOMPATIBLE", number,
                f"{a.name}: {a.type_name} -> {b.type_name} changes wire kind "
                f"({a.kind} -> {b.kind})",
                "the registry rejects this on its own",
            ))
            continue

        # Same kind from here down: everything below is wire-compatible and
        # therefore invisible to the registry.
        if a.has_presence and not b.has_presence:
            findings.append(Finding(
                "SEMANTIC_HAZARD", number,
                f"{a.name}: `optional` dropped",
                "absence collapses into zero -- 'could not compute' becomes "
                "indistinguishable from a real 0",
            ))

        if a.type_name != b.type_name:
            if b.type_name == "bool":
                findings.append(Finding(
                    "SEMANTIC_HAZARD", number,
                    f"{a.name}: {a.type_name} -> bool",
                    "every nonzero value collapses to true",
                ))
            elif a.type_name in _SIGNED and b.type_name in _UNSIGNED:
                findings.append(Finding(
                    "SEMANTIC_HAZARD", number,
                    f"{a.name}: {a.type_name} -> {b.type_name} (signedness)",
                    "negative values wrap to large positives",
                ))
            elif _WIDTH.get(b.type_name, 0) < _WIDTH.get(a.type_name, 0):
                findings.append(Finding(
                    "SEMANTIC_HAZARD", number,
                    f"{a.name}: {a.type_name} -> {b.type_name} (narrowing)",
                    "values beyond the smaller range are truncated",
                ))
            else:
                findings.append(Finding(
                    "BENIGN", number,
                    f"{a.name}: {a.type_name} -> {b.type_name}",
                    "same kind, no loss of range or meaning",
                ))

        if a.name != b.name:
            # A rename is free on the wire. A SWAP is not: if this number now
            # carries a name that used to live elsewhere, and that elsewhere
            # now carries this one, two columns have exchanged meaning while
            # every byte still parses. lat/lon is the case that matters.
            swapped = (b.name in old_by_name
                       and old_by_name[b.name] in new
                       and new[old_by_name[b.name]].name == a.name)
            if swapped:
                findings.append(Finding(
                    "SEMANTIC_HAZARD", number,
                    f"{a.name} <-> {b.name} exchanged field numbers",
                    "same type, so the bytes parse and the two fields' values "
                    "are silently transposed",
                ))
            else:
                findings.append(Finding(
                    "BENIGN", number, f"renamed {a.name} -> {b.name}",
                    "field numbers are the wire identity; names are documentation",
                ))

    # --- removed and added --------------------------------------------------
    for number in sorted(set(old) - set(new)):
        findings.append(Finding(
            "SEMANTIC_HAZARD", number,
            f"{old[number].name}: field REMOVED",
            "wire-compatible (readers skip unknown numbers) and therefore "
            "silent data loss; the number must be added to `reserved` in the "
            "same commit or it can be reused with a different meaning later",
        ))

    for number in sorted(set(new) - set(old)):
        f = new[number]
        moved_from = old_by_name.get(f.name)
        if moved_from is not None and moved_from not in new:
            findings.append(Finding(
                "SEMANTIC_HAZARD", number,
                f"{f.name}: renumbered {moved_from} -> {number}",
                "every record already written carries the old number and will "
                "read as absent",
            ))
        elif not f.has_presence and f.kind != "length":
            findings.append(Finding(
                "BENIGN", number,
                f"{f.name}: added without `optional`",
                "compatible, but absent and zero will be indistinguishable",
            ))
        else:
            findings.append(Finding(
                "BENIGN", number, f"{f.name}: field added ({f.type_name})"))

    return findings


def summarise(findings: list[Finding]) -> tuple[int, int, int]:
    """(wire_incompatible, semantic_hazard, benign)."""
    return (
        sum(1 for f in findings if f.severity == "WIRE_INCOMPATIBLE"),
        sum(1 for f in findings if f.severity == "SEMANTIC_HAZARD"),
        sum(1 for f in findings if f.severity == "BENIGN"),
    )
