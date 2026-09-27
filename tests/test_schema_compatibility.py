"""What the Schema Registry actually enforces for protobuf, measured.

Marked `contract`, and skips when no registry is reachable.

    make contract-schema

Avro resolves by field NAME, but protobuf resolves by field NUMBER and skips
unknowns, so removing or renumbering an identity field is wire-compatible
rather than a registration failure. ADR 0005's revision is why these exist.

Writing the rule into a doc is not enough. A doc cannot fail. These assert the
behaviour against the live registry so it cannot drift.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

import pytest

pytestmark = pytest.mark.contract

SUBJECT = "enriched.vehicle_positions-value"
PROTO = Path(__file__).parent.parent / "schemas" / "enriched_vehicle_position.proto"
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081")


def check(schema: str) -> bool:
    """Ask the registry whether `schema` would be accepted. Does not write."""
    body = json.dumps({"schema": schema, "schemaType": "PROTOBUF"}).encode()
    req = urllib.request.Request(
        f"{REGISTRY}/compatibility/subjects/{SUBJECT}/versions/latest",
        data=body,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    try:
        return json.load(urllib.request.urlopen(req, timeout=10))["is_compatible"]
    except urllib.error.HTTPError as exc:
        pytest.skip(f"subject not registered: {exc.code}")
    except Exception as exc:  # noqa: BLE001 -- no registry running
        pytest.skip(f"no registry at {REGISTRY}: {exc}")


@pytest.fixture(scope="module")
def base() -> str:
    if not PROTO.exists():
        pytest.skip("proto missing")
    return PROTO.read_text()


class TestWhatIsRejected:
    """Changing a field's SCALAR KIND on an existing number."""

    def test_identity_field_type_change_is_rejected(self, base):
        """THE demonstration for Phase 2E -- this is the rejection ADR 0005
        should have predicted, not removal."""
        mutated = base.replace("string vehicle_id          = 1;",
                               "int32  vehicle_id          = 1;")
        assert mutated != base, "mutation did not apply -- proto text drifted"
        assert check(mutated) is False

    def test_optional_field_type_change_is_rejected(self, base):
        mutated = base.replace("optional string neighborhood_name          = 32;",
                               "optional int64  neighborhood_name          = 32;")
        assert mutated != base
        assert check(mutated) is False

    def test_float_width_change_is_rejected(self, base):
        """double -> float crosses fixed64 to fixed32, a wire-type change."""
        mutated = base.replace("double latitude            = 3;",
                               "float  latitude            = 3;")
        assert mutated != base
        assert check(mutated) is False

    def test_zigzag_is_its_own_kind(self, base):
        """int32 and sint32 are both varint on the wire, but zigzag encoding
        means reinterpreting one as the other silently changes values. The
        registry catches it, which is sharper than wire type alone."""
        mutated = base.replace("optional int32  schedule_deviation_seconds = 31;",
                               "optional sint32 schedule_deviation_seconds = 31;")
        assert mutated != base
        assert check(mutated) is False


class TestWhatIsAllowed:
    """The corrections to ADR 0005. Each of these was claimed to fail."""

    def test_removing_a_field_is_allowed(self, base):
        """Protobuf readers skip unknown numbers, so a removed field is simply
        absent. ADR 0005 claimed this fails. It does not."""
        import re
        mutated = re.sub(r"\s*string vehicle_id\s*=\s*1;", "", base)
        assert mutated != base
        assert check(mutated) is True

    def test_renumbering_a_field_is_allowed(self, base):
        mutated = base.replace("string vehicle_id          = 1;",
                               "string vehicle_id          = 34;")
        assert mutated != base
        assert check(mutated) is True

    def test_same_kind_widening_is_allowed(self, base):
        mutated = base.replace("optional int32  schedule_deviation_seconds = 31;",
                               "optional int64  schedule_deviation_seconds = 31;")
        assert mutated != base
        assert check(mutated) is True


class TestSemanticDiffGate:
    """The complement to the registry -- consumers/enrichment/semantic_diff.py.

    Every case in TestWhatSlipsThrough is registry-COMPATIBLE. These assert
    the second gate catches them, and just as importantly that it does NOT
    fire on safe changes: a gate that blocks `int32 -> int64` gets bypassed,
    and then it catches nothing at all.

    These need no registry -- they diff two schema texts directly.
    """

    def blocks(self, candidate: str, base: str) -> bool:
        from consumers.enrichment.semantic_diff import diff, summarise
        wire, hazard, _ = summarise(diff(candidate, base))
        return bool(wire or hazard)

    # --- catches what the registry lets through ---------------------------

    def test_catches_dropped_optional(self, base):
        assert self.blocks(
            base.replace("optional int32  schedule_deviation_seconds = 31;",
                         "int32  schedule_deviation_seconds = 31;"), base)

    def test_catches_coordinate_swap(self, base):
        swapped = (base
                   .replace("double latitude            = 3;", "double latitude            = 99;")
                   .replace("double longitude           = 4;", "double longitude           = 3;")
                   .replace("double latitude            = 99;", "double latitude            = 4;"))
        assert self.blocks(swapped, base)

    def test_catches_signedness_change(self, base):
        assert self.blocks(
            base.replace("optional int32  schedule_deviation_seconds = 31;",
                         "optional uint32 schedule_deviation_seconds = 31;"), base)

    def test_catches_int_to_bool(self, base):
        assert self.blocks(
            base.replace("optional int32  schedule_deviation_seconds = 31;",
                         "optional bool   schedule_deviation_seconds = 31;"), base)

    def test_catches_narrowing(self, base):
        assert self.blocks(
            base.replace("int64  position_timestamp  = 2;",
                         "int32  position_timestamp  = 2;"), base)

    def test_catches_removal(self, base):
        import re
        assert self.blocks(re.sub(r"\s*string vehicle_id\s*=\s*1;", "", base), base)

    # --- does NOT fire on safe changes ------------------------------------

    def test_allows_unchanged(self, base):
        assert not self.blocks(base, base)

    def test_allows_safe_widening(self, base):
        """int32 -> int64 loses nothing. Blocking it trains people to bypass."""
        assert not self.blocks(
            base.replace("optional int32  schedule_deviation_seconds = 31;",
                         "optional int64  schedule_deviation_seconds = 31;"), base)

    def test_allows_string_to_bytes(self, base):
        assert not self.blocks(
            base.replace("optional string neighborhood_name          = 32;",
                         "optional bytes  neighborhood_name          = 32;"), base)

    def test_allows_rename(self, base):
        """Names are documentation; field numbers are the wire identity."""
        assert not self.blocks(
            base.replace("optional string trip_headsign    = 23;",
                         "optional string headsign         = 23;"), base)

    def test_ignores_comment_and_whitespace_changes(self, base):
        """The registry returns a canonicalised form with comments stripped, so
        the differ must compare descriptors rather than text or every check
        would report the entire file as changed."""
        noisy = "// a new comment\n" + base.replace("\n\n", "\n\n\n")
        assert not self.blocks(noisy, base)


class TestWhatSlipsThrough:
    """Compatible by the rule, destructive in practice. The reason `reserved`
    and the bbox DLQ check are process controls rather than decoration."""

    def test_coordinate_swap_is_not_caught(self, base):
        """latitude and longitude are both double, so exchanging their field
        numbers passes cleanly and transposes every coordinate in the stream.

        This is exactly the transposed-coordinate failure
        errors.KING_COUNTY_BBOX exists to catch -- and it is that bounds check,
        not the registry, that would catch it. The compatibility gate does not
        protect the one field pair where a swap is both plausible and
        catastrophic.
        """
        mutated = (base
                   .replace("double latitude            = 3;", "double latitude            = 99;")
                   .replace("double longitude           = 4;", "double longitude           = 3;")
                   .replace("double latitude            = 99;", "double latitude            = 4;"))
        assert mutated != base
        assert check(mutated) is True, (
            "if this ever starts failing the registry got stricter -- good news, "
            "but ADR 0005's revision needs updating"
        )

    def test_int_to_bool_is_not_caught(self, base):
        """Same varint kind, so it passes. Every nonzero deviation becomes
        True -- the data is destroyed and the gate says fine."""
        mutated = base.replace("optional int32  schedule_deviation_seconds = 31;",
                               "optional bool   schedule_deviation_seconds = 31;")
        assert mutated != base
        assert check(mutated) is True

    def test_dropping_optional_is_not_caught(self, base):
        """The worst one for THIS schema, because `optional` is load-bearing.

        proto3 explicit presence is why an unset deviation is distinguishable
        from a bus exactly on time. Measured on the wire:

            optional int32 unset      HasField=False   0 bytes
            optional int32 set to 0   HasField=True    3 bytes

        The zero IS written. Dropping `optional` does not change the bytes the
        producer emits -- it changes what the READER can recover from them,
        collapsing "could not compute" and "exactly on time" into the same 0.

        Registry says fine. The .proto's stated reason for using `optional`
        evaporates.
        """
        mutated = base.replace("optional int32  schedule_deviation_seconds = 31;",
                               "int32  schedule_deviation_seconds = 31;")
        assert mutated != base
        assert check(mutated) is True

    def test_same_typed_field_swap_is_not_caught(self, base):
        """The lat/lon hazard is not unique to coordinates. Any two fields of
        the same scalar kind can exchange numbers cleanly -- here two int32s,
        where the result is every vehicle's direction reported as its route
        type and vice versa."""
        mutated = (base
                   .replace("int32  direction_id        = 7;", "int32  direction_id        = 98;")
                   .replace("optional int32  route_type       = 22;", "optional int32  route_type       = 7;")
                   .replace("int32  direction_id        = 98;", "int32  direction_id        = 22;"))
        assert mutated != base
        assert check(mutated) is True

    def test_signedness_change_is_not_caught(self, base):
        """int32 -> uint32 passes. Negative deviations -- a bus running EARLY,
        which is half the distribution -- wrap to huge positives."""
        mutated = base.replace("optional int32  schedule_deviation_seconds = 31;",
                               "optional uint32 schedule_deviation_seconds = 31;")
        assert mutated != base
        assert check(mutated) is True
