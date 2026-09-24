"""Executable specification for the sink framing and its JSON Schemas.

    make test
    pytest tests/test_sink_framing.py -m contract

Build step 4B. The two Flink output topics are schemaless JSON, and the JDBC
sink rejects those ("requires records with a non-null Struct value and non-null
Struct schema"), so the jobs write Confluent framing and the schemas live in
schemas/json/. ADR 0008 holds the decision.

The tests that matter most here are the last two: the schema is what the
connector builds its columns from, so a field the detector starts emitting
without a matching schema entry is a column that silently never arrives.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path

import pytest

from consumers import framing
from consumers.bunching.detect import detect_in_window
from consumers.prediction.accuracy import accuracy_record

pytestmark = pytest.mark.contract

SCHEMAS = Path(__file__).resolve().parents[1] / "schemas" / "json"

# Same coordinates as the bunching suite uses (Aurora Ave N, a real E Line
# stop), because the proximity gate compares along-route distance against
# straight-line distance and invented coordinates would pass for the wrong
# reason.
#
# INTEGER epoch seconds, matching what the job now emits: the window end is
# int(context.window().end / 1000) and parse_record keeps int64 as an int. A
# float fixture would let a FLOAT64 regression through the type check below,
# which is the check that exists because that regression happened.
WINDOW_END = 1_758_400_000
LAT, LON = 47.6970, -122.3450


def schema(name: str) -> dict:
    return json.loads((SCHEMAS / name).read_text())


def parsed_position(vehicle_id: str, dist: float) -> dict:
    """A parsed position, as detect.parse_record returns it.

    `current_stop_sequence` is here for MIN_STOP_SEQUENCE rather than for the
    alert payload: the detector drops anything before stop 4, because buses
    leaving a terminal on a dispatch schedule are a dispatching artifact and not
    bunching a rider would notice. Leaves the field out and this fixture
    silently stops alerting, which is exactly how it failed the first time.
    """
    return {
        "vehicle_id": vehicle_id,
        "route_id": "100512",
        "direction_id": 0,
        "trip_id": f"trip-{vehicle_id}",
        "route_short_name": "E Line",
        "shape_dist_traveled": dist,
        "position_timestamp": WINDOW_END - 5,
        "latitude": LAT,
        "longitude": LON,
        "current_stop_sequence": 20,
        "schedule_deviation_seconds": 60,
    }


class TestFraming:
    """The five bytes, which are the whole interface with the connector."""

    def test_magic_then_big_endian_id_then_the_payload(self):
        raw = framing.frame({"a": 1}, 7)
        assert raw[0] == 0x00, "the Confluent magic byte"
        assert struct.unpack(">I", raw[1:5])[0] == 7
        assert raw[5:] == b'{"a": 1}'

    def test_round_trips(self):
        sid, payload = framing.unframe(framing.frame({"a": 1}, 7))
        assert sid == 7
        assert json.loads(payload) == {"a": 1}

    def test_accepts_a_json_string_as_well_as_a_dict(self):
        assert framing.frame('{"a": 1}', 7) == framing.frame({"a": 1}, 7)

    def test_id_is_big_endian(self):
        """Only wrong once, and the registry would need 16M schemas to reach
        the high byte on its own."""
        assert framing.frame("", 0x01020304)[1:5] == b"\x01\x02\x03\x04"

    def test_unframed_input_is_rejected(self):
        """What a plain-JSON record looks like to a Confluent deserializer,
        which is why the topics had to change rather than the connector."""
        with pytest.raises(ValueError):
            framing.unframe(b'{"a": 1}')
        with pytest.raises(ValueError):
            framing.unframe(b"\x00\x00")


def assert_types_match(payload: dict, doc: dict) -> None:
    """Every value's Python type has to satisfy the schema's declared type.

    Matching KEYS is not enough, which this test learned the expensive way: the
    first version of the alert schema declared window_end as `number`, the job
    emitted a float, and the TimestampConverter refused the record as FLOAT64
    having written nothing. A schema that says `integer` and a payload that
    carries `1.0` are two different Connect types.
    """
    for field, value in payload.items():
        declared = doc["properties"][field]["type"]
        declared = declared if isinstance(declared, list) else [declared]
        where = f"{field}={value!r}"
        if value is None:
            assert "null" in declared, f"{where}, but the schema is not nullable"
        elif isinstance(value, bool):
            raise AssertionError(f"{where} is a bool; JSON Schema has no bool here")
        elif "integer" in declared:
            assert isinstance(value, int), \
                f"{where} is {type(value).__name__}, schema says integer"
        elif "number" in declared:
            assert isinstance(value, (int, float)), \
                f"{where} is {type(value).__name__}, schema says number"
        elif "string" in declared:
            assert isinstance(value, str), \
                f"{where} is {type(value).__name__}, schema says string"


class TestSchemasMatchWhatTheJobsWrite:
    """The schema is the connector's column list, so drift drops data."""

    def test_the_alert_schema_covers_every_field_the_detector_emits(self):
        alerts = detect_in_window(
            "100512:0",
            [parsed_position("v1", 200.0), parsed_position("v2", 700.0)],
            WINDOW_END,
        )
        assert len(alerts) == 1, "the fixture pair has to alert for this to mean anything"
        doc = schema("bunching_alert.json")
        declared = set(doc["properties"])
        assert set(alerts[0]) == declared, (
            "consumers/bunching/detect.py and schemas/json/bunching_alert.json "
            "have drifted. The JDBC sink builds its columns from the schema, so "
            "a field the schema does not declare never reaches the warehouse."
        )
        assert_types_match(alerts[0], doc)

    def test_the_accuracy_schema_covers_every_field_accuracy_record_emits(self):
        record = accuracy_record(
            # A parsed prediction and a parsed observation, minimal because
            # accuracy_record only reads these keys.
            {
                "trip_id": "803357351",
                "stop_id": "76731",
                "start_date": "2026-09-22",
                "route_id": "102747",
                "predicted_arrival": 1_790_058_326,
                "issued_at": 1_790_057_546,
            },
            {"observed_at": 1_790_058_326, "route_short_name": "36"},
        )
        doc = schema("prediction_accuracy.json")
        declared = set(doc["properties"])
        assert set(record) == declared, (
            "consumers/prediction/accuracy.py and "
            "schemas/json/prediction_accuracy.json have drifted."
        )
        assert_types_match(record, doc)

    @pytest.mark.parametrize("name", ["bunching_alert.json",
                                      "prediction_accuracy.json"])
    def test_every_required_field_is_declared(self, name):
        """A required name with no property is a schema the registry accepts
        and the converter then fails on, one record at a time."""
        doc = schema(name)
        assert set(doc["required"]) <= set(doc["properties"])

    @pytest.mark.parametrize("name", ["bunching_alert.json",
                                      "prediction_accuracy.json"])
    def test_epoch_seconds_fields_are_declared_integers(self, name):
        """The connector's TimestampConverter reads these as unix seconds and
        accepts INT32/INT64 only: a declared `number` lets a float through, and
        the task then dies on "Schema Schema{FLOAT64} does not correspond to a
        known timestamp type format". Measured, after a full 1.35M-record
        migration had already run."""
        doc = schema(name)
        for field in ("window_end", "observed_at", "issued_at", "predicted_arrival"):
            if field in doc["properties"]:
                assert doc["properties"][field]["type"] == "integer"
