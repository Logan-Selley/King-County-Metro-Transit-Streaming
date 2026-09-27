"""Executable specification for the prediction-accuracy join.

    make contract-p3f                # all of them
    make contract-p3f K=lead         # narrow

Written before the implementation it tests, like the Phase 1, 2 and 3
suites. NO CLUSTER, no broker, no pyflink.

--- what these fixtures encode ---

The two streams disagree about almost everything, and the fixtures are shaped
to hold that disagreement rather than smooth it over:

    raw.trip_updates            JSON, ISO-8601 times, MANY records per key
    enriched.vehicle_positions  protobuf-decoded, int64 epoch times, ONE
                                arrival per key, and only when
                                current_status == STOPPED_AT

Times below are real epoch seconds with readable ISO equivalents, because a
join that compares a string to a float matches nothing and does it quietly.
"""

from __future__ import annotations

import pathlib
from datetime import datetime

import pytest

from consumers.prediction.accuracy import (
    PredictionBuffer,
    service_day,
    accuracy_record,
    epoch_s,
    join_key,
    lead_bucket,
    parse_observation,
    parse_prediction,
)
from consumers.prediction.config import CONFIG

pytestmark = pytest.mark.contract


ISSUED_ISO = "2026-09-22T06:12:26+00:00"
ISSUED_S = 1790057546.0
ARRIVAL_ISO = "2026-09-22T06:25:26+00:00"   # 13 minutes after issue
ARRIVAL_S = 1790058326.0

# THE TWO STREAMS DISAGREE ON THIS FIELD AND THE FIXTURES MUST TOO.
# Verified on the live topics: raw.trip_updates publishes ISO, enriched
# publishes GTFS. Were both sides the ISO form, the join would compare two
# identical strings and match nothing on real data.
SERVICE_DATE_ISO = "2026-09-22"    # raw.trip_updates
SERVICE_DATE_GTFS = "20260922"     # enriched.vehicle_positions


def prediction(**over) -> dict:
    """One raw.trip_updates record, as the producer publishes it."""
    rec = {
        "trip_id": "803357351",
        "stop_id": "76731",
        "stop_sequence": 26,
        "route_id": "102747",
        "direction_id": 0,
        "start_date": SERVICE_DATE_ISO,
        "vehicle_id": "3751",
        "trip_timestamp": ISSUED_ISO,
        "arrival_time": ARRIVAL_ISO,
        "arrival_delay": 25,
        "departure_time": ARRIVAL_ISO,
        "departure_delay": 25,
        "schedule_relationship": None,
        "feed_etag": '"4ca9fe7740496e0e27c313ad66e1dc33"',
    }
    rec.update(over)
    return rec


def observation(**over) -> dict:
    """One decoded enriched.vehicle_positions record at a stop."""
    rec = {
        "vehicle_id": "3751",
        "trip_id": "803357351",
        "stop_id": "76731",
        "start_date": SERVICE_DATE_GTFS,
        "route_id": "102747",
        "route_short_name": "36",
        "current_status": "STOPPED_AT",
        "current_stop_sequence": 26,
        "position_timestamp": int(ARRIVAL_S),
        "latitude": 47.5793,
        "longitude": -122.3104,
    }
    rec.update(over)
    return rec


# =============================================================================
# 0. isolation -- the Flink image's narrow dependency set
# =============================================================================


class TestImportIsolation:
    """accuracy.py must import nothing the Flink image lacks.

    Same static check as the bunching suite, for the same reason: importing
    the module and seeing that it works proves nothing, because the project
    venv has every dependency in the repo. ADR 0006 explains the split.
    """

    FORBIDDEN = ("consumers.enrichment", "producer", "static", "pyflink",
                 "psycopg", "shapely", "minio", "requests")

    def test_accuracy_stays_importable_in_the_image(self):
        import ast
        import importlib.util

        spec = importlib.util.find_spec("consumers.prediction.accuracy")
        tree = ast.parse(pathlib.Path(spec.origin).read_text())
        names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names.update(a.name for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.add(node.module)
        for imported in names:
            for bad in self.FORBIDDEN:
                assert not imported.startswith(bad), (
                    f"accuracy.py imports {imported}, which the Flink image "
                    f"cannot resolve. See ADR 0006.")


# =============================================================================
# 1. helpers -- pinned so the rest can be trusted
# =============================================================================


class TestEpochS:
    """The two streams use different time formats and always will."""

    def test_iso_string(self):
        assert epoch_s(ISSUED_ISO) == pytest.approx(ISSUED_S)

    def test_epoch_number_passes_through(self):
        assert epoch_s(1790057546) == 1790057546.0
        assert epoch_s(1790057546.5) == 1790057546.5

    def test_none_and_garbage_return_none(self):
        """A record whose time cannot be read is dropped by the caller, not
        allowed to raise inside an operator."""
        assert epoch_s(None) is None
        assert epoch_s("not a time") is None
        assert epoch_s("20260922") is None  # GTFS date form, not ISO

    def test_the_two_streams_agree_after_normalising(self):
        """The whole point: ISO from JSON and int64 from protobuf must land
        on the same number, or the join silently matches nothing."""
        assert epoch_s(ARRIVAL_ISO) == epoch_s(int(ARRIVAL_S))


class TestLeadBucket:
    def test_negative_lead_is_its_own_bucket(self):
        """"The sign said the bus left 3 minutes ago" is a different rider
        experience from "the sign said 2 minutes", and folding them together
        flatters the short end of the curve."""
        assert lead_bucket(-60) == "past"
        assert lead_bucket(-0.1) == "past"

    def test_boundaries_land_in_the_upper_bucket(self):
        assert lead_bucket(0) == "0-2m"
        assert lead_bucket(119) == "0-2m"
        assert lead_bucket(120) == "2-5m"
        assert lead_bucket(300) == "5-10m"

    def test_long_lead(self):
        """Lower-inclusive throughout: a value ON a bound belongs to the
        bucket that bound opens."""
        assert lead_bucket(1200) == "20-30m"
        assert lead_bucket(1799) == "20-30m"
        assert lead_bucket(1800) == "30-45m"
        assert lead_bucket(2700) == "45-60m"
        assert lead_bucket(3599) == "45-60m"

    def test_every_bucket_is_reachable(self):
        """Nine labels, nine reachable. A bound list one longer than the
        label list would silently relabel every bucket instead of failing."""
        seen = {lead_bucket(s) for s in
                (-1, 0, 150, 400, 700, 1000, 1500, 2000, 3000)}
        assert len(seen) == 9


# =============================================================================
# 2. parsing
# =============================================================================


class TestParsePrediction:
    def test_keeps_both_timestamps_as_epoch(self):
        p = parse_prediction(prediction())
        assert p["predicted_arrival"] == pytest.approx(ARRIVAL_S)
        assert p["issued_at"] == pytest.approx(ISSUED_S)

    def test_keeps_the_join_identity(self):
        p = parse_prediction(prediction())
        for f in ("trip_id", "stop_id", "start_date"):
            assert p[f] == prediction()[f]

    def test_drops_record_without_arrival_time(self):
        assert parse_prediction(prediction(arrival_time=None)) is None

    def test_drops_record_without_start_date(self):
        """Without it the key cannot distinguish service days."""
        assert parse_prediction(prediction(start_date=None)) is None

    def test_does_not_require_arrival_delay(self):
        """arrival_delay is Metro's own claim about lateness. This module
        exists to check that claim, so it must not depend on it."""
        assert parse_prediction(prediction(arrival_delay=None)) is not None

    def test_does_not_require_a_vehicle(self):
        """45.8% of trip updates carry a vehicle. The other 54% are trips
        that have not started, which is the long-lead-time end of the curve
        and the interesting half."""
        assert parse_prediction(prediction(vehicle_id=None)) is not None

    def test_garbage_returns_none_rather_than_raising(self):
        assert parse_prediction({}) is None
        assert parse_prediction(None) is None


class TestParseObservation:
    def test_stopped_at_is_an_arrival(self):
        o = parse_observation(observation())
        assert o is not None
        assert o["observed_at"] == pytest.approx(ARRIVAL_S)

    def test_in_transit_is_not_an_arrival(self):
        """The filter that turns a position feed into an arrival feed.
        Measured: 23.1% of positions are STOPPED_AT."""
        assert parse_observation(observation(current_status="IN_TRANSIT_TO")) is None

    def test_absent_status_is_not_an_arrival(self):
        """Phase 0's most expensive finding was that an ABSENT
        current_status means IN_TRANSIT_TO, not unknown. decode.to_dict()
        already applies the proto2 default, so a missing value here is
        genuinely missing -- and either way it is not an arrival."""
        assert parse_observation(observation(current_status=None)) is None

    def test_drops_stopped_at_without_a_stop(self):
        assert parse_observation(observation(stop_id=None)) is None

    def test_garbage_returns_none_rather_than_raising(self):
        assert parse_observation({}) is None
        assert parse_observation(None) is None


class TestServiceDay:
    """The cross-format normaliser, and the bug it exists for.

    Verified on the live topics on 2026-09-22:

        raw.trip_updates            start_date = "2026-09-22"
        enriched.vehicle_positions  start_date = "20260922"

    The divergence is deliberate on the enriched side --
    consumers/enrichment/schema.py does `.replace("-", "")` with a docstring
    saying "as the agency publishes it" -- so neither stream is wrong and
    the join has to reconcile them.
    """

    def test_both_wire_formats_normalise_to_one_value(self):
        assert service_day(SERVICE_DATE_ISO) == service_day(SERVICE_DATE_GTFS)
        assert service_day(SERVICE_DATE_ISO) == "20260922"

    def test_accepts_a_date_object(self):
        from datetime import date

        assert service_day(date(2026, 9, 22)) == "20260922"

    def test_rejects_anything_else(self):
        assert service_day(None) is None
        assert service_day("2026-9-22") is None     # unpadded, not GTFS
        assert service_day("not a date") is None
        assert service_day(20260922) is None        # int, not a date


class TestJoinKey:
    def test_same_trip_and_stop_share_a_key_across_wire_formats(self):
        """THE regression test for the format divergence.

        Left unnormalised, join_key builds "2026-09-22:803357351:76731"
        against "20260922:803357351:76731". They never match, every
        prediction sits in state until its TTL expires, and the job runs
        clean with an empty sink -- the same silent shape as Phase 2's
        schedule_deviation returning None for every record.
        """
        pred, obs = prediction(), observation()
        assert pred["start_date"] != obs["start_date"], "fixtures must differ"
        assert join_key(parse_prediction(pred)) == \
               join_key(parse_observation(obs))

    def test_key_starts_with_the_service_day(self):
        """Sorts by service day, which is what makes the key column readable
        in the Flink UI during a spot-check."""
        assert join_key(parse_prediction(prediction())).startswith("20260922")

    def test_service_date_is_part_of_the_key(self):
        """trip_id repeats every service day. Without start_date a
        prediction issued today matches tomorrow's run of the same trip and
        reports an error of roughly 24 hours."""
        a = parse_prediction(prediction())
        b = parse_prediction(prediction(start_date="2026-09-23"))  # ISO, next day
        assert join_key(a) != join_key(b)

    def test_different_stops_do_not_share_a_key(self):
        a = parse_prediction(prediction())
        b = parse_prediction(prediction(stop_id="99999"))
        assert join_key(a) != join_key(b)

    def test_key_is_a_string(self):
        """A tuple crossing the Python/JVM boundary without an explicit type
        gets pickled rather than rejected."""
        assert isinstance(join_key(parse_prediction(prediction())), str)


# =============================================================================
# 3. the measurement
# =============================================================================


class TestAccuracyRecord:
    def test_lead_time_comes_from_the_prediction_alone(self):
        """A prediction's lead time is a property of when it was issued. It
        must not change depending on when the bus actually turned up."""
        p = parse_prediction(prediction())
        early = accuracy_record(p, parse_observation(observation(
            position_timestamp=int(ARRIVAL_S - 600))))
        late = accuracy_record(p, parse_observation(observation(
            position_timestamp=int(ARRIVAL_S + 600))))
        assert early["lead_time_s"] == late["lead_time_s"] == pytest.approx(780)

    def test_perfect_prediction_has_zero_error(self):
        r = accuracy_record(parse_prediction(prediction()),
                            parse_observation(observation()))
        assert r["error_s"] == pytest.approx(0, abs=1)

    def test_sign_convention_bus_arrived_early(self):
        """Predicted 06:25:26, arrived 06:23:26. The prediction was LATE
        relative to reality, so error is POSITIVE and the rider who trusted
        the sign missed the bus."""
        r = accuracy_record(parse_prediction(prediction()),
                            parse_observation(observation(
                                position_timestamp=int(ARRIVAL_S - 120))))
        assert r["error_s"] == pytest.approx(120)

    def test_sign_convention_bus_arrived_late(self):
        r = accuracy_record(parse_prediction(prediction()),
                            parse_observation(observation(
                                position_timestamp=int(ARRIVAL_S + 180))))
        assert r["error_s"] == pytest.approx(-180)

    def test_abs_error_is_unsigned(self):
        r = accuracy_record(parse_prediction(prediction()),
                            parse_observation(observation(
                                position_timestamp=int(ARRIVAL_S + 180))))
        assert r["abs_error_s"] == pytest.approx(180)

    def test_carries_the_bucket_and_identity(self):
        r = accuracy_record(parse_prediction(prediction()),
                            parse_observation(observation()))
        assert r["lead_bucket"] == "10-15m"
        for f in ("trip_id", "stop_id", "route_id", "start_date"):
            assert f in r

    def test_drops_a_pair_beyond_the_join_window(self):
        """A 24-hour lead is a next-service-day artifact, not a data point."""
        p = parse_prediction(prediction(
            trip_timestamp="2026-09-21T06:12:26+00:00"))  # 24h before arrival
        assert accuracy_record(p, parse_observation(observation())) is None

    def test_drops_a_prediction_issued_after_the_bus_arrived(self):
        """Not a forecast. 97% of trip updates carry arrival_time ==
        departure_time, and Metro keeps restating that time while the bus
        sits at the stop. Over 702,069 real joined pairs these were 13.4% of
        the total, 74.6% of the 0-2m bucket and 99.5% of "past", and kept
        they doubled the 0-2m median error."""
        p = parse_prediction(prediction(
            trip_timestamp="2026-09-22T06:30:26+00:00"))   # 5 min after arrival
        assert accuracy_record(p, parse_observation(observation())) is None

    def test_drops_a_prediction_issued_at_the_moment_of_arrival(self):
        """The boundary belongs to "already arrived"."""
        p = parse_prediction(prediction(trip_timestamp=ARRIVAL_ISO))
        assert accuracy_record(p, parse_observation(observation())) is None

    def test_keeps_negative_lead_issued_before_the_arrival(self):
        """The case keep_negative_lead is actually for, and the rarer one:
        135 of 702,069 real pairs. Issued at 06:27:26, predicting 06:25:26,
        a time already behind it -- and the bus then turned up even later,
        at 06:29:26. The sign was stale for a late bus."""
        assert CONFIG.keep_negative_lead
        p = parse_prediction(prediction(
            trip_timestamp="2026-09-22T06:27:26+00:00"))
        r = accuracy_record(p, parse_observation(observation(
            position_timestamp=int(ARRIVAL_S + 240))))
        assert r is not None and r["lead_bucket"] == "past"
        assert r["error_s"] == pytest.approx(-240)


# =============================================================================
# 4. the join itself
# =============================================================================


class TestPredictionBuffer:
    def test_prediction_before_observation_emits_nothing(self):
        emit, buf = PredictionBuffer().on_prediction(parse_prediction(prediction()))
        assert emit == []
        assert len(buf.predictions) == 1

    def test_observation_drains_every_buffered_prediction(self):
        """One arrival resolves a whole curve for that stop: mean 18
        predictions per key, max 78, and 92.1% of repeats carry a revised
        arrival_time, so these really are distinct estimates.

        Timestamps are built with timedelta rather than string arithmetic.
        Subtracting inside an f-string produced "06:-5:26" for the 30-minute
        case, an unparseable time whose prediction could never emit, so the
        length assertion below was unreachable.
        """
        from datetime import timedelta

        issued_base = datetime.fromisoformat(ARRIVAL_ISO)
        buf = PredictionBuffer()
        for m in (30, 20, 10, 5, 2):
            ts = (issued_base - timedelta(minutes=m)).isoformat()
            _, buf = buf.on_prediction(parse_prediction(prediction(trip_timestamp=ts)))
        emit, buf = buf.on_observation(parse_observation(observation()))
        assert len(emit) == 5
        # Lower-inclusive bucketing: a 30-minute lead is 1800s, and 1800 is
        # the lower bound of "30-45m". lead_bucket's own tests pin this.
        assert {r["lead_bucket"] for r in emit} == \
               {"30-45m", "20-30m", "10-15m", "5-10m", "2-5m"}

    def test_second_observation_is_ignored(self):
        """A bus dwells at a stop and reports STOPPED_AT on every poll.
        Taking the last would measure DEPARTURE and make the error depend on
        dwell time."""
        assert CONFIG.first_observation_wins
        _, buf = PredictionBuffer().on_prediction(parse_prediction(prediction()))
        first, buf = buf.on_observation(parse_observation(observation()))
        second, buf = buf.on_observation(parse_observation(observation(
            position_timestamp=int(ARRIVAL_S + 60))))
        assert len(first) == 1
        assert second == []

    def test_prediction_processed_after_its_observation_still_resolves(self):
        """Processing order is not event order across two topics.

        During a replay the enriched topic (~1.6M records, 3 partitions) is
        read far faster than raw.trip_updates (~37M, 6 partitions), so an
        arrival routinely reaches the join BEFORE predictions that were
        issued well ahead of it. Those must resolve against the stored
        arrival rather than wait for an observation that already came.
        """
        _, buf = PredictionBuffer().on_observation(parse_observation(observation()))
        emit, buf = buf.on_prediction(parse_prediction(prediction()))  # issued 13 min before
        assert len(emit) == 1
        assert emit[0]["lead_bucket"] == "10-15m"
        assert buf.predictions == []

    def test_restatement_after_the_arrival_emits_nothing(self):
        """The same late path, for a prediction issued after the bus came:
        it resolves to no record rather than a false data point, and is not
        buffered either."""
        _, buf = PredictionBuffer().on_observation(parse_observation(observation()))
        emit, buf = buf.on_prediction(parse_prediction(prediction(
            trip_timestamp="2026-09-22T06:30:26+00:00")))
        assert emit == []
        assert buf.predictions == []

    def test_buffer_is_capped(self):
        """Unbounded ListState is how one node runs out of heap. The cap
        drops the OLDEST, because the newest prediction is closest to truth."""
        buf = PredictionBuffer()
        for i in range(CONFIG.max_predictions_per_key + 25):
            _, buf = buf.on_prediction(parse_prediction(prediction(stop_sequence=i)))
        assert len(buf.predictions) <= CONFIG.max_predictions_per_key

    def test_observation_is_remembered_across_calls(self):
        """So a late prediction can still be resolved afterwards."""
        _, buf = PredictionBuffer().on_observation(parse_observation(observation()))
        assert buf.observed_at == pytest.approx(ARRIVAL_S)

    def test_round_trips_through_state(self):
        """ValueState holds a dict, not the dataclass: a pickled dataclass
        restores without __init__ and raises on a field added later."""
        _, buf = PredictionBuffer().on_prediction(parse_prediction(prediction()))
        restored = PredictionBuffer.from_dict(buf.to_dict())
        assert restored.predictions == buf.predictions
        assert restored.observed_at == buf.observed_at

    def test_empty_state_is_a_fresh_buffer(self):
        """ValueState is None the first time a key is seen."""
        b = PredictionBuffer.from_dict(None)
        assert b.predictions == [] and b.observed_at is None
