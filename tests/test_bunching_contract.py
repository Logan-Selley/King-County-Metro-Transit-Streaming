"""Executable specification for the bunching detector.

    make contract-p3                 # all of them
    make contract-p3 K=proximity     # narrow

Marked `contract`, like the Phase 1 and Phase 2 suites, and written before
the implementation it tests.

NO CLUSTER, no broker, no warehouse, no pyflink. Everything here imports
consumers.bunching.detect, which is why that module exists separately from
job.py -- a detector reachable only through a Flink submit has no test path.

--- what these fixtures encode ---

Coordinates are real King County positions on Aurora Ave N, which the E Line
runs. They are not arbitrary: the proximity gate compares along-route distance
against straight-line distance, so a fixture with invented lat/lon would
either pass or fail for reasons unrelated to the logic.

`shape_dist_traveled` is in FEET (config.py has the measurement). Every
distance in this file is feet unless it says otherwise.
"""

from __future__ import annotations

import pathlib

import pytest

from consumers.bunching.config import CONFIG
from consumers.bunching.decode import DESCRIPTOR_PATH, decode, strip_framing
from consumers.bunching.detect import (
    MIN_PROGRESS_FT,
    MIN_STOP_SEQUENCE,
    BunchingState,
    assign_route_key,
    confirm_proximity,
    detect_in_window,
    haversine_ft,
    pair_key,
    parse_record,
)

# One real message off enriched.vehicle_positions, Confluent prefix included.
FIXTURE = "enriched_vehicle_position_framed.bin"


@pytest.fixture(scope="module")
def record() -> dict:
    """The fixture record, decoded. Skips if the descriptor is not built."""
    if not pathlib.Path(DESCRIPTOR_PATH).exists():
        pytest.skip(f"no descriptor at {DESCRIPTOR_PATH} -- run `make schema-gen`")
    return decode((pathlib.Path(__file__).parent / "fixtures" / FIXTURE).read_bytes())

pytestmark = pytest.mark.contract


# Aurora Ave N, northbound. ~0.0001 degree of latitude is ~36 ft, so these
# are close enough to be a genuine bunching pair and far enough apart to be
# distinguishable.
AURORA_LAT = 47.6970
AURORA_LON = -122.3450

WINDOW_END = 1_758_400_000.0  # epoch seconds; arbitrary but fixed


def position(
    vehicle_id: str,
    dist: float,
    *,
    lat: float = AURORA_LAT,
    lon: float = AURORA_LON,
    age_s: float = 5.0,
    route_id: str = "100512",
    direction_id: int = 0,
    deviation: int | None = 60,
    trip_id: str | None = None,
    stop_seq: int | None = 12,
) -> dict:
    """One parsed position, as parse_record should return it."""
    return {
        "vehicle_id": vehicle_id,
        "route_id": route_id,
        "direction_id": direction_id,
        "trip_id": trip_id or f"trip-{vehicle_id}",
        "route_short_name": "E Line",
        "shape_dist_traveled": dist,
        "position_timestamp": WINDOW_END - age_s,
        "latitude": lat,
        "longitude": lon,
        "schedule_deviation_seconds": deviation,
        # Past MIN_STOP_SEQUENCE by default, so a fixture that is not about
        # the terminal gate does not accidentally trip it.
        "current_stop_sequence": stop_seq,
    }


def offset_lat(feet: float) -> float:
    """A latitude that many feet north of AURORA_LAT. ~364,000 ft per degree."""
    return AURORA_LAT + feet / 364_000.0


# =============================================================================
# 0. isolation -- what the Flink image can actually import
# =============================================================================


class TestImportIsolation:
    """The detector's import graph must stay inside the Flink image.

    This is a STATIC check on purpose. The obvious dynamic version -- import
    the module and see if it works -- passes in the project venv no matter
    what, because the venv has every dependency the whole project uses. The
    image has a deliberately narrow subset (ADR 0006), so an import that is
    fine here can still kill the job.

    A stray import is not hypothetical: `from consumers.enrichment.enrich
    import schedule_deviation` reaches `reference.py` and then `import psycopg`,
    which the image lacks:

        ModuleNotFoundError: No module named 'psycopg'

    The contract tests would all still pass; the job would die at submit, and
    the traceback names psycopg rather than the import that pulled it in.
    """

    FORBIDDEN = ("consumers.enrichment", "producer", "static", "pyflink",
                 "psycopg", "shapely", "minio", "requests")

    def module_imports(self, module_name: str) -> set[str]:
        """Top-level import targets, read from the AST rather than executed."""
        import ast
        import importlib.util

        spec = importlib.util.find_spec(module_name)
        tree = ast.parse(pathlib.Path(spec.origin).read_text())
        names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names.update(a.name for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.add(node.module)
        return names

    @pytest.mark.parametrize("module", ["consumers.bunching.detect",
                                        "consumers.bunching.config"])
    def test_detector_modules_stay_importable_in_the_image(self, module):
        for imported in self.module_imports(module):
            for bad in self.FORBIDDEN:
                assert not imported.startswith(bad), (
                    f"{module} imports {imported}, which the Flink image "
                    f"cannot resolve. See ADR 0006."
                )

    def test_decode_may_use_protobuf_but_not_the_generated_bindings(self):
        """decode.py is the one module that imports protobuf, and it must
        NOT import schemas.*: the image's runtime (5.29.6) refuses gencode
        7.35.1 at import. It builds the class from a descriptor instead."""
        imports = self.module_imports("consumers.bunching.decode")
        assert any(i.startswith("google.protobuf") for i in imports)
        assert not any(i.startswith("schemas") for i in imports)


# =============================================================================
# 1. geometry -- pinned so the gates can be trusted
# =============================================================================


class TestHaversine:
    def test_zero_distance(self):
        assert haversine_ft(AURORA_LAT, AURORA_LON, AURORA_LAT, AURORA_LON) == 0.0

    def test_known_latitude_offset(self):
        """One degree of latitude is ~364,000 ft everywhere on earth."""
        d = haversine_ft(47.0, -122.0, 48.0, -122.0)
        assert 363_000 < d < 366_000

    def test_symmetric(self):
        a = haversine_ft(47.6, -122.3, 47.7, -122.4)
        b = haversine_ft(47.7, -122.4, 47.6, -122.3)
        assert a == pytest.approx(b)


class TestConfirmProximity:
    """Gate 1: does a small shape-distance gap mean the buses are together?

    Measured motivation, not hypothetical: 101 of 280 (route, direction)
    pairs in the loaded feed carry more than one shape, and 92 shape pairs
    start over 500 m apart. Equal distance along two different geometries is
    not adjacency.
    """

    def test_genuine_pair_confirms(self):
        a = position("A", 10_000)
        b = position("B", 10_500, lat=offset_lat(500))
        assert confirm_proximity(a, b, 500, CONFIG.gap_threshold_ft) is True

    def test_incomparable_shapes_rejected(self):
        """The C Line case: two shapes whose origins are 8 km apart.

        Both buses report ~10,000 ft along their own shape, so the gap looks
        like zero and the naive detector alerts. They are five miles apart.
        """
        a = position("A", 10_000)
        b = position("B", 10_050, lat=offset_lat(27_000))  # ~8.2 km north
        assert confirm_proximity(a, b, 50, CONFIG.gap_threshold_ft) is False

    def test_missing_position_refuses(self):
        """Unconfirmable is not the same as confirmed.

        A record without lat/lon is exactly the cross-shape case this gate
        exists to catch, so it must fail closed.
        """
        a = position("A", 10_000)
        b = position("B", 10_100)
        b["latitude"] = None
        assert confirm_proximity(a, b, 100, CONFIG.gap_threshold_ft) is False

    def test_allows_gps_scatter_past_the_threshold(self):
        """A real pair slightly over the bound still confirms.

        Straight-line distance cannot exceed along-route distance for two
        points on one path, but GPS scatter and shape generalisation both
        push measured pairs a little over. The gate allows 25%.
        """
        a = position("A", 10_000)
        b = position("B", 10_950, lat=offset_lat(1_100))
        assert confirm_proximity(a, b, 950, CONFIG.gap_threshold_ft) is True


# =============================================================================
# 2. parse_record
# =============================================================================


class TestParseRecord:
    """parse_record takes a DICT, not a JSON string.

    decode.decode() has already turned the topic's protobuf into a dict by
    the time this runs, so the signature is a dict. The decode tests below
    cover the bytes/dict boundary.
    """

    def decoded(self, **overrides) -> dict:
        """A record shaped as decode.to_dict() produces it.

        29 keys, unset optionals present as None rather than absent -- that
        second part is what makes `rec.get(k)` and `k in rec` behave
        differently, and parse_record has to test the value.
        """
        rec = {
            "vehicle_id": "1234",
            "route_id": "100512",
            "direction_id": 0,
            "trip_id": "t1",
            "route_short_name": "E Line",
            "shape_dist_traveled": 10_000.0,
            "position_timestamp": 1_789_935_300,  # int64, NOT a string
            "latitude": AURORA_LAT,
            "longitude": AURORA_LON,
            "schedule_deviation_seconds": 105,
            "occupancy_status": "MANY_SEATS_AVAILABLE",  # not a detector field
            "neighborhood_name": None,  # unset optional
        }
        rec.update(overrides)
        return rec

    def test_keeps_the_detector_fields(self):
        rec = parse_record(self.decoded())
        assert rec["vehicle_id"] == "1234"
        assert rec["shape_dist_traveled"] == 10_000.0
        assert rec["latitude"] == AURORA_LAT
        assert rec["longitude"] == AURORA_LON

    def test_position_timestamp_stays_numeric(self):
        """The watermark assigner multiplies this by 1000.

        MessageToDict would have rendered int64 as a string, and "1789935300"
        * 1000 is a thousand copies of the string rather than an error.
        """
        rec = parse_record(self.decoded())
        assert isinstance(rec["position_timestamp"], (int, float))

    def test_drops_record_without_shape_distance(self):
        """The vehicle could not be located on its shape, so it cannot be
        ordered against anything. The key is PRESENT and None."""
        assert parse_record(self.decoded(shape_dist_traveled=None)) is None

    def test_drops_record_without_route(self):
        assert parse_record(self.decoded(route_id=None)) is None

    def test_drops_record_without_position(self):
        """confirm_proximity cannot confirm a candidate without lat/lon."""
        assert parse_record(self.decoded(latitude=None)) is None

    def test_garbage_input_returns_none_rather_than_raising(self):
        """An exception here takes down the TaskManager slot, which restarts
        the job from its checkpoint -- so one bad record becomes a crash
        loop, not a dropped record."""
        assert parse_record({}) is None
        assert parse_record(None) is None


# =============================================================================
# 2b. decode -- the bytes/dict boundary
# =============================================================================


class TestStripFraming:
    """Confluent framing, asserted rather than trusted.

    Verified against 3,000 live records: magic 0x00, schema id 3, and a
    single 0x00 message-index byte on every one.
    """

    def test_strips_the_single_message_form(self):
        payload = b"\x0a\x04test"
        assert strip_framing(b"\x00\x00\x00\x00\x03\x00" + payload) == payload

    def test_strips_the_general_indexed_form(self):
        """[1, 0] -- a length-1 index array, the non-optimised spelling.

        Hardcoding a 6-byte skip decodes this one byte short, which shifts
        every field and usually still parses.
        """
        payload = b"\x0a\x04test"
        assert strip_framing(b"\x00\x00\x00\x00\x03\x01\x00" + payload) == payload

    def test_rejects_json(self):
        """A JSON record has `{` (0x7b) where the magic byte belongs."""
        with pytest.raises(ValueError):
            strip_framing(b'{"vehicle_id": "1234"}')

    def test_rejects_short_input(self):
        with pytest.raises(ValueError):
            strip_framing(b"\x00\x00\x00")


class TestDecode:
    """Decodes a REAL record captured off the topic.

    tests/fixtures/enriched_vehicle_position_framed.bin is one message as the
    enrichment consumer actually published it, prefix and all. Asserting
    against invented bytes would prove the test's own encoder works.

    This also covers the version claim: the fixture decodes under protobuf
    7.36.1 here and 5.29.6 in the Flink image, through the same descriptor.
    """

    def test_decodes_to_a_dict(self, record):
        assert record is not None, "the fixture must decode"
        assert record["vehicle_id"] == "8190"

    def test_int64_stays_an_int(self, record):
        """The MessageToDict trap: it renders int64 as a string."""
        assert isinstance(record["position_timestamp"], int)
        assert record["position_timestamp"] > 1_700_000_000

    def test_double_stays_a_float(self, record):
        assert isinstance(record["shape_dist_traveled"], float)

    def test_unset_optional_is_none_not_zero(self, record):
        """ADR 0005's point, at the last step: an unset optional must not
        collapse into the zero that means something else."""
        assert any(v is None for v in record.values())

    def test_undecodable_bytes_return_none(self):
        assert decode(b"not protobuf at all") is None
        assert decode(b"") is None


# =============================================================================
# 3. assign_route_key
# =============================================================================


class TestAssignRouteKey:
    def test_direction_is_part_of_the_key(self):
        north = position("A", 10_000, direction_id=0)
        south = position("B", 10_000, direction_id=1)
        assert assign_route_key(north) != assign_route_key(south)

    def test_same_route_and_direction_share_a_key(self):
        assert assign_route_key(position("A", 1.0)) == assign_route_key(position("B", 2.0))

    def test_shape_is_not_part_of_the_key(self):
        """ADR 0007: shape variants are handled by confirm_proximity.

        Keying on shape would stop comparing A Line direction 1's two shape
        variants against each other -- 101 of 310 trips -- even though they
        share a start point and differ by 44 m in total length.
        """
        a = position("A", 10_000)
        b = position("B", 10_200)
        a["shape_id"], b["shape_id"] = "20675016", "20675017"
        assert assign_route_key(a) == assign_route_key(b)

    def test_key_is_a_string(self):
        """Flink key_by needs a hashable, serialisable key, and a tuple
        crossing the Python/JVM boundary without an explicit type is where
        PyFlink produces a pickled blob instead of an error."""
        assert isinstance(assign_route_key(position("A", 1.0)), str)


# =============================================================================
# 4. detect_in_window -- the core
# =============================================================================


class TestDetectInWindow:
    def test_clear_route_emits_nothing(self):
        recs = [position("A", 10_000, lat=offset_lat(0)),
                position("B", 20_000, lat=offset_lat(10_000)),
                position("C", 30_000, lat=offset_lat(20_000))]
        assert detect_in_window("100512:0", recs, WINDOW_END) == []

    def test_empty_window(self):
        assert detect_in_window("100512:0", [], WINDOW_END) == []

    def test_single_vehicle_cannot_bunch(self):
        assert detect_in_window("100512:0", [position("A", 10_000)], WINDOW_END) == []

    def test_bunched_pair_is_detected(self):
        recs = [position("A", 10_000, lat=offset_lat(0)),
                position("B", 10_400, lat=offset_lat(400))]
        alerts = detect_in_window("100512:0", recs, WINDOW_END)
        assert len(alerts) == 1
        assert alerts[0]["gap_ft"] == pytest.approx(400, abs=1)

    def test_alert_carries_what_makes_it_interpretable(self):
        recs = [position("A", 10_000, lat=offset_lat(0), deviation=480),
                position("B", 10_400, lat=offset_lat(400), deviation=30)]
        a = detect_in_window("100512:0", recs, WINDOW_END)[0]
        for field in ("route_id", "direction_id", "route_short_name",
                      "gap_ft", "window_end"):
            assert field in a, f"alert missing {field}"
        # Both vehicles, both trips, both deviations -- bunching with one bus
        # eight minutes late is a different story from two buses on time.
        deviations = {v for k, v in a.items() if "deviation" in k}
        assert deviations == {480, 30}

    def test_three_in_a_row_is_two_pairs_not_three(self):
        """Consecutive comparison, not all-pairs. All-pairs is O(n^2) and
        double-counts the middle bus against both neighbours AND the outer
        two against each other."""
        recs = [position("A", 10_000, lat=offset_lat(0)),
                position("B", 10_400, lat=offset_lat(400)),
                position("C", 10_800, lat=offset_lat(800))]
        assert len(detect_in_window("100512:0", recs, WINDOW_END)) == 2

    def test_unsorted_input_still_pairs_correctly(self):
        """Window contents arrive in no particular order."""
        recs = [position("C", 10_800, lat=offset_lat(800)),
                position("A", 10_000, lat=offset_lat(0)),
                position("B", 10_400, lat=offset_lat(400))]
        alerts = detect_in_window("100512:0", recs, WINDOW_END)
        assert len(alerts) == 2
        assert all(al["gap_ft"] == pytest.approx(400, abs=1) for al in alerts)

    def test_gap_exactly_at_threshold_does_not_alert(self):
        """Strictly below. A threshold that fires at its own value makes the
        number in config.py mean something slightly different from what it
        says."""
        recs = [position("A", 10_000, lat=offset_lat(0)),
                position("B", 10_000 + CONFIG.gap_threshold_ft,
                         lat=offset_lat(CONFIG.gap_threshold_ft))]
        assert detect_in_window("100512:0", recs, WINDOW_END) == []


class TestDeduplicatesToLatestPerVehicle:
    """A 60s window holds ~3 observations per vehicle at the measured 20s
    publish rate. Comparing all of them counts each pair three times."""

    def test_repeated_observations_produce_one_alert(self):
        recs = []
        for age in (45, 25, 5):
            recs.append(position("A", 10_000 - age * 10,
                                 lat=offset_lat(-age * 10), age_s=age))
            recs.append(position("B", 10_400 - age * 10,
                                 lat=offset_lat(400 - age * 10), age_s=age))
        alerts = detect_in_window("100512:0", recs, WINDOW_END)
        assert len(alerts) == 1

    def test_uses_the_latest_position_not_the_first(self):
        """Vehicle A closes on B during the window. The alert must reflect
        where A ended up, not where it started."""
        recs = [
            position("A", 8_000, lat=offset_lat(-2_000), age_s=50),
            position("A", 10_000, lat=offset_lat(0), age_s=5),
            position("B", 10_300, lat=offset_lat(300), age_s=5),
        ]
        alerts = detect_in_window("100512:0", recs, WINDOW_END)
        assert len(alerts) == 1
        assert alerts[0]["gap_ft"] == pytest.approx(300, abs=1)


class TestStaleGate:
    """CONFIG.max_position_age_s. A stale-burst position measures where a bus
    WAS -- pairing it against a fresh one invents a gap that closed minutes
    ago."""

    def test_stale_vehicle_is_excluded(self):
        recs = [position("A", 10_000, lat=offset_lat(0), age_s=5),
                position("B", 10_400, lat=offset_lat(400),
                         age_s=CONFIG.max_position_age_s + 30)]
        assert detect_in_window("100512:0", recs, WINDOW_END) == []

    def test_fresh_vehicles_both_kept(self):
        recs = [position("A", 10_000, lat=offset_lat(0),
                         age_s=CONFIG.max_position_age_s - 10),
                position("B", 10_400, lat=offset_lat(400), age_s=5)]
        assert len(detect_in_window("100512:0", recs, WINDOW_END)) == 1


class TestTerminalLayoverGate:
    """Gate 2, and the reason MIN_PROGRESS_FT exists.

    Measured on 8,000 live records: 12.6% report shape_dist_traveled of
    exactly 0.0, and 95.6% of those are STOPPED_AT. Sampled against PostGIS,
    every one sits 1.7-86.5 m from its shape's start point -- so the
    projection is right and these are buses waiting at the first stop. Two of
    them have a gap of zero and would alert on a layover.
    """

    def test_two_vehicles_at_the_terminal_do_not_alert(self):
        recs = [position("A", 0.0, lat=offset_lat(0)),
                position("B", 0.0, lat=offset_lat(20))]
        assert detect_in_window("100512:0", recs, WINDOW_END) == []

    def test_one_at_terminal_one_under_way_does_not_alert(self):
        recs = [position("A", 0.0, lat=offset_lat(0)),
                position("B", 300.0, lat=offset_lat(300))]
        assert detect_in_window("100512:0", recs, WINDOW_END) == []

    def test_vehicles_still_at_their_first_stops_do_not_alert(self):
        """Route 255's failure mode, and the reason MIN_STOP_SEQUENCE exists.

        Distance alone cannot see this: these buses are 1,600 ft along a
        74,000 ft shape, well past MIN_PROGRESS_FT, and stacked at Totem Lake
        Transit Center waiting to depart. 87% of that route's alerts were
        this, with a median gap of 0 ft.
        """
        recs = [position("A", 1_600, lat=offset_lat(0), stop_seq=2),
                position("B", 1_620, lat=offset_lat(20), stop_seq=3)]
        assert detect_in_window("100512:0", recs, WINDOW_END) == []

    def test_one_vehicle_still_at_its_first_stops_does_not_alert(self):
        """The pair is only as trustworthy as its less-advanced vehicle."""
        recs = [position("A", 1_600, lat=offset_lat(0), stop_seq=2),
                position("B", 2_000, lat=offset_lat(400), stop_seq=20)]
        assert detect_in_window("100512:0", recs, WINDOW_END) == []

    def test_missing_stop_sequence_fails_closed(self):
        """99.6% populated, so the rare absence is cheaper to drop than to
        admit a vehicle whose progress cannot be checked."""
        recs = [position("A", 10_000, lat=offset_lat(0), stop_seq=None),
                position("B", 10_400, lat=offset_lat(400), stop_seq=None)]
        assert detect_in_window("100512:0", recs, WINDOW_END) == []

    def test_vehicles_past_both_gates_alert(self):
        recs = [position("A", 10_000, lat=offset_lat(0), stop_seq=MIN_STOP_SEQUENCE),
                position("B", 10_400, lat=offset_lat(400), stop_seq=MIN_STOP_SEQUENCE)]
        assert len(detect_in_window("100512:0", recs, WINDOW_END)) == 1

    def test_vehicles_past_the_gate_still_alert(self):
        recs = [position("A", MIN_PROGRESS_FT + 50, lat=offset_lat(0)),
                position("B", MIN_PROGRESS_FT + 450, lat=offset_lat(400))]
        assert len(detect_in_window("100512:0", recs, WINDOW_END)) == 1


class TestProximityGateInsideDetection:
    """Gate 1 applied where it matters -- inside the detector, not only as a
    helper."""

    def test_incomparable_shapes_do_not_alert(self):
        """Both report ~10,000 ft along their own shape. They are 8 km
        apart on the ground, which is the 92-shape-pair population from
        ADR 0007."""
        recs = [position("A", 10_000, lat=offset_lat(0)),
                position("B", 10_050, lat=offset_lat(27_000))]
        assert detect_in_window("100512:0", recs, WINDOW_END) == []

    def test_compatible_shape_variants_still_alert(self):
        """The A Line direction 1 case. Two shapes, shared origin, 44 m
        difference in total length -- these vehicles ARE comparable, and a
        shape-keyed detector would never have compared them."""
        a = position("A", 10_000, lat=offset_lat(0))
        b = position("B", 10_400, lat=offset_lat(400))
        a["shape_id"], b["shape_id"] = "10671009", "10671010"
        assert len(detect_in_window("100512:0", [a, b], WINDOW_END)) == 1


# =============================================================================
# 7. cross-window state -- the cooldown and the consecutive-window run
#
# These are the two CONFIG values that no single window can express. They are
# pinned here rather than tested through the cluster because BunchingState is a
# dataclass in detect.py with no Flink in it, which is the point of the split:
# a cooldown reachable only through a running job has no test path.
# =============================================================================


class TestConsecutiveWindows:
    """CONFIG.min_consecutive_windows: one window is jitter, two is a pattern."""

    def test_first_window_does_not_alert(self):
        """A pair seen bunched once is not yet an alert."""
        emit, state = BunchingState().emit(WINDOW_END)
        assert emit is False
        assert state.consecutive == 1

    def test_second_consecutive_window_alerts(self):
        _, first = BunchingState().emit(WINDOW_END)
        emit, state = first.emit(WINDOW_END + CONFIG.window_s)
        assert emit is True
        assert state.consecutive == 2

    def test_adjacent_windows_are_not_a_gap(self):
        """Exactly one window_s apart is consecutive. The comparison is >,
        not >=, and getting that wrong would mean no pair ever alerted."""
        _, first = BunchingState().emit(WINDOW_END)
        _, second = first.emit(WINDOW_END + CONFIG.window_s)
        assert second.consecutive == 2

    def test_a_missed_window_restarts_the_run(self):
        """Bunched, absent, bunched again: one window in the second run is not
        two consecutive windows, however the timestamps read."""
        _, first = BunchingState().emit(WINDOW_END)
        emit, state = first.emit(WINDOW_END + 2 * CONFIG.window_s)
        assert emit is False
        assert state.consecutive == 1


class TestCooldown:
    """CONFIG.cooldown_s: suppress the repeat, not the detection."""

    def _bunched(self) -> BunchingState:
        """State for a pair that has already alerted at WINDOW_END + window_s."""
        _, first = BunchingState().emit(WINDOW_END)
        emit, second = first.emit(WINDOW_END + CONFIG.window_s)
        assert emit is True, "the fixture pair has to have alerted"
        return second

    def test_no_realert_inside_the_cooldown(self):
        state = self._bunched()
        emit, _ = state.emit(WINDOW_END + 2 * CONFIG.window_s)
        assert emit is False

    def test_realerts_at_the_cooldown_boundary(self):
        """A pair still bunched ten minutes later is still news. Suppressing it
        forever would hide a bus stuck behind another one."""
        state = self._bunched()
        alert_at = state.last_alert_s
        t = alert_at + CONFIG.window_s
        while t < alert_at + CONFIG.cooldown_s:
            emit, state = state.emit(t)
            assert emit is False, "nothing inside the cooldown should emit"
            t += CONFIG.window_s
        emit, _ = state.emit(alert_at + CONFIG.cooldown_s)
        assert emit is True

    def test_counting_continues_while_suppressed(self):
        state = self._bunched()
        _, next_state = state.emit(WINDOW_END + 2 * CONFIG.window_s)
        assert next_state.consecutive == 3

    def test_suppressed_windows_do_not_move_the_alert_time(self):
        """Otherwise each window inside the cooldown would push the next alert
        out by another window_s, and a pair bunched every window would never
        re-alert at all."""
        state = self._bunched()
        _, next_state = state.emit(WINDOW_END + 2 * CONFIG.window_s)
        assert next_state.last_alert_s == state.last_alert_s

    def test_cooldown_survives_a_separation(self):
        """Separating and rejoining inside the cooldown is jitter, not a new
        episode: the run restarts but the alert is still suppressed. Flip this
        assertion to change the policy."""
        state = self._bunched()
        _, state = state.emit(WINDOW_END + 300)       # apart in between
        emit, state = state.emit(WINDOW_END + 360)    # bunched twice running again
        assert state.consecutive == 2, "the run restarted, as it should"
        assert emit is False

    def test_a_new_episode_after_the_cooldown_alerts(self):
        """The other side of the same policy: past the cooldown, a fresh run is
        a fresh alert."""
        state = self._bunched()
        _, state = state.emit(WINDOW_END + 660)       # apart, past the cooldown
        emit, _ = state.emit(WINDOW_END + 720)
        assert emit is True


class TestPairKey:
    """The cooldown key has to survive the pair changing order."""

    def test_order_does_not_matter(self):
        assert (pair_key({"vehicle_id_a": "v1", "vehicle_id_b": "v2"})
                == pair_key({"vehicle_id_a": "v2", "vehicle_id_b": "v1"}))

    def test_distinct_pairs_have_distinct_keys(self):
        assert (pair_key({"vehicle_id_a": "v1", "vehicle_id_b": "v2"})
                != pair_key({"vehicle_id_a": "v1", "vehicle_id_b": "v3"}))

    def test_key_is_stable_when_the_pair_overtakes(self):
        """The bug this exists for. detect_in_window orders each pair by
        shape_dist_traveled, so when the faster vehicle passes the slower one
        the lead changes and vehicle_id_a flips. An order-sensitive key would
        split that pair's history across two state entries, consecutive would
        never reach min_consecutive_windows, and the detector would emit
        nothing -- with a healthy-looking job."""
        together = detect_in_window(
            "100512:0", [position("v1", 200), position("v2", 700)], WINDOW_END)
        overtaken = detect_in_window(
            "100512:0", [position("v1", 900), position("v2", 700)],
            WINDOW_END + CONFIG.window_s)
        assert together[0]["vehicle_id_a"] == "v1"
        assert overtaken[0]["vehicle_id_a"] == "v2", "the pair has to have swapped"
        assert pair_key(together[0]) == pair_key(overtaken[0])
