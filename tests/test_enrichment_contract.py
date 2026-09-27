"""Executable specification for the enrichment contract.

Marked `contract`, so excluded from `make test` and CI like the Phase 1 ones.

    make contract-p2                 # all of them
    make contract-p2 K=interpolate   # narrow

These need no broker, no warehouse, and no network. The ones that would need
a live stack (loading 1.1M stop_times, registering against the registry) are
verified by `make static-load` and `make schema-register` instead, because
faking them would test the fake.

The warehouse-backed sections skip cleanly when no stack is running; the
rest need nothing but the venv.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from consumers.enrichment.enrich import interpolate, service_date_origin
from consumers.enrichment.reference import resolve_version
from producer.errors import DlqReason
from static import feed as static_feed

# For the one test that reads a DAG's SOURCE rather than importing it: this
# venv has no airflow, and Airflow's image has no psycopg. Neither can import
# the other, so the contract between them is checked as text.
REPO_ROOT = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.contract


# =============================================================================
# 1. helpers -- pin the pure arithmetic everything else leans on
# =============================================================================


class TestInterpolate:
    def test_midpoint(self):
        assert interpolate([0.0, 10.0], [100, 200], 5.0) == 150.0

    def test_exact_endpoints(self):
        assert interpolate([0.0, 10.0], [100, 200], 0.0) == 100.0
        assert interpolate([0.0, 10.0], [100, 200], 10.0) == 200.0

    def test_outside_range_returns_none(self):
        """Extrapolating off the end of a schedule produces a confident
        number that is wrong. None is the honest answer."""
        assert interpolate([0.0, 10.0], [100, 200], -1.0) is None
        assert interpolate([0.0, 10.0], [100, 200], 11.0) is None

    def test_too_few_points(self):
        assert interpolate([5.0], [100], 5.0) is None

    def test_duplicate_x_does_not_divide_by_zero(self):
        """Two timepoints at the same shape_dist_traveled is legal GTFS --
        a bus scheduled to wait at a stop."""
        assert interpolate([0.0, 5.0, 5.0, 10.0], [0, 60, 90, 120], 5.0) is not None


class TestServiceDateOrigin:
    """A GTFS service date begins at local midnight in the AGENCY's timezone,
    not UTC midnight."""

    def test_origin_is_agency_local_not_utc(self):
        got = service_date_origin("20260914")
        assert got == datetime(2026, 9, 14, 7, tzinfo=timezone.utc), (
            "PDT is UTC-7; UTC midnight would be 7 hours early on every trip"
        )
        assert got != datetime(2026, 9, 14, tzinfo=timezone.utc)

    def test_handles_standard_time(self):
        """PST is UTC-8, so the offset is not a constant."""
        assert service_date_origin("20261215") == datetime(2026, 12, 15, 8, tzinfo=timezone.utc)

    def test_dst_days_are_23_and_25_hours(self):
        """The GTFS spec anchors on noon minus 12h precisely so these work.
        Anchoring on midnight gets both of these wrong by an hour, and 4.5% of
        Metro trips run past midnight into exactly this window."""
        spring = service_date_origin("20260309") - service_date_origin("20260308")
        fall = service_date_origin("20261102") - service_date_origin("20261101")
        assert spring.total_seconds() / 3600 == 23
        assert fall.total_seconds() / 3600 == 25

    def test_accepts_both_date_spellings(self):
        """The same date arrives in two formats because it crosses a
        serialization boundary: "20260903" off the protobuf, "2026-09-03"
        after publish.serialize() round-trips it through json.dumps.

        enrich_v2 receives the ISO form; parsing only the GTFS form returns
        None for every record and looks exactly like "no schedule".
        """
        from datetime import date as _date
        expected = datetime(2026, 9, 3, 7, tzinfo=timezone.utc)
        assert service_date_origin("20260903") == expected
        assert service_date_origin("2026-09-03") == expected
        assert service_date_origin(_date(2026, 9, 3)) == expected

    def test_rejects_garbage(self):
        assert service_date_origin("") is None
        assert service_date_origin(None) is None
        assert service_date_origin("garbage") is None
        assert service_date_origin("20260931") is None  # no 31st September


# =============================================================================
# 2. the static manifest
# =============================================================================


class TestStaticManifest:
    def test_neighborhood_source_is_county_not_seattle(self):
        """83.4% coverage vs 58.4% -- Metro serves the whole county, and a
        Seattle-only layer silently turns a county mart into a Seattle mart."""
        assert "NEIGHBORHOOD_AREA_384" in static_feed.NEIGHBORHOODS_URL
        assert static_feed.NEIGHBORHOOD_COVERAGE > 0.80

    def test_neighborhood_query_requests_wgs84(self):
        """The layer is natively EPSG:2926. GTFS-RT is WGS84. Asking the
        service to reproject avoids a pyproj dependency for one static layer."""
        assert "outSR=4326" in static_feed.SOURCE.neighborhoods_geojson

    def test_trips_carries_block_id(self):
        """The field ADR 0003 deferred, available free from the static join."""
        trips = next(t for t in static_feed.GTFS_TABLES if t.table == "trips")
        assert "block_id" in trips.columns
        assert "shape_id" in trips.columns

    def test_stop_times_carries_shape_dist_traveled(self):
        """Without it, schedule deviation needs linear referencing against
        every stop rather than a subtraction."""
        st = next(t for t in static_feed.GTFS_TABLES if t.table == "stop_times")
        assert "shape_dist_traveled" in st.columns

    def test_expected_row_counts_are_present(self):
        """An order-of-magnitude miss means a truncated download, not a
        busy Tuesday -- but only if there is something to compare against."""
        assert all(t.expect_rows > 0 for t in static_feed.GTFS_TABLES)

    def test_removed_extension_files_are_not_required(self):
        """block.txt and block_trip.txt were DELETED by the 2026-09-14 service
        change. Nothing may depend on them, and block_id must still come from
        trips.txt -- which is exactly why ADR 0003 sources it there."""
        required = {t.filename for t in static_feed.GTFS_TABLES}
        assert "block_trip.txt" not in required
        assert "block.txt" not in required


class TestFeedInfo:
    """feed_info.txt carries the agency's own version label.

    Both identifiers are kept because they answer different questions: the
    ETag is byte identity (what conditional GET needs), feed_version is
    schedule identity (what a human recognises).
    """

    def test_parses_the_published_row(self):
        rows = [{
            "feed_publisher_name": "Metro Transit",
            "feed_version": "FAL26-161.1",
            "feed_start_date": "20260914",
            "feed_end_date": "20270326",
        }]
        info = static_feed.parse_feed_info(rows)
        assert info.feed_version == "FAL26-161.1"
        assert info.feed_start_date == "20260914"
        assert info.publisher_name == "Metro Transit"

    def test_absent_feed_info_is_not_an_error(self):
        """Optional in the GTFS spec, and absent from every Metro feed before
        2026-09-14. A loader that required it would have failed on all of them."""
        info = static_feed.parse_feed_info([])
        assert info.feed_version is None
        assert info.covers("20260915") is None, "unknown window must not read as False"

    def test_empty_strings_become_none(self):
        info = static_feed.parse_feed_info([{"feed_version": "  ", "feed_start_date": ""}])
        assert info.feed_version is None
        assert info.feed_start_date is None

    def test_validity_window_bounds_the_service_date(self):
        """The window is the only thing that answers 'old version or new one'
        for an in-flight trip. Neither identifier can."""
        info = static_feed.parse_feed_info([{
            "feed_version": "FAL26-161.1",
            "feed_start_date": "20260914", "feed_end_date": "20270326",
        }])
        assert info.covers("20260914") is True   # inclusive lower bound
        assert info.covers("20270326") is True   # inclusive upper bound
        assert info.covers("20260913") is False
        assert info.covers("20270327") is False


class TestStaticLoadExitCodes:
    """The loader's exit codes, and the DAG that depends on one of them.

    `--load` exits 0 when it loaded a new version and 99 when the ETag has not
    moved, so `transit_static_refresh` can tell a real load from a no-op day: an
    unchanged morning is SKIPPED rather than a green run that did nothing.
    """

    def test_unchanged_is_99_and_not_zero(self):
        """99 is the parcel project's convention for "nothing to do". What
        matters is that it is neither 0 (loaded) nor 1 (a real failure), since
        Airflow treats those as success and failure."""
        from static.run import EXIT_UNCHANGED

        assert EXIT_UNCHANGED == 99
        assert EXIT_UNCHANGED not in (0, 1)

    def test_the_dag_skips_exactly_the_code_the_loader_returns(self):
        """The number is written down twice, and nothing else checks that the
        two copies agree.

        airflow/dags/transit_static_refresh.py cannot import static.run --
        Airflow's image carries neither psycopg nor shapely, which is the same
        separation that makes the task a DockerOperator. So this reads the DAG's
        source rather than importing it, the way the import-isolation tests
        read theirs.
        """
        from static.run import EXIT_UNCHANGED

        dag_src = (REPO_ROOT / "airflow" / "dags"
                   / "transit_static_refresh.py").read_text()
        assert f"UNCHANGED_EXIT_CODE = {EXIT_UNCHANGED}" in dag_src, (
            "the DAG and static/run.py disagree about the 'feed unchanged' exit "
            "code, so an unchanged day would fail the task instead of skipping it"
        )
        assert "skip_on_exit_code=UNCHANGED_EXIT_CODE" in dag_src, (
            "the DAG defines the code but never hands it to the operator"
        )


# =============================================================================
# 3. ReferenceData
# =============================================================================
# These need a warehouse with a loaded static version, so they skip when one
# is absent rather than failing. `make static-load` makes them run.


@pytest.fixture(scope="module")
def ref():
    psycopg = pytest.importorskip("psycopg")
    import os

    from dotenv import load_dotenv
    load_dotenv()
    try:
        conn = psycopg.connect(
            f"host={os.environ.get('WAREHOUSE_HOST','localhost')} "
            f"port={os.environ.get('WAREHOUSE_PORT','5434')} "
            f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} "
            f"password={os.environ['POSTGRES_PASSWORD']}",
            connect_timeout=3,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no warehouse: {exc}")

    from consumers.enrichment.reference import ReferenceData
    with conn:
        try:
            version, feed_version = resolve_version(conn)
        except Exception as exc:  # noqa: BLE001 -- missing schema or no load yet
            pytest.skip(f"no static version loaded: {exc}")
        data = ReferenceData(version, feed_version)
        data.load(conn)
    return data


class TestReferenceData:
    def test_loads_every_index(self, ref):
        s = ref.stats
        assert s.trips > 30_000, "trips.txt is 32,060 rows"
        assert s.shapes > 400, "431 distinct shapes"
        assert s.schedules > 20_000, "most trips should have a schedule"
        assert s.neighborhoods == 350

    def test_known_trip_resolves(self, ref):
        trip_id = next(iter(ref._trips))
        got = ref.trip(trip_id)
        assert got is not None and got.trip_id == trip_id
        assert got.route_id, "the routes join should have come along"

    def test_unknown_trip_returns_none_and_counts_a_miss(self, ref):
        before = ref.stats.trip_misses
        assert ref.trip("definitely-not-a-trip-id") is None
        assert ref.stats.trip_misses == before + 1

    def test_neighborhood_hits_downtown_seattle(self, ref):
        """Westlake, squarely inside a neighborhood polygon."""
        assert ref.neighborhood(-122.3365, 47.6119) is not None

    def test_neighborhood_returns_none_far_outside(self, ref):
        """Null is NORMAL -- 16.6% of the fleet is outside every polygon.
        This must never be a DLQ case."""
        assert ref.neighborhood(-122.9, 45.5) is None  # Portland

    def test_locate_on_shape_is_in_feed_units(self, ref):
        """project() returns DEGREES on a WGS84 line, which are comparable
        to nothing. The result must be normalised back to feed units so it
        can be subtracted from stop_times.shape_dist_traveled."""
        shape_id = next(iter(ref._shapes))
        shape = ref._shapes[shape_id]
        if not shape.max_dist:
            pytest.skip("shape has no max_dist")
        mid = shape.geom.interpolate(0.5, normalized=True)
        got = ref.locate_on_shape(shape_id, mid.x, mid.y)
        assert got is not None
        assert 0 <= got <= shape.max_dist
        assert got == pytest.approx(shape.max_dist / 2, rel=0.15)


# =============================================================================
# 4. enrich_v1
# =============================================================================


RAW = {
    "vehicle_id": "4365", "position_timestamp": "2026-09-05T20:15:00+00:00",
    "trip_id": "PLACEHOLDER", "route_id": "100001", "direction_id": 0,
    "start_date": "2026-09-05", "latitude": 47.6059, "longitude": -122.3343,
    "bearing": None, "speed": None, "current_status": "IN_TRANSIT_TO",
    "current_stop_sequence": 7, "stop_id": "558", "occupancy_status": "EMPTY",
    "block_id": None, "feed_etag": "test-etag",
}


class TestEnrichV1:
    def test_unknown_trip_routes_to_dlq(self, ref):
        from consumers.enrichment.enrich import enrich_v1
        out = enrich_v1({**RAW, "trip_id": "not-a-real-trip"}, ref)
        assert not out.ok
        assert out.reason == DlqReason.UNKNOWN_TRIP_ID
        assert "not-a-real-trip" in out.detail

    def test_known_trip_enriches(self, ref):
        from consumers.enrichment.enrich import enrich_v1
        trip_id = next(iter(ref._trips))
        out = enrich_v1({**RAW, "trip_id": trip_id}, ref)
        assert out.ok
        r = out.record
        assert r["trip_id"] == trip_id
        assert r["static_feed_version"] == ref.version_id, "lineage is not optional"
        assert "enriched_at" in r

    def test_carries_both_version_identifiers(self, ref):
        """The surrogate key is exact but only resolvable against this
        warehouse; the agency label is portable and human-recognisable.
        They fail in opposite directions, so carry both."""
        from consumers.enrichment.enrich import enrich_v1
        out = enrich_v1({**RAW, "trip_id": next(iter(ref._trips))}, ref)
        assert out.record["static_feed_version"] == ref.version_id
        assert out.record.get("gtfs_feed_version") == ref.feed_version

    def test_raw_fields_are_carried_through(self, ref):
        """The enriched record is a SUPERSET. A consumer must never have to
        join back to raw to recover the position it started from."""
        from consumers.enrichment.enrich import enrich_v1
        out = enrich_v1({**RAW, "trip_id": next(iter(ref._trips))}, ref)
        for field in ("vehicle_id", "latitude", "longitude", "current_status",
                      "occupancy_status", "stop_id"):
            assert field in out.record, f"{field} was dropped"

    def test_v1_adds_no_spatial_fields(self, ref):
        """v1 -> v2 IS the schema evolution event (ADR 0005). Adding spatial
        fields to v1 forfeits the demonstration."""
        from consumers.enrichment.enrich import enrich_v1
        out = enrich_v1({**RAW, "trip_id": next(iter(ref._trips))}, ref)
        for field in ("shape_dist_traveled", "schedule_deviation_seconds",
                      "neighborhood_name", "neighborhood_num"):
            assert field not in out.record, f"{field} belongs to v2"

    def test_block_id_comes_from_static_not_raw(self, ref):
        """Raw positions never carry block_id -- it is absent from the basic
        protobuf entirely. Any value here came from the static join."""
        from consumers.enrichment.enrich import enrich_v1
        with_block = [t for t in ref._trips.values() if t.block_id]
        if not with_block:
            pytest.skip("no trips with block_id")
        out = enrich_v1({**RAW, "trip_id": with_block[0].trip_id}, ref)
        assert out.record["block_id"] == with_block[0].block_id


# =============================================================================
# 5. enrich_v2 -- Phase 2D
# =============================================================================


class TestEnrichV2:
    def test_v2_is_a_superset_of_v1(self, ref):
        from consumers.enrichment.enrich import enrich_v1, enrich_v2
        trip_id = next(iter(ref._trips))
        v1 = enrich_v1({**RAW, "trip_id": trip_id}, ref)
        v2 = enrich_v2({**RAW, "trip_id": trip_id}, ref)
        assert v2.ok
        assert set(v1.record) <= set(v2.record), "v2 dropped a v1 field"

    def test_v2_inherits_the_dlq_path(self, ref):
        from consumers.enrichment.enrich import enrich_v2
        out = enrich_v2({**RAW, "trip_id": "not-a-real-trip"}, ref)
        assert not out.ok and out.reason == DlqReason.UNKNOWN_TRIP_ID

    def test_null_neighborhood_is_not_a_reject(self, ref):
        """A vehicle on the water taxi is not a data quality problem."""
        from consumers.enrichment.enrich import enrich_v2
        trip_id = next(iter(ref._trips))
        out = enrich_v2({**RAW, "trip_id": trip_id,
                         "latitude": 45.5, "longitude": -122.9}, ref)
        assert out.ok, "outside-the-county must enrich, with a null neighborhood"
        assert out.record.get("neighborhood_name") is None

    def test_deviation_is_none_outside_the_schedule_range(self, ref):
        from consumers.enrichment.enrich import schedule_deviation
        trip_id = next(iter(ref._schedules))
        got = schedule_deviation(ref, trip_id, -1.0,
                                 datetime.now(timezone.utc), "20260914")
        assert got is None, "before the first timepoint there is nothing to interpolate"

    def test_deviation_is_none_without_a_service_date(self, ref):
        """start_date comes from the realtime record, not TripRef -- trips.txt
        has no date. Without it there is no origin to anchor the offset to."""
        from consumers.enrichment.enrich import schedule_deviation
        trip_id = next(iter(ref._schedules))
        sched = ref._schedules[trip_id]
        mid = (sched.dists[0] + sched.dists[-1]) / 2
        assert schedule_deviation(ref, trip_id, mid,
                                  datetime.now(timezone.utc), None) is None

    def test_on_time_vehicle_deviates_near_zero(self, ref):
        """The end-to-end arithmetic check. Place a vehicle exactly where the
        schedule says it should be at a given instant; the deviation must be
        ~0, not ~25,200 (a timezone error) and not ~86,400 (a service-date one).
        """
        from consumers.enrichment.enrich import schedule_deviation, service_date_origin
        from datetime import timedelta

        trip_id = next(iter(ref._schedules))
        sched = ref._schedules[trip_id]
        i = len(sched.dists) // 2
        dist, offset = sched.dists[i], sched.seconds[i]

        origin = service_date_origin("20260914")
        observed_at = origin + timedelta(seconds=offset)

        got = schedule_deviation(ref, trip_id, dist, observed_at, "20260914")
        assert got is not None
        assert abs(got) <= 1, f"expected ~0s, got {got}s"

    def test_known_failure_magnitudes_exceed_the_bound(self):
        """The bound is only useful if it sits below every failure mode it is
        meant to catch. Pure arithmetic on the constant -- no stack needed."""
        from producer.errors import MAX_PLAUSIBLE_DEVIATION_S
        for seconds, label in [
            (25_200, "UTC-midnight origin instead of agency-local (7h)"),
            (86_400, "observation calendar date instead of service date (24h)"),
            (4_294_967_296 - 300, "int32 -> uint32 signedness wrap"),
        ]:
            assert seconds > MAX_PLAUSIBLE_DEVIATION_S, f"bound would miss: {label}"

        # ...and above the measured live extreme, or it would null good data.
        # 1,204 enriched records ranged -2,069s to +1,905s.
        assert MAX_PLAUSIBLE_DEVIATION_S > 2_100

    def test_implausible_deviation_is_nulled_and_counted(self, ref):
        """Nulled rather than rejected: the position, neighborhood and shape
        distance on that record are all still good."""
        from consumers.enrichment.enrich import STATS, schedule_deviation, service_date_origin
        from datetime import timedelta

        trip_id = next(iter(ref._schedules))
        sched = ref._schedules[trip_id]
        i = len(sched.dists) // 2
        # A vehicle "observed" 24 hours off its scheduled instant -- the
        # service-date-anchor failure mode.
        observed_at = (service_date_origin("20260914")
                       + timedelta(seconds=sched.seconds[i] + 86_400))

        before = STATS.implausible_deviation
        got = schedule_deviation(ref, trip_id, sched.dists[i], observed_at, "20260914")
        assert got is None
        assert STATS.implausible_deviation == before + 1

    def test_plausible_deviation_is_counted_as_computed(self, ref):
        from consumers.enrichment.enrich import STATS, schedule_deviation, service_date_origin
        from datetime import timedelta

        trip_id = next(iter(ref._schedules))
        sched = ref._schedules[trip_id]
        i = len(sched.dists) // 2
        observed_at = (service_date_origin("20260914")
                       + timedelta(seconds=sched.seconds[i] + 300))

        before = STATS.deviations_computed
        got = schedule_deviation(ref, trip_id, sched.dists[i], observed_at, "20260914")
        assert got is not None
        assert STATS.deviations_computed == before + 1

    def test_late_vehicle_is_positive(self, ref):
        """Sign convention: behind schedule is POSITIVE. Getting this backwards
        makes every chronically late route look like it is running early."""
        from consumers.enrichment.enrich import schedule_deviation, service_date_origin
        from datetime import timedelta

        trip_id = next(iter(ref._schedules))
        sched = ref._schedules[trip_id]
        i = len(sched.dists) // 2
        observed_at = (service_date_origin("20260914")
                       + timedelta(seconds=sched.seconds[i] + 300))

        got = schedule_deviation(ref, trip_id, sched.dists[i], observed_at, "20260914")
        assert got is not None and 290 <= got <= 310, f"5 min late should be ~+300, got {got}"
