"""Executable specification for the producer stubs.

These FAIL until you implement the stubs, and that is the point -- they are
the contract, not an afterthought. Implement until green.

They are marked `contract` and excluded from `make test` and from CI, so a
half-finished producer does not turn the build red. Run them with:

    make contract                    # all of them
    make contract K=dedupe           # just the dedupe ones
    .venv/bin/pytest tests/test_producer_contract.py -m contract -v

Nothing here needs a broker, a warehouse, or the network. Fetch is tested
against a stub session; decode against the committed fixtures.

Order of attack -- each builds on the last:

    1. dedupe    pure logic, no I/O, fastest feedback
    2. decode    against real captured payloads
    3. fetch     conditional GET state machine
    4. run       process_feed, verified end to end with `--dry-run --once`
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from producer import feeds
from producer.decode import (
    ServiceAlertRecord,
    TripUpdateRecord,
    VehiclePositionRecord,
    decode_service_alerts,
    decode_trip_updates,
    decode_vehicle_positions,
)
from producer.dedupe import LastValueCache
from producer.errors import FeedError
from producer.fetch import ConditionalFetcher

pytestmark = pytest.mark.contract

FIXTURES = Path(__file__).parent / "fixtures"


# =============================================================================
# 1. dedupe
# =============================================================================


@dataclass(frozen=True)
class FakeRecord:
    """Minimal stand-in exposing the properties dedupe uses.

    `dedupe_key` is optional: None means the Kafka key is the dedupe identity,
    which is the case for positions and alerts. Trip updates set it to
    (trip_id, stop_id, stop_sequence) -- see producer/dedupe.py.
    """

    key: str
    fingerprint: tuple
    dedupe_key: str | None = None


class TestLastValueCache:
    def test_first_sighting_passes(self):
        cache = LastValueCache()
        records = [FakeRecord("a", (1,)), FakeRecord("b", (1,))]
        assert cache.filter_new(records) == records

    def test_identical_repeat_is_suppressed(self):
        """The dominant case: FULL_DATASET feeds restate everything."""
        cache = LastValueCache()
        records = [FakeRecord("a", (1,)), FakeRecord("b", (1,))]
        cache.filter_new(records)
        assert cache.filter_new(records) == []

    def test_changed_fingerprint_passes(self):
        """THE case that matters. Get this wrong and you have keyed dedupe,
        which deletes the prediction-accuracy analysis."""
        cache = LastValueCache()
        cache.filter_new([FakeRecord("a", (1,))])

        changed = FakeRecord("a", (2,))
        assert cache.filter_new([changed]) == [changed]

        # ...and the new value must now be what is remembered.
        assert cache.filter_new([FakeRecord("a", (2,))]) == []

    def test_order_is_preserved(self):
        cache = LastValueCache()
        records = [FakeRecord(k, (1,)) for k in "dcba"]
        assert [r.key for r in cache.filter_new(records)] == ["d", "c", "b", "a"]

    def test_intra_batch_duplicates_are_suppressed(self):
        """Cache must update as it goes, not after the loop."""
        cache = LastValueCache()
        dup = FakeRecord("a", (1,))
        assert cache.filter_new([dup, dup, dup]) == [dup]

    def test_trip_update_style_identity_dedupes_per_stop(self):
        """One Kafka key (trip_id) carries many stops; the dedupe identity is
        per stop (ADR 0004). Records sharing a key but with distinct
        dedupe_keys all pass on the first poll, all suppress when restated
        unchanged, and a changed prediction for one stop passes alone."""
        cache = LastValueCache()
        poll_1 = [
            FakeRecord("T1", ("arr1",), dedupe_key="T1:1:1"),
            FakeRecord("T1", ("arr2",), dedupe_key="T1:2:2"),
        ]
        assert cache.filter_new(poll_1) == poll_1
        assert cache.filter_new(poll_1) == []

        changed = FakeRecord("T1", ("arr1b",), dedupe_key="T1:1:1")
        assert cache.filter_new([changed]) == [changed]

    def test_stats_are_tracked(self):
        cache = LastValueCache()
        batch = [FakeRecord("a", (1,)), FakeRecord("b", (1,))]
        cache.filter_new(batch)
        cache.filter_new(batch)

        assert cache.stats.seen == 4
        assert cache.stats.passed == 2
        assert cache.stats.suppressed == 2
        assert cache.stats.suppression_rate == 0.5

    def test_suppression_refreshes_lru_position(self):
        """A suppressed record is still USED, so it must not age toward
        eviction. Without a touch on the suppression path, a long-stable
        prediction drifts to the eviction end, gets dropped, and then
        re-emits as though it were new -- a spurious republish of data that
        did not change.
        """
        cache = LastValueCache(maxsize=2)
        stable = FakeRecord("stable", (1,))
        cache.filter_new([stable])
        cache.filter_new([FakeRecord("b", (1,))])

        # `stable` is suppressed here -- which should refresh it, making "b"
        # the least recently used and therefore the eviction victim.
        assert cache.filter_new([stable]) == []
        cache.filter_new([FakeRecord("c", (1,))])

        assert cache.filter_new([stable]) == [], "stable was evicted despite being used"

    def test_per_feed_maxsize_is_honoured(self):
        """Sizing lives on FeedSpec, because one global number cannot serve
        feeds that differ by an order of magnitude in identities per poll."""
        assert feeds.get("trip_updates").dedupe_maxsize > 250_000, (
            "trip updates carry ~18.6k stop predictions per poll at an evening "
            "trough and ~67k+ at peak; anything near 60k thrashes by morning"
        )
        assert (
            feeds.get("trip_updates").dedupe_maxsize
            > feeds.get("vehicle_positions").dedupe_maxsize
        )

    def test_eviction_is_bounded(self):
        """A process meant to run for days must not grow without limit."""
        cache = LastValueCache(maxsize=10)
        cache.filter_new([FakeRecord(str(i), (1,)) for i in range(100)])
        assert len(cache) <= 10


# =============================================================================
# 2. decode
# =============================================================================


class TestDecodeVehiclePositions:
    @pytest.fixture(scope="class")
    def records(self) -> list[VehiclePositionRecord]:
        return decode_vehicle_positions(
            (FIXTURES / "vehicle_positions.pb").read_bytes(), etag="test-etag"
        )

    def test_decodes_every_entity(self, records):
        # The fixture holds 280 vehicles; allow a couple dropped for a
        # missing timestamp if that is the choice you made, but not more.
        assert 278 <= len(records) <= 280

    def test_returns_the_record_type(self, records):
        assert all(isinstance(r, VehiclePositionRecord) for r in records)

    def test_current_status_is_never_none(self, records):
        """The proto2 default trap. 73% of entities omit this field, and the
        declared default means they are IN_TRANSIT_TO, not unknown."""
        assert all(r.current_status is not None for r in records)

    def test_current_status_applies_the_declared_default(self, records):
        """Absent must decode to IN_TRANSIT_TO, and it must be the MAJORITY.

        If you presence-checked instead of reading the accessor, this fails
        with roughly 27% IN_TRANSIT_TO instead of 73%.
        """
        statuses = [r.current_status for r in records]
        assert set(statuses) <= {"IN_TRANSIT_TO", "STOPPED_AT", "INCOMING_AT"}
        in_transit = statuses.count("IN_TRANSIT_TO")
        assert in_transit / len(statuses) > 0.5, (
            f"only {in_transit}/{len(statuses)} IN_TRANSIT_TO -- you are "
            "presence-checking current_status instead of reading the accessor"
        )

    def test_bearing_and_speed_are_mostly_none(self, records):
        """These genuinely ARE absent (~2%), so None is correct here."""
        assert sum(r.bearing is not None for r in records) / len(records) < 0.10
        assert sum(r.speed is not None for r in records) / len(records) < 0.10

    def test_block_id_is_none_from_protobuf(self, records):
        """It does not exist in the basic protobuf at all -- ADR 0003."""
        assert all(r.block_id is None for r in records)

    def test_coordinates_are_in_king_county(self, records):
        assert all(46.9 < r.latitude < 48.0 for r in records)
        assert all(-123.0 < r.longitude < -121.0 for r in records)

    def test_key_is_vehicle_id(self, records):
        """ADR 0002."""
        assert all(r.key == r.vehicle_id for r in records)
        assert all(r.key for r in records)

    def test_timestamps_are_timezone_aware(self, records):
        """Naive datetimes land in Postgres as local time and silently shift."""
        assert all(r.position_timestamp.tzinfo is not None for r in records)

    def test_etag_is_carried_through(self, records):
        assert all(r.feed_etag == "test-etag" for r in records)

    def test_fingerprints_are_unique_within_a_poll(self, records):
        assert len({r.fingerprint for r in records}) == len(records)


class TestDecodeTripUpdates:
    @pytest.fixture(scope="class")
    def records(self) -> list[TripUpdateRecord]:
        return decode_trip_updates((FIXTURES / "trip_updates.pb").read_bytes())

    def test_fans_out_to_stop_level(self, records):
        """40 trips in the fixture, ~30 stops each. One record per entity is
        wrong by a factor of 30."""
        assert len(records) > 400, (
            f"got {len(records)} records from 40 trips -- you are returning one "
            "record per trip instead of one per stop_time_update"
        )

    def test_returns_the_record_type(self, records):
        assert all(isinstance(r, TripUpdateRecord) for r in records)

    def test_key_is_trip_id_not_vehicle_id(self, records):
        """ADR 0002 / findings.md §5 -- keying on vehicle_id would drop every
        prediction issued before its vehicle started."""
        assert all(r.key == r.trip_id for r in records)
        assert all(r.key for r in records)

    def test_absent_vehicle_id_is_none_not_empty_string(self, records):
        """Protobuf returns '' for an unset string. '' in a nullable column is
        a different value from NULL and breaks `is null` predicates."""
        assert all(r.vehicle_id != "" for r in records)

    def test_records_without_a_vehicle_are_kept(self, records):
        """They are trips that have not started -- the long-lead-time
        predictions the accuracy analysis is actually about."""
        assert any(r.vehicle_id is None for r in records)

    def test_fingerprint_includes_predicted_times(self, records):
        """Two predictions for one stop with DIFFERENT times must not collapse."""
        sample = next(r for r in records if r.arrival_time is not None)
        shifted = TripUpdateRecord(**{**sample.__dict__, "arrival_time": datetime.now(timezone.utc)})
        assert sample.fingerprint != shifted.fingerprint

    def test_stop_sequence_is_populated(self, records):
        assert all(r.stop_sequence is not None for r in records)


class TestDecodeServiceAlerts:
    @pytest.fixture(scope="class")
    def payload(self) -> bytes:
        path = FIXTURES / "service_alerts_enhanced.json"
        if not path.exists():
            pytest.skip(
                "fixture missing -- capture one with:\n"
                "  curl -s https://s3.amazonaws.com/kcm-alerts-realtime-prod/"
                "alerts_enhanced.json > tests/fixtures/service_alerts_enhanced.json"
            )
        return path.read_bytes()

    def test_decodes_json_not_protobuf(self, payload):
        """ADR 0003 -- this feed is the enhanced JSON."""
        records = decode_service_alerts(payload)
        assert records
        assert all(isinstance(r, ServiceAlertRecord) for r in records)

    def test_alert_id_is_never_none(self, payload):
        """The topic is compacted; a null key cannot be compacted."""
        records = decode_service_alerts(payload)
        assert all(r.alert_id for r in records)
        assert all(r.key == r.alert_id for r in records)

    def test_alert_ids_are_unique(self, payload):
        records = decode_service_alerts(payload)
        ids = [r.alert_id for r in records]
        assert len(ids) == len(set(ids))

    def test_header_text_is_extracted_from_translations(self, payload):
        records = decode_service_alerts(payload)
        assert any(r.header_text for r in records)


# =============================================================================
# 3. fetch
# =============================================================================


class FakeResponse:
    def __init__(self, status_code: int, content: bytes = b"", headers: dict | None = None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.history: list = []


class FakeSession:
    """Records the headers it was called with, so the contract on
    If-None-Match is checkable without a network."""

    def __init__(self, responses: list):
        self.responses = list(responses)
        self.calls: list[dict] = []
        self.headers: dict = {}

    def get(self, url, headers=None, timeout=None):
        self.calls.append({"url": url, "headers": dict(headers or {})})
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def spec():
    return feeds.get("vehicle_positions")


class TestConditionalFetcher:
    def test_first_poll_sends_no_if_none_match(self, spec):
        session = FakeSession([FakeResponse(200, b"x", {"ETag": '"abc"'})])
        fetcher = ConditionalFetcher(session=session)
        fetcher.fetch(spec)
        assert "If-None-Match" not in session.calls[0]["headers"]

    def test_second_poll_sends_the_stored_etag(self, spec):
        session = FakeSession([
            FakeResponse(200, b"x", {"ETag": '"abc"'}),
            FakeResponse(304),
        ])
        fetcher = ConditionalFetcher(session=session)
        fetcher.fetch(spec)
        fetcher.fetch(spec)
        assert session.calls[1]["headers"].get("If-None-Match") == '"abc"'

    def test_200_returns_body_and_marks_changed(self, spec):
        session = FakeSession([FakeResponse(200, b"payload", {"ETag": '"abc"'})])
        result = ConditionalFetcher(session=session).fetch(spec)
        assert result.changed and not result.unchanged
        assert result.body == b"payload"
        assert result.etag == '"abc"'

    def test_304_returns_no_body_and_marks_unchanged(self, spec):
        session = FakeSession([
            FakeResponse(200, b"x", {"ETag": '"abc"'}),
            FakeResponse(304),
        ])
        fetcher = ConditionalFetcher(session=session)
        fetcher.fetch(spec)
        result = fetcher.fetch(spec)
        assert result.unchanged and not result.changed
        assert result.body is None

    def test_304_does_not_clear_the_stored_etag(self, spec):
        """Clear it and every subsequent poll returns 200 -- the conditional
        GET silently stops working and nothing looks broken."""
        session = FakeSession([
            FakeResponse(200, b"x", {"ETag": '"abc"'}),
            FakeResponse(304),
            FakeResponse(304),
        ])
        fetcher = ConditionalFetcher(session=session)
        fetcher.fetch(spec)
        fetcher.fetch(spec)
        fetcher.fetch(spec)
        assert session.calls[2]["headers"].get("If-None-Match") == '"abc"'

    def test_new_etag_replaces_the_old_one(self, spec):
        session = FakeSession([
            FakeResponse(200, b"x", {"ETag": '"abc"'}),
            FakeResponse(200, b"y", {"ETag": '"def"'}),
            FakeResponse(304),
        ])
        fetcher = ConditionalFetcher(session=session)
        fetcher.fetch(spec)
        fetcher.fetch(spec)
        fetcher.fetch(spec)
        assert session.calls[2]["headers"].get("If-None-Match") == '"def"'

    def test_error_status_raises_feed_error(self, spec):
        session = FakeSession([FakeResponse(503)])
        with pytest.raises(FeedError):
            ConditionalFetcher(session=session).fetch(spec)

    def test_network_exception_raises_feed_error(self, spec):
        import requests

        session = FakeSession([requests.ConnectionError("boom")])
        with pytest.raises(FeedError):
            ConditionalFetcher(session=session).fetch(spec)

    def test_polls_counted_on_every_path(self, spec):
        session = FakeSession([
            FakeResponse(200, b"x", {"ETag": '"abc"'}),
            FakeResponse(304),
            FakeResponse(503),
        ])
        fetcher = ConditionalFetcher(session=session)
        fetcher.fetch(spec)
        fetcher.fetch(spec)
        with pytest.raises(FeedError):
            fetcher.fetch(spec)

        state = fetcher.state_for(spec)
        assert state.polls == 3, "failed polls must count too, or the 304 rate lies"
        assert state.changes == 1
        assert state.errors == 1

    def test_consecutive_unchanged_tracks_staleness(self, spec):
        session = FakeSession([
            FakeResponse(200, b"x", {"ETag": '"abc"'}),
            FakeResponse(304),
            FakeResponse(304),
        ])
        fetcher = ConditionalFetcher(session=session)
        for _ in range(3):
            fetcher.fetch(spec)
        assert fetcher.state_for(spec).consecutive_unchanged == 2

    def test_change_resets_the_unchanged_counter(self, spec):
        session = FakeSession([
            FakeResponse(200, b"x", {"ETag": '"abc"'}),
            FakeResponse(304),
            FakeResponse(200, b"y", {"ETag": '"def"'}),
        ])
        fetcher = ConditionalFetcher(session=session)
        for _ in range(3):
            fetcher.fetch(spec)
        assert fetcher.state_for(spec).consecutive_unchanged == 0


class TestTransientFailureRetry:
    """Phase 1's exit criterion is 24 hours of uninterrupted collection, and the
    first 24-hour run finished with 75 failures. Every one was transient: 36 DNS
    resolution failures for s3.amazonaws.com and the rest S3 dropping a pooled
    keep-alive connection mid-response. Neither is a dead feed. Both are now
    retried at the transport layer, so the policy is pinned here rather than
    left on the urllib3 default, which is zero retries and a single attempt.
    """

    def test_the_default_session_retries_transport_failures(self):
        from producer.fetch import build_session

        retry = build_session().get_adapter("https://s3.amazonaws.com/x").max_retries
        assert retry.total >= 3
        assert retry.connect >= 3
        assert retry.read >= 3
        assert 503 in retry.status_forcelist

    def test_retries_are_limited_to_idempotent_methods(self):
        from producer.fetch import build_session

        retry = build_session().get_adapter("https://s3.amazonaws.com/x").max_retries
        assert "GET" in retry.allowed_methods
        assert "POST" not in retry.allowed_methods

    def test_an_injected_session_keeps_its_own_semantics(self, spec):
        # The stub sessions the rest of this file uses are not real sessions,
        # so injecting one must leave single-attempt behaviour untouched.
        session = FakeSession([FakeResponse(200, b"x", {"ETag": '"abc"'})])
        fetcher = ConditionalFetcher(session=session)
        assert fetcher.session is session
