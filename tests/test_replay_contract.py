"""Phase 6 contract: the replay, specified before it was written.

The executable spec for build steps 6B (re-ingest), 6C (isolation and the gate
override) and 6D/6E (comparison). Nothing here needs the stack: the archive is a
fake reader over the committed feed fixtures, the broker is a recording
publisher, and the comparison runs on synthetic rows shaped like the real ones.

The scaffolding that was given (parse_key, guard_topics, the replay namespace)
and the implementation written against this spec both run here, and none of it
needs the stack.

Three facts these tests encode were MEASURED, not assumed, on 2026-09-25:

  * Live feed_etag values carry the header's quotes ("210aa75e...") while the
    archive key stores the ETag bare, so a replay must restore them.
  * The live alerts topic held 1,037 records in the 09-24 window with 611
    unique keys, and the warehouse held exactly those 611. Duplicate keys are
    normal on a topic and must count once.
  * PyFlink local mode runs a bounded Kafka job in its own container (5,662
    records through a Python map in 8.2 s), which is why the replayed detector
    is bounded and needs no cluster slots.
"""

from __future__ import annotations

import ast
import inspect
from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from producer.feeds import FEEDS
from producer.replay import (
    REPLAY_PREFIX,
    ArchiveFetcher,
    guard_topics,
    parse_key,
    replay_spec,
    run_replay,
)

pytestmark = [pytest.mark.contract]

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
VP = FEEDS["vehicle_positions"]


def utc(*a) -> datetime:
    return datetime(*a, tzinfo=timezone.utc)


def key_at(dt: datetime, etag: str = "abc123", feed: str = "vehicle_positions",
           ext: str = "pb") -> str:
    """An archive key exactly as producer/archive.py's key_for writes it."""
    return f"raw/{feed}/{dt:%Y/%m/%d/%H}/{int(dt.timestamp())}-{etag}.{ext}"


class FakeReader:
    """The two methods ArchiveFetcher may use, over an in-memory archive.

    iter_keys returns keys under the prefix in listing order (sorted), like
    MinIO; `listed` records every prefix asked for, so a test can check the
    fetcher lists by hour rather than the whole feed.
    """

    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects
        self.listed: list[str] = []
        self.got: list[str] = []     # every key whose body was read
        self.puts = 0

    def iter_keys(self, spec, prefix: str = ""):
        full = f"{spec.archive_prefix}/{prefix}" if prefix else spec.archive_prefix
        self.listed.append(full)
        yield from sorted(k for k in self.objects if k.startswith(full))

    def get(self, key: str) -> bytes:
        self.got.append(key)
        return self.objects[key]

    def put(self, *a, **k):  # the replay must never call this
        self.puts += 1
        raise AssertionError("the replay wrote to the archive")


class RecordingPublisher:
    def __init__(self):
        self.published: list[tuple[str, str]] = []   # (topic, record key)
        self.dlq: list[str] = []                     # dlq topics written
        self.flushed = False

    def publish(self, spec, records) -> int:
        self.published.extend((spec.topic, r.key) for r in records)
        return len(records)

    def publish_dlq(self, topic, reason, payload, detail="", entity_key=None) -> None:
        self.dlq.append(topic)

    def flush(self) -> int:
        self.flushed = True
        return 0


def vp_payload() -> bytes:
    return (FIXTURES / "vehicle_positions.pb").read_bytes()


# =============================================================================
# 6B. Key parsing and the namespace guard
# =============================================================================

class TestParseKey:
    def test_real_keys_from_both_archive_periods(self):
        a = parse_key("raw/vehicle_positions/2026/09/24/00/"
                      "1790211598-ce06afa1bedf6c74675baf8e4a6a16c2.pb")
        assert a.fetched_at == utc(2026, 9, 24, 0, 59, 58)
        assert a.etag == "ce06afa1bedf6c74675baf8e4a6a16c2"
        b = parse_key("raw/service_alerts/2026/09/05/03/"
                      "1788579143-21d771875560ef366c5009171b5a8cd1.json")
        assert b.fetched_at == utc(2026, 9, 5, 3, 32, 23)

    def test_rejects_what_is_not_an_archive_key(self):
        for bad in ("flink-checkpoints/abc/chk-1/_metadata", "raw/vehicle_positions/x.pb"):
            with pytest.raises(ValueError):
                parse_key(bad)

    def test_rejects_hour_and_epoch_that_disagree(self):
        with pytest.raises(ValueError):
            parse_key("raw/vehicle_positions/2026/09/24/05/1790211598-x.pb")


class TestGuard:
    def test_live_topic_refused(self):
        with pytest.raises(SystemExit):
            guard_topics(["replay.raw.vehicle_positions", "dlq.vehicle_positions"])

    def test_replay_topics_allowed(self):
        guard_topics(["replay.raw.vehicle_positions", "replay.dlq.vehicle_positions"])


# =============================================================================
# 6B. yours: FeedSpec.dlq_topic, and process_feed using it
# =============================================================================

class TestDlqTopic:
    def test_live_feeds_keep_their_dlq_topics(self):
        for name, spec in FEEDS.items():
            assert spec.dlq_topic == f"dlq.{name}"

    def test_dlq_topic_is_overridable(self):
        assert replace(VP, dlq="replay.dlq.vehicle_positions").dlq_topic == \
            "replay.dlq.vehicle_positions"

    def test_process_feed_routes_dlq_through_the_spec(self):
        """The literal f"dlq.{spec.name}" in process_feed would send a replay's
        rejects to the LIVE DLQ, where transit_health counts them."""
        from producer.run import Pipeline, process_feed

        spec = replay_spec(VP)
        k = key_at(utc(2026, 9, 24, 12, 0, 0))
        pub = RecordingPublisher()
        fetcher = ArchiveFetcher(FakeReader({k: b"\x00not protobuf"}),
                                 utc(2026, 9, 24, 12), utc(2026, 9, 24, 13), feeds=[VP])
        process_feed(spec, Pipeline(fetcher=fetcher, archive=None, publisher=pub))
        assert pub.dlq and set(pub.dlq) == {"replay.dlq.vehicle_positions"}


# =============================================================================
# 6B. yours: replay_spec
# =============================================================================

class TestReplaySpec:
    def test_writes_into_the_namespace(self):
        s = replay_spec(VP)
        assert s.topic == REPLAY_PREFIX + VP.topic
        assert s.dlq_topic == REPLAY_PREFIX + VP.dlq_topic

    def test_reads_the_live_archive_and_changes_nothing_else(self):
        s = replay_spec(VP)
        assert s.archive_prefix == VP.archive_prefix
        for f in ("name", "url", "wire_format", "entity_field", "key_field",
                  "compacted", "dedupe_maxsize"):
            assert getattr(s, f) == getattr(VP, f), f


# =============================================================================
# 6B. yours: ArchiveFetcher
# =============================================================================

class TestArchiveFetcher:
    START, END = utc(2026, 9, 24, 12), utc(2026, 9, 24, 14)

    def archive(self):
        return {
            key_at(utc(2026, 9, 24, 11, 59, 59), "before"): b"x",
            key_at(utc(2026, 9, 24, 12, 0, 0), "first"): b"a",
            key_at(utc(2026, 9, 24, 12, 30, 0), "second"): b"b",
            key_at(utc(2026, 9, 24, 13, 59, 59), "last"): b"c",
            key_at(utc(2026, 9, 24, 14, 0, 0), "after"): b"y",
        }

    def drain(self, fetcher):
        out = []
        while not fetcher.exhausted:
            out.append(fetcher.fetch(VP))
        return out

    def test_half_open_window_in_order_each_once(self):
        got = self.drain(ArchiveFetcher(FakeReader(self.archive()), self.START, self.END, feeds=[VP]))
        assert [r.body for r in got] == [b"a", b"b", b"c"]

    def test_fetch_time_comes_from_the_key(self):
        got = self.drain(ArchiveFetcher(FakeReader(self.archive()), self.START, self.END, feeds=[VP]))
        assert got[0].fetched_at == utc(2026, 9, 24, 12, 0, 0)
        assert all(r.status == 200 and r.changed for r in got)

    def test_etag_restored_to_the_form_live_records_carry(self):
        """Measured: live feed_etag is quoted, the key is not."""
        got = self.drain(ArchiveFetcher(FakeReader(self.archive()), self.START, self.END, feeds=[VP]))
        assert got[0].etag == '"first"'

    def test_lists_by_hour_not_the_whole_feed(self):
        reader = FakeReader(self.archive())
        self.drain(ArchiveFetcher(reader, self.START, self.END, feeds=[VP]))
        assert reader.listed, "never listed"
        assert all("/2026/09/24/" in p for p in reader.listed), reader.listed

    def test_empty_window_is_exhausted_immediately(self):
        f = ArchiveFetcher(FakeReader(self.archive()), utc(2026, 9, 20), utc(2026, 9, 20, 1), feeds=[VP])
        assert f.exhausted

    def test_other_feeds_in_the_window_are_not_replayed(self):
        """Added in review, 2026-09-26. The live archive holds all three feeds
        in every hour, and a fetcher that queued them all never reported
        exhausted for a vehicle-positions replay: its next fetch popped an
        empty queue. Every real replay would have crashed at the end."""
        objs = self.archive() | {
            key_at(utc(2026, 9, 24, 12, 0, 10), "tu", feed="trip_updates"): b"t",
            key_at(utc(2026, 9, 24, 12, 0, 20), "sa", feed="service_alerts"): b"s",
        }
        reader = FakeReader(objs)
        got = self.drain(ArchiveFetcher(reader, self.START, self.END, feeds=[VP]))
        assert [r.body for r in got] == [b"a", b"b", b"c"]
        assert all("/vehicle_positions/" in p for p in reader.listed), reader.listed

    def test_bodies_are_read_one_fetch_at_a_time(self):
        """Listing up front is fine; downloading up front holds a day of
        payloads in memory. One fetch, one body read."""
        reader = FakeReader(self.archive())
        f = ArchiveFetcher(reader, self.START, self.END, feeds=[VP])
        assert reader.got == []
        f.fetch(VP)
        assert len(reader.got) == 1

    def test_matched_by_name_so_the_replay_spec_works(self):
        """process_feed calls fetch with the REPLAY spec: same name, other topic."""
        f = ArchiveFetcher(FakeReader(self.archive()), self.START, self.END, feeds=[VP])
        assert f.fetch(replay_spec(VP)).body == b"a"

    def test_an_unparseable_key_in_range_is_an_error(self):
        objs = self.archive() | {"raw/vehicle_positions/2026/09/24/12/garbage.pb": b"z"}
        with pytest.raises(ValueError):
            self.drain(ArchiveFetcher(FakeReader(objs), self.START, self.END, feeds=[VP]))


# =============================================================================
# 6B. yours: run_replay
# =============================================================================

class TestRunReplay:
    START, END = utc(2026, 9, 24, 12), utc(2026, 9, 24, 13)

    def objects(self):
        p = vp_payload()
        return {key_at(utc(2026, 9, 24, 12, 0, 0), "one"): p,
                key_at(utc(2026, 9, 24, 12, 0, 20), "two"): p}

    def test_refuses_a_live_spec(self):
        with pytest.raises(SystemExit):
            run_replay(VP, ArchiveFetcher(FakeReader(self.objects()), self.START, self.END, feeds=[VP]),
                       RecordingPublisher())

    def test_publishes_only_into_the_namespace_and_never_archives(self):
        reader, pub = FakeReader(self.objects()), RecordingPublisher()
        run_replay(replay_spec(VP), ArchiveFetcher(reader, self.START, self.END, feeds=[VP]), pub)
        assert pub.published
        assert {t for t, _ in pub.published} == {"replay.raw.vehicle_positions"}
        assert reader.puts == 0
        assert pub.flushed

    def test_dedupe_is_the_live_dedupe(self):
        """The same payload twice: the second publishes nothing, as live."""
        stats = run_replay(replay_spec(VP),
                           ArchiveFetcher(FakeReader(self.objects()), self.START, self.END, feeds=[VP]),
                           RecordingPublisher())
        assert stats.payloads == 2
        assert stats.suppressed >= stats.published > 0

    def test_other_feeds_in_the_archive_do_not_stop_the_replay(self):
        """The crash the review found, end to end: IndexError before the fix."""
        objs = self.objects() | {
            key_at(utc(2026, 9, 24, 12, 0, 10), "tu", feed="trip_updates"): b"t"}
        stats = run_replay(replay_spec(VP),
                           ArchiveFetcher(FakeReader(objs), self.START, self.END, feeds=[VP]),
                           RecordingPublisher())
        assert stats.payloads == 2

    def test_deterministic(self):
        runs = []
        for _ in range(2):
            pub = RecordingPublisher()
            run_replay(replay_spec(VP),
                       ArchiveFetcher(FakeReader(self.objects()), self.START, self.END, feeds=[VP]), pub)
            runs.append(pub.published)
        assert runs[0] == runs[1]


# =============================================================================
# 6C. yours: the detector's wiring, and the gate as a parameter
# =============================================================================

class TestRunSettings:
    def settings(self, env):
        from consumers.bunching.config import run_settings
        return run_settings(env)

    def test_no_replay_env_is_the_live_job_exactly(self):
        from consumers.bunching import config
        from consumers.bunching.detect import MIN_STOP_SEQUENCE
        s = self.settings({})
        assert (s.source_topic, s.sink_topic, s.group) == \
            (config.SOURCE_TOPIC, config.SINK_TOPIC, config.CONSUMER_GROUP)
        assert s.sink_subject == f"{config.SINK_TOPIC}-value"
        assert not s.bounded and s.commit_offsets
        assert s.min_stop_sequence == MIN_STOP_SEQUENCE
        assert s.job_name == "bunching-detector"
        assert self.settings({"BUNCHING_REPLAY": ""}) == s

    def test_baseline(self):
        from consumers.bunching.detect import MIN_STOP_SEQUENCE
        s = self.settings({"BUNCHING_REPLAY": "baseline"})
        assert s.source_topic == "replay.enriched.vehicle_positions"
        assert s.sink_topic == "replay.alerts.bunching.baseline"
        assert s.sink_subject == "alerts.bunching-value"
        assert s.group == "replay-bunching-baseline"
        assert s.bounded and not s.commit_offsets
        assert s.min_stop_sequence == MIN_STOP_SEQUENCE
        assert s.job_name == "bunching-replay-baseline"

    def test_baseline_refuses_a_gate(self):
        with pytest.raises(ValueError):
            self.settings({"BUNCHING_REPLAY": "baseline",
                           "BUNCHING_REPLAY_MIN_STOP_SEQUENCE": "0"})

    def test_variant_requires_a_gate(self):
        with pytest.raises(ValueError):
            self.settings({"BUNCHING_REPLAY": "variant"})
        s = self.settings({"BUNCHING_REPLAY": "variant",
                           "BUNCHING_REPLAY_MIN_STOP_SEQUENCE": "0"})
        assert s.min_stop_sequence == 0
        assert s.sink_topic == "replay.alerts.bunching.variant"

    @pytest.mark.parametrize("gate", ["-1", "four", "1.5"])
    def test_variant_gate_must_be_a_non_negative_int(self, gate):
        with pytest.raises(ValueError):
            self.settings({"BUNCHING_REPLAY": "variant",
                           "BUNCHING_REPLAY_MIN_STOP_SEQUENCE": gate})

    def test_unknown_run_refused(self):
        with pytest.raises(ValueError):
            self.settings({"BUNCHING_REPLAY": "prod"})


class TestJobUsesTheSettings:
    """job.py imports pyflink, so these read its source rather than run it."""

    SRC = (ROOT / "consumers" / "bunching" / "job.py").read_text()

    def test_settings_resolved_from_the_environment(self):
        assert "run_settings(os.environ)" in self.SRC

    def test_no_topic_or_group_constant_is_used_directly(self):
        """Every topic and group comes from the settings, so a replay cannot
        half-apply: reading the replay topic while writing the live one."""
        tree = ast.parse(self.SRC)
        used = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert not used & {"SOURCE_TOPIC", "SINK_TOPIC", "CONSUMER_GROUP"}, \
            used & {"SOURCE_TOPIC", "SINK_TOPIC", "CONSUMER_GROUP"}


class TestGateParameter:
    """detect_in_window takes the gate as a parameter, defaulting to live."""

    WINDOW_END = 1_758_400_000.0

    def pos(self, vid, dist, feet, stop_seq):
        return {
            "vehicle_id": vid, "route_id": "100512", "direction_id": 0,
            "trip_id": f"trip-{vid}", "route_short_name": "255",
            "shape_dist_traveled": dist, "position_timestamp": self.WINDOW_END - 5,
            "latitude": 47.6970 + feet / 364_000.0, "longitude": -122.3450,
            "schedule_deviation_seconds": 60, "current_stop_sequence": stop_seq,
        }

    def test_signature_defaults_to_the_live_gate(self):
        from consumers.bunching.detect import MIN_STOP_SEQUENCE, detect_in_window
        p = inspect.signature(detect_in_window).parameters["min_stop_sequence"]
        assert p.default == MIN_STOP_SEQUENCE

    def test_gate_zero_admits_the_terminal_pair_the_live_gate_drops(self):
        """Route 255 at Totem Lake: stacked at stops 2 and 3."""
        from consumers.bunching.detect import detect_in_window
        recs = [self.pos("A", 1_600, 0, 2), self.pos("B", 1_620, 20, 3)]
        assert detect_in_window("100512:0", recs, self.WINDOW_END) == []
        assert len(detect_in_window("100512:0", recs, self.WINDOW_END,
                                    min_stop_sequence=0)) == 1


# =============================================================================
# 6C. yours: the enrichment consumer's wiring
# =============================================================================

class TestEnrichmentSettings:
    def parse(self, argv):
        from consumers.enrichment.run import settings_from_args
        return settings_from_args(argv)

    def test_defaults_are_live(self):
        s = self.parse([])
        assert (s.source, s.target, s.dlq, s.group) == \
            ("raw.vehicle_positions", "enriched.vehicle_positions",
             "dlq.vehicle_positions", "enrichment")
        assert s.exit_when_idle_s is None

    def test_replay_wiring(self):
        s = self.parse(["--source", "replay.raw.vehicle_positions",
                        "--target", "replay.enriched.vehicle_positions",
                        "--dlq", "replay.dlq.vehicle_positions",
                        "--group", "replay-enrichment", "--exit-when-idle", "30"])
        assert s.target == "replay.enriched.vehicle_positions"
        assert s.exit_when_idle_s == 30

    def test_mixing_live_and_replay_topics_is_refused(self):
        """Reading the replay topic and writing the live enriched topic would
        put history into the warehouse through the live sink."""
        with pytest.raises((ValueError, SystemExit)):
            self.parse(["--source", "replay.raw.vehicle_positions"])

    def test_serializer_can_pin_the_live_subject(self):
        """The replay writes records of exactly the live schema; resolving the
        subject from the replay topic would need a subject nobody registered."""
        from consumers.enrichment.schema import build_serializer
        assert "subject" in inspect.signature(build_serializer).parameters


class TestReplayMatchesLiveEnrichment:
    """The replayed enrichment runs the live schema version (Makefile)."""

    def test_same_schema_version_as_the_live_container(self):
        import re
        compose = (ROOT / "docker-compose.yml").read_text()
        live = re.search(r'consumers\.enrichment\.run", "--schema-version", "(\d)"', compose)
        make = re.search(r"replay-enrich:.*?--schema-version (\d)",
                         (ROOT / "Makefile").read_text(), re.S)
        assert live and make and live.group(1) == make.group(1)


# =============================================================================
# 6D/6E. yours: the comparison
# =============================================================================

class TestNormalize:
    def test_timestamps_from_either_side_agree(self):
        from replay.compare import normalize
        live = normalize({"vehicle_id": "7", "position_timestamp": utc(2026, 9, 24, 12)})
        rep = normalize({"vehicle_id": "7", "position_timestamp":
                         int(utc(2026, 9, 24, 12).timestamp())})
        assert live == rep

    def test_naive_datetime_is_an_error(self):
        from replay.compare import normalize
        with pytest.raises((ValueError, TypeError)):
            normalize({"position_timestamp": datetime(2026, 9, 24, 12)})

    def test_ignored_fields_dropped_and_none_is_not_zero(self):
        from replay.compare import normalize
        n = normalize({"enriched_at": 1, "bearing": None, "speed": 0.0})
        assert "enriched_at" not in n
        assert n["bearing"] is None and n["speed"] == 0.0


class TestDiffKeys:
    K = ("vehicle_id_a", "vehicle_id_b", "window_end")

    def alert(self, a, b, t):
        return {"vehicle_id_a": a, "vehicle_id_b": b, "window_end": t}

    def test_duplicates_on_the_topic_count_once(self):
        """Measured: 1,037 topic records, 611 keys, 611 warehouse rows."""
        from replay.compare import diff_keys
        t = utc(2026, 9, 24, 12)
        live = [self.alert("1", "2", t)]
        rep = [self.alert("1", "2", int(t.timestamp()))] * 3
        d = diff_keys(live, rep, self.K)
        assert (d.matched, d.live_only, d.replay_only) == (1, [], [])

    def test_one_sided_keys_reported_sorted(self):
        from replay.compare import diff_keys
        d = diff_keys([self.alert("9", "8", 100), self.alert("1", "2", 100)],
                      [self.alert("3", "4", 100)], self.K)
        assert d.matched == 0
        assert d.live_only == sorted(d.live_only) and len(d.live_only) == 2
        assert len(d.replay_only) == 1


class TestFieldMismatches:
    K = ("vehicle_id", "position_timestamp")

    def test_counts_per_field_on_matched_keys_only(self):
        from replay.compare import field_mismatches
        live = [{"vehicle_id": "1", "position_timestamp": 10, "speed": 1.0, "enriched_at": 5},
                {"vehicle_id": "2", "position_timestamp": 10, "speed": 1.0, "enriched_at": 5}]
        rep = [{"vehicle_id": "1", "position_timestamp": 10, "speed": 2.0, "enriched_at": 9},
               {"vehicle_id": "3", "position_timestamp": 10, "speed": 9.0, "enriched_at": 9}]
        assert field_mismatches(live, rep, self.K) == {"speed": 1}

    def test_duplicates_compare_the_last(self):
        from replay.compare import field_mismatches
        live = [{"vehicle_id": "1", "position_timestamp": 10, "speed": 2.0}]
        rep = [{"vehicle_id": "1", "position_timestamp": 10, "speed": 1.0},
               {"vehicle_id": "1", "position_timestamp": 10, "speed": 2.0}]
        assert field_mismatches(live, rep, self.K) == {}


class TestExperimentSummaries:
    def test_by_route(self):
        from replay.compare import alerts_by_route
        c = alerts_by_route([{"route_short_name": "255"}, {"route_short_name": "255"},
                             {"route_short_name": None}])
        assert c == Counter({"255": 2, None: 1})

    def test_pm_peak_share_in_pacific_time(self):
        from replay.compare import share_in_local_hours
        # 2026-09-24 23:30 UTC is 16:30 PDT; 12:00 UTC is 05:00 PDT.
        alerts = [{"window_end": int(utc(2026, 9, 24, 23, 30).timestamp())},
                  {"window_end": int(utc(2026, 9, 24, 12).timestamp())}]
        assert share_in_local_hours(alerts, range(16, 18)) == 0.5
        assert share_in_local_hours([], range(16, 18)) == 0.0
