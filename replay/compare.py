"""Compare a replay against the live pipeline, and two replays against each other.
Phase 6, steps 6D (fidelity) and 6E (the experiment).

    python -m replay.compare fidelity --start 2026-09-24T00:00:00Z --end 2026-09-25T00:00:00Z
    python -m replay.compare experiment --start ... --end ...

FIDELITY comes first and the experiment means nothing without it. The baseline
replay runs the live logic over archived input; if its output does not match
what the live pipeline wrote from the same input, then any difference the
variant shows could be the replay machinery, not the logic change. The two
comparisons it makes:

  enriched  replay.enriched.vehicle_positions  vs  raw.enriched_vehicle_positions
            keyed by (vehicle_id, position_timestamp), the sink's primary key
  alerts    replay.alerts.bunching.baseline    vs  raw.bunching_alerts
            keyed by (vehicle_id_a, vehicle_id_b, window_end), the sink's key

Both sides restricted to event times inside [start, end). The replay's cold
dedupe cache republishes positions from before the window (producer/replay.py
explains why), and a comparison that did not restrict would count those as
"replay only" when they are the same records the live topic got earlier.

THE SHIFTED ALERTS, MEASURED, AND WHAT DOES NOT EXPLAIN THEM. An end-to-end
probe on 2026-09-26 (the reference detector in local mode over a copy of the
live 16:00-18:00 PDT slice of 09-24) matched 137 of 150 live alerts exactly,
with zero field differences on those 137. The other 13 live and 20 replay alerts
were the SAME pairs, shifted: pair 4375/4385 alerted live at ...7280 and ...7880,
and in the replay at ...7100 and ...7700. Both sides 600 s apart (the cooldown),
the replay 180 s (three windows) earlier throughout.

Two explanations were tested, and both are wrong. COLD STATE IS NOT IT: the
full-day run warms up for thirty minutes first, and 4375/4385 shifts by the same
180 s there, at the same timestamps as the cold probe. ARRIVAL ORDER IS NOT IT
either: the probe copied the live topic in live's own order and still shifted.

THE WORKING EXPLANATION, untested. A replay runs far faster than real time, so
its watermark trails the data further behind and fewer records count as late.
Live is likely to have dropped records during the 17:42 lag that the replay
kept. That still fits the write-up's reading, that the replay's alerts are what
the live job would have produced on an undisturbed evening, and it is a
falsifiable claim: slow the replay toward real time and the shift should shrink.

Exact-key equality on alerts will not reach 100% and should not be expected to.
The replay still starts before the compared window and runs past its end (the
09-24 run began at 23:30 and ended 00:10), which is about the window's edges, not
about this shift.

THE EXPERIMENT compares replay.alerts.bunching.baseline with .variant, both
from the same replayed enriched stream, so the only difference between them is
the gate.

THE LOADERS AND THE CLI READ ONLY: the replay topics are consumed to their end
with no consumer group committed, and the warehouse is read as dbt_transform,
the role that can read raw.* and nothing more (terraform/core/access.tf).

EXECUTED BY tests/test_replay_contract.py: TestNormalize, TestDiffKeys,
TestFieldMismatches and TestExperimentSummaries.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ENRICHED_KEY = ("vehicle_id", "position_timestamp")
ALERT_KEY = ("vehicle_id_a", "vehicle_id_b", "window_end")

# Fields a faithful replay CANNOT reproduce, and why. Anything added here needs
# its reason, because every entry is a column the fidelity check stops looking
# at, and a list that grows without reasons is how a comparison stops meaning
# anything.
IGNORED_FIELDS = {
    # Wall clock at enrichment time (consumers/enrichment/enrich.py). The replay
    # enriches days later by construction.
    "enriched_at",
}

TIMESTAMP_FIELDS = {"position_timestamp", "enriched_at", "window_end"}


# =============================================================================
# 6D/6E: the comparison and the summaries
# =============================================================================

def normalize(row: dict) -> dict:
    """One row from either side, in a form where equal means equal.

    CONTRACT
      * TIMESTAMP_FIELDS become int epoch SECONDS whether they arrive as an int
        (the replay topics carry epoch seconds) or a timezone-aware datetime
        (psycopg's timestamptz). A naive datetime is an error, not a guess.
      * IGNORED_FIELDS are dropped.
      * None stays None, and None != 0. ADR 0005's point survives here: an
        unset bearing and a bearing of 0 are different values.
      * Floats compare exactly. Both sides carry the same float64 produced by
        the same code from the same bytes, so a tolerance would only hide a
        real difference. If you find a column that genuinely needs one,
        measure it and say why in a comment.
      * Every other field passes through unchanged.
    """
    out: dict = {}
    for name, value in row.items():
        if name in IGNORED_FIELDS:
            continue
        out[name] = _epoch_seconds(value) if name in TIMESTAMP_FIELDS else value
    return out


# Distinguishes "absent on this side" from "present and None", so a field one
# side does not carry at all is reported rather than passing as equal.
_MISSING = object()


def _epoch_seconds(value):
    """An instant as int epoch seconds, from either side's representation.

    A naive datetime is refused rather than assumed UTC: if the two sides
    disagreed about what a naive value meant, the result would be a field
    mismatch with no cause in the data, and the fix would look like a
    timezone bug in the pipeline.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"naive datetime has no instant: {value!r}")
        return _epoch(value)
    # bool is an int subclass and would slip through as 1/0.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"not a timestamp: {value!r}")
    return int(value)


def _row_key(row: dict, key: tuple[str, ...]) -> tuple:
    return tuple(row.get(name) for name in key)


def _index(rows: list[dict], key: tuple[str, ...]) -> dict:
    """Normalized rows by key, last row winning, one entry per key.

    Last wins because that is what an upsert keeps, and it is what the sink's
    primary key means: a record the cold cache republished is not a second row.
    """
    return {_row_key(normalized, key): normalized
            for normalized in (normalize(row) for row in rows)}


@dataclass
class KeyDiff:
    matched: int = 0
    live_only: list = field(default_factory=list)
    replay_only: list = field(default_factory=list)


def diff_keys(live: list[dict], replay: list[dict], key: tuple[str, ...]) -> KeyDiff:
    """Which keys appear on one side only.

    CONTRACT
      * Rows are normalized first, so a key is compared on its normalized
        values (a datetime and an epoch int for the same instant match).
      * Duplicate keys on one side count once. The live warehouse cannot hold
        duplicates (upsert on this key), and the replay topics can, legitimately
        (a record republished by the cold cache). Neither is a difference.
      * live_only and replay_only are sorted, so a report is stable run to run.
    """
    live_by = _index(live, key)
    replay_by = _index(replay, key)
    return KeyDiff(
        matched=len(live_by.keys() & replay_by.keys()),
        live_only=sorted(live_by.keys() - replay_by.keys()),
        replay_only=sorted(replay_by.keys() - live_by.keys()),
    )


def field_mismatches(live: list[dict], replay: list[dict],
                     key: tuple[str, ...]) -> dict[str, int]:
    """For keys on both sides, how many rows differ in each field.

    CONTRACT
      * {field: rows where that field differs}, over matched keys only;
        a field that never differs is absent from the result.
      * Compared after normalize, so IGNORED_FIELDS never appear.
      * Where one side has duplicate rows for a key, compare the LAST one
        (the one an upsert would have kept).
    """
    live_by = _index(live, key)
    replay_by = _index(replay, key)
    counts: Counter = Counter()
    for row_key in live_by.keys() & replay_by.keys():
        left, right = live_by[row_key], replay_by[row_key]
        # The union, so a field only one side carries counts as a difference
        # instead of being skipped.
        for name in set(left) | set(right):
            if left.get(name, _MISSING) != right.get(name, _MISSING):
                counts[name] += 1
    return dict(counts)


def alerts_by_route(alerts: list[dict]) -> Counter:
    """Alert counts per route_short_name (None stays None)."""
    return Counter(alert.get("route_short_name") for alert in alerts)


def share_in_local_hours(alerts: list[dict], hours: range, tz: str = "America/Los_Angeles") -> float:
    """Fraction of alerts whose window_end falls in these LOCAL hours.

    Phase 3's headline was "40% in 16:00-18:00", in Pacific time. 0.0 for no
    alerts rather than a division error: the variant may legitimately be empty
    on a quiet window.
    """
    if not alerts:
        return 0.0
    zone = ZoneInfo(tz)
    inside = 0
    for alert in alerts:
        window_end = alert.get("window_end")
        # Counted in the denominator but not the numerator: a share over only
        # the alerts that have a window_end would read high on a partial window.
        if window_end is None:
            continue
        local = datetime.fromtimestamp(_epoch_seconds(window_end), tz=zone)
        if local.hour in hours:
            inside += 1
    return inside / len(alerts)


# =============================================================================
# Loaders
# =============================================================================

def _epoch(dt: datetime) -> int:
    return int(dt.astimezone(timezone.utc).timestamp())


def _read_topic(topic: str, value_decoder) -> list:
    """Every record on `topic`, from the beginning to the offsets current now.

    Assigned rather than subscribed, with no group commit, so reading a replay
    topic leaves no consumer group behind (Phase 5 found twelve abandoned ones).
    """
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
        "group.id": f"replay-compare-readonly-{os.getpid()}",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        ends, assignment = {}, []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if high > low:
                ends[p] = high
                assignment.append(TopicPartition(topic, p, low))
        consumer.assign(assignment)
        out, done = [], set()
        while len(done) < len(ends):
            msg = consumer.poll(5.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    done.add(msg.partition())
                    continue
                raise RuntimeError(msg.error())
            if msg.offset() >= ends[msg.partition()] - 1:
                done.add(msg.partition())
            decoded = value_decoder(msg.value())
            if decoded is not None:
                out.append(decoded)
        return out
    finally:
        consumer.close()


def load_replay_enriched(topic: str = "replay.enriched.vehicle_positions") -> list[dict]:
    from consumers.bunching.decode import decode

    return _read_topic(topic, decode)


def load_replay_alerts(topic: str) -> list[dict]:
    from consumers.framing import unframe

    return _read_topic(topic, lambda raw: json.loads(unframe(raw)[1]))


def _warehouse():
    import psycopg
    from dotenv import load_dotenv

    load_dotenv()
    return psycopg.connect(
        f"host=localhost port={os.environ.get('WAREHOUSE_PORT', '5434')} "
        f"dbname={os.environ['POSTGRES_DB']} user=dbt_transform "
        f"password={os.environ['DBT_TRANSFORM_PASSWORD']}")


def _query(sql: str, params) -> list[dict]:
    with _warehouse() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def load_live_enriched(start: datetime, end: datetime) -> list[dict]:
    return _query("select * from raw.enriched_vehicle_positions "
                  "where position_timestamp >= %s and position_timestamp < %s", (start, end))


def load_live_alerts(start: datetime, end: datetime) -> list[dict]:
    return _query("select * from raw.bunching_alerts "
                  "where window_end >= %s and window_end < %s", (start, end))


def in_window(rows: list[dict], ts_field: str, start: datetime, end: datetime) -> list[dict]:
    """Rows whose event time is inside [start, end), for the replay side."""
    lo, hi = _epoch(start), _epoch(end)
    return [r for r in rows if r.get(ts_field) is not None and lo <= r[ts_field] < hi]


# =============================================================================
# CLI
# =============================================================================

def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def _report_keys(name: str, diff: KeyDiff) -> None:
    print(f"  {name}: {diff.matched:,} matched, {len(diff.live_only):,} live only, "
          f"{len(diff.replay_only):,} replay only")
    for label, keys in (("live only", diff.live_only), ("replay only", diff.replay_only)):
        for k in keys[:5]:
            print(f"      e.g. {label}: {k}")


def fidelity(start: datetime, end: datetime) -> int:
    live_e = load_live_enriched(start, end)
    rep_e = in_window(load_replay_enriched(), "position_timestamp", start, end)
    print(f"enriched: {len(live_e):,} live rows, {len(rep_e):,} replay records in window")
    _report_keys("keys", diff_keys(live_e, rep_e, ENRICHED_KEY))
    print(f"  field mismatches on matched keys: {field_mismatches(live_e, rep_e, ENRICHED_KEY) or 'none'}")

    live_a = load_live_alerts(start, end)
    rep_a = in_window(load_replay_alerts("replay.alerts.bunching.baseline"), "window_end", start, end)
    print(f"alerts: {len(live_a):,} live, {len(rep_a):,} replay baseline")
    _report_keys("keys", diff_keys(live_a, rep_a, ALERT_KEY))
    print(f"  field mismatches on matched keys: {field_mismatches(live_a, rep_a, ALERT_KEY) or 'none'}")
    return 0


def experiment(start: datetime, end: datetime) -> int:
    base = in_window(load_replay_alerts("replay.alerts.bunching.baseline"), "window_end", start, end)
    var = in_window(load_replay_alerts("replay.alerts.bunching.variant"), "window_end", start, end)
    b, v = alerts_by_route(base), alerts_by_route(var)
    print(f"alerts: baseline {len(base):,}, variant {len(var):,}")
    print("  route              baseline  variant")
    for route, _ in (b + v).most_common(12):
        print(f"  {str(route):<18} {b[route]:>8}  {v[route]:>7}")
    # range(16, 19): Phase 3's "16:00-18:00" was three hourly BINS, 16:00,
    # 17:00 and 18:00 (73 + 96 + 75 = its 244 alerts), not two hours. The first
    # version of this report used range(16, 18) and so compared a two-bin share
    # against Phase 3's three-bin 40%.
    print(f"  PM peak (16:00-18:59 Pacific) share: "
          f"baseline {share_in_local_hours(base, range(16, 19)):.1%}, "
          f"variant {share_in_local_hours(var, range(16, 19)):.1%}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="replay.compare", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=("fidelity", "experiment"))
    ap.add_argument("--start", type=_utc, required=True)
    ap.add_argument("--end", type=_utc, required=True)
    args = ap.parse_args(argv)
    return fidelity(args.start, args.end) if args.mode == "fidelity" else experiment(args.start, args.end)


if __name__ == "__main__":
    sys.exit(main())
