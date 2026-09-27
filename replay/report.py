"""The Phase 6 experiment, as data: every number in docs/findings.md section 13.

    python -m replay.report --start 2026-09-24T07:00:00Z --end 2026-09-25T07:00:00Z \\
        --out site/data/replay.json

replay.compare prints the two headline reports; this module computes the
breakdowns the write-up rests on (where the extra alerts are, why turning the
gate off also removes alerts, which minutes the fidelity differences sit in)
and writes them as one JSON document, which the findings site reads.

IT HAS A DEADLINE. The replay topics keep 7 days (terraform/core/replay.tf), so
a run's evidence is gone a week after it: the committed site/data/replay.json is
the only copy of the run it documents, and this module is how it was made. The
numbers it recomputes are the ones docs/findings.md section 13 rests on, so a
change to either rule shows up as a failing test rather than a quietly different
JSON.

READS the replay topics and the warehouse, like replay.compare, and writes one
file. The pure functions at the top are what tests/test_replay_report.py pins.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from replay.compare import (
    ALERT_KEY,
    ENRICHED_KEY,
    _index,
    _query,
    _read_topic,
    _utc,
    diff_keys,
    field_mismatches,
    in_window,
    load_live_alerts,
    load_live_enriched,
    load_replay_alerts,
    load_replay_enriched,
    normalize,
    share_in_local_hours,
)

PT = ZoneInfo("America/Los_Angeles")

# The live gate (consumers/bunching/detect.py MIN_STOP_SEQUENCE). A copy, not an
# import, because detect.py is the Flink job's module; the report restates the
# value it measured against.
LIVE_GATE = 4
# The detector's cooldown (consumers/bunching/config.py cooldown_s).
COOLDOWN_S = 600


# --- pure functions (tested) -------------------------------------------------

def keyed(alerts: list[dict]) -> dict[tuple, dict]:
    """Normalized alerts by the sink's key, last one winning like the upsert."""
    return {tuple(normalize(a)[k] for k in ALERT_KEY): normalize(a) for a in alerts}


def pair_of(alert: dict) -> frozenset:
    """A pair regardless of which vehicle the detector listed first."""
    return frozenset((alert["vehicle_id_a"], alert["vehicle_id_b"]))


def pair_times(alerts) -> dict[frozenset, list[int]]:
    """Every alert's window_end, per pair."""
    out: dict[frozenset, list[int]] = defaultdict(list)
    for a in alerts:
        out[pair_of(a)].append(a["window_end"])
    return out


def cause_of_added(alert: dict, min_seq: int | None, baseline_pairs: dict) -> str:
    """Why an alert exists only with the gate off.

    `min_seq` is the lower stop sequence of the two vehicles inside the alert's
    window. Below the live gate means the gate is what dropped it. Otherwise the
    pair also alerts in the baseline within one cooldown, so it is the same
    bunching with its cooldown chain shifted.
    """
    if min_seq is not None and min_seq < LIVE_GATE:
        return "terminal"
    near = [t for t in baseline_pairs.get(pair_of(alert), [])
            if abs(t - alert["window_end"]) <= COOLDOWN_S]
    return "shifted" if near else "unexplained"


def cause_of_removed(alert: dict, variant: dict[tuple, dict], added_keys: set) -> str:
    """Why a baseline alert disappears with the gate off.

    "suppressed": the variant alerted the same pair in the cooldown before it,
    with an alert that exists only when the gate is off, so that alert's
    cooldown swallowed this one.
    """
    prior = [k for k, v in variant.items()
             if pair_of(v) == pair_of(alert)
             and alert["window_end"] - COOLDOWN_S <= v["window_end"] < alert["window_end"]]
    if any(k in added_keys for k in prior):
        return "suppressed"
    return "prior in both" if prior else "no prior"


def by_local_hour(alerts) -> dict[int, int]:
    """Alert counts in all 24 Pacific hours, zero-filled."""
    counts = Counter(datetime.fromtimestamp(a["window_end"], PT).hour for a in alerts)
    return {h: counts[h] for h in range(24)}


def local_minute(ts: int) -> str:
    """An epoch second as HH:MM Pacific, for the report's difference windows."""
    return datetime.fromtimestamp(ts, PT).strftime("%H:%M")


# --- the report --------------------------------------------------------------

def _stop_names() -> dict[str, str]:
    """stop_id -> stop_name, latest static version winning."""
    rows = _query("select distinct on (stop_id) stop_id, stop_name from static.stops "
                  "order by stop_id, version_id desc", ())
    return {r["stop_id"]: r["stop_name"] for r in rows}


def _tracks(vehicles: set[str]) -> dict[str, list[tuple]]:
    """Replayed positions of just these vehicles, filtered while reading."""
    from consumers.bunching.decode import decode

    def keep(raw):
        r = decode(raw)
        return r if r and r.get("vehicle_id") in vehicles else None

    tracks: dict[str, list[tuple]] = defaultdict(list)
    for r in _read_topic("replay.enriched.vehicle_positions", keep):
        tracks[r["vehicle_id"]].append((int(r["position_timestamp"]), r.get("trip_id"),
                                        r.get("current_stop_sequence"), r.get("stop_id")))
    for v in tracks:
        tracks[v].sort()
    return tracks


def _in_window_seq(tracks, alert) -> tuple[int | None, str | None]:
    """Lowest stop sequence of the pair inside the alert's 60 s window, and that stop."""
    best = None
    for v, trip in ((alert["vehicle_id_a"], alert["trip_id_a"]),
                    (alert["vehicle_id_b"], alert["trip_id_b"])):
        for ts, t, seq, stop in tracks.get(v, []):
            if t == trip and alert["window_end"] - 60 <= ts < alert["window_end"] and seq is not None:
                if best is None or seq < best[0]:
                    best = (seq, stop)
    return best if best else (None, None)


def build(start: datetime, end: datetime) -> dict:
    """The whole report: fidelity on both sides, then the gate experiment."""
    # Fidelity: enriched.
    live_e = load_live_enriched(start, end)
    rep_e = in_window(load_replay_enriched(), "position_timestamp", start, end)
    de = diff_keys(live_e, rep_e, ENRICHED_KEY)
    mism = field_mismatches(live_e, rep_e, ENRICHED_KEY)
    L, R = _index(live_e, ENRICHED_KEY), _index(rep_e, ENRICHED_KEY)
    mism_minutes = Counter(local_minute(k[1]) for k in L.keys() & R.keys()
                           if any(L[k].get(f) != R[k].get(f) for f in mism))
    enriched = {
        "live_rows": len(live_e), "replay_rows": len(rep_e), "matched": de.matched,
        "live_only": len(de.live_only), "replay_only": len(de.replay_only),
        "replay_only_by_minute": dict(Counter(local_minute(t) for _, t in de.replay_only).most_common()),
        "field_mismatches": mism,
        "mismatch_rows_by_minute": dict(mism_minutes.most_common()),
    }
    del live_e, rep_e, L, R

    # Fidelity: alerts.
    live_a = load_live_alerts(start, end)
    base_raw = in_window(load_replay_alerts("replay.alerts.bunching.baseline"), "window_end", start, end)
    var_raw = in_window(load_replay_alerts("replay.alerts.bunching.variant"), "window_end", start, end)
    da = diff_keys(live_a, base_raw, ALERT_KEY)
    alerts_fidelity = {
        "live": len(live_a), "baseline": len(base_raw), "matched": da.matched,
        "field_mismatches": field_mismatches(live_a, base_raw, ALERT_KEY),
        "live_only": [[a, b, local_minute(t)] for a, b, t in da.live_only],
        "replay_only": [[a, b, local_minute(t)] for a, b, t in da.replay_only],
    }

    # The experiment.
    B, V = keyed(base_raw), keyed(var_raw)
    added_keys = V.keys() - B.keys()
    added = [V[k] for k in added_keys]
    removed = [B[k] for k in B.keys() - V.keys()]
    tracks = _tracks({a[v] for a in added + removed for v in ("vehicle_id_a", "vehicle_id_b")})
    stops = _stop_names()
    bpairs = pair_times(B.values())

    added_rows = []
    for a in added:
        seq, stop = _in_window_seq(tracks, a)
        added_rows.append({"route": a["route_short_name"], "gap_ft": a["gap_ft"], "min_seq": seq,
                           "stop": stops.get(stop, stop), "cause": cause_of_added(a, seq, bpairs)})
    by_route_added = defaultdict(list)
    for r in added_rows:
        by_route_added[r["route"]].append(r)

    rb = Counter(a["route_short_name"] for a in B.values())
    rv = Counter(a["route_short_name"] for a in V.values())
    routes = [{"route": r, "gate_on": rb[r], "gate_off": rv[r]} for r, _ in (rb + rv).most_common()]

    removed_rows = [{"route": a["route_short_name"], "min_seq": _in_window_seq(tracks, a)[0],
                     "cause": cause_of_removed(a, V, added_keys)} for a in removed]

    return {
        "window": {"start": start.isoformat(), "end": end.isoformat(), "tz": "America/Los_Angeles"},
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fidelity": {"enriched": enriched, "alerts": alerts_fidelity},
        "experiment": {
            "gate_on": len(B), "gate_off": len(V), "both": len(B.keys() & V.keys()),
            "added": len(added), "removed": len(removed),
            "pm_peak_share": {"gate_on": share_in_local_hours(list(B.values()), range(16, 19)),
                              "gate_off": share_in_local_hours(list(V.values()), range(16, 19))},
            "by_hour": {"gate_on": by_local_hour(B.values()), "gate_off": by_local_hour(V.values())},
            "routes": routes,
            "added_by_cause": dict(Counter(r["cause"] for r in added_rows)),
            "added_by_route": [
                {"route": route, "added": len(rows),
                 "median_gap_ft": statistics.median(r["gap_ft"] for r in rows),
                 "stops": dict(Counter(r["stop"] for r in rows).most_common(3))}
                for route, rows in sorted(by_route_added.items(), key=lambda kv: -len(kv[1]))[:8]
            ],
            "removed_by_cause": dict(Counter(r["cause"] for r in removed_rows)),
            "removed_min_seq": dict(Counter("none" if r["min_seq"] is None else
                                            ("4+" if r["min_seq"] >= LIVE_GATE else str(r["min_seq"]))
                                            for r in removed_rows)),
        },
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="replay.report", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=_utc, required=True)
    ap.add_argument("--end", type=_utc, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    report = build(args.start, args.end)
    with open(args.out, "w") as f:
        json.dump(report, f, indent=1, default=str)
        f.write("\n")
    e = report["experiment"]
    print(f"wrote {args.out}: gate on {e['gate_on']}, off {e['gate_off']}, "
          f"added {e['added']} {e['added_by_cause']}, removed {e['removed']} {e['removed_by_cause']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
