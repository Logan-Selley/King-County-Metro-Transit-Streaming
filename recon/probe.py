"""Reconnaissance against the King County Metro GTFS-RT feeds.

Four questions, each answered with measurements rather than assumptions:

  1. How big is each feed, and how many entities does it carry?
  2. What does "enhanced" JSON carry that the basic protobuf does not?
  3. What is the *actual* refresh cadence?
  4. Which optional spec fields does Metro actually populate?

(4) is the one that is easy to skip and expensive to get wrong. Every field in
gtfs-realtime.proto below the top level is `optional`, so "the schema has an
occupancy_status field" says nothing about whether Metro sets it. A schema
designed off the spec rather than off the wire ends up with a column that is
100% null and an ADR justifying it. This counts population rates over real
entities.

Two subcommands:

    probe.py snapshot          one pull of all six artifacts, full census
    probe.py cadence -m 10     poll with If-None-Match, time real refreshes

Both write JSON under data/recon/ and print a summary. The JSON is the input
to docs/findings.md; the summary is for reading at the terminal.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests
from google.transit import gtfs_realtime_pb2 as rt

S3 = "https://s3.amazonaws.com/kcm-alerts-realtime-prod"

# Each feed is described as served as basic protobuf, basic JSON, and enhanced
# JSON. The basic JSON mirrors 403; only the enhanced ones exist. So the
# comparison is basic PB against enhanced JSON, which is what compare_shapes()
# does.
FEEDS = {
    "vehicle_positions": {
        "pb": f"{S3}/vehiclepositions.pb",
        "enhanced": f"{S3}/vehiclepositions_enhanced.json",
        "entity_field": "vehicle",
        "topic": "raw.vehicle_positions",
    },
    "trip_updates": {
        "pb": f"{S3}/tripupdates.pb",
        "enhanced": f"{S3}/tripupdates_enhanced.json",
        "entity_field": "trip_update",
        "topic": "raw.trip_updates",
    },
    "service_alerts": {
        "pb": f"{S3}/alerts.pb",
        "enhanced": f"{S3}/alerts_enhanced.json",
        "entity_field": "alert",
        "topic": "raw.service_alerts",
    },
}

STATIC_ZIP = "https://metro.kingcounty.gov/GTFS/google_transit.zip"

# A polite, identifiable UA. This is an unauthenticated public endpoint run by
# a transit agency; showing up as python-requests/2.x with no contact is how
# you become the reason they add a rate limit.
UA = "transit-stream/0.1 (portfolio project; contact: archane24@gmail.com)"

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "recon"


def fetch(url: str, etag: str | None = None, timeout: int = 45) -> dict:
    """GET with optional conditional request. Returns a dict, never raises."""
    headers = {"User-Agent": UA}
    if etag:
        headers["If-None-Match"] = etag
    t0 = time.monotonic()
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        return {"url": url, "error": str(exc), "elapsed_ms": None}
    return {
        "url": url,
        "status": r.status_code,
        "bytes": len(r.content),
        "etag": r.headers.get("ETag"),
        "last_modified": r.headers.get("Last-Modified"),
        "content_type": r.headers.get("Content-Type"),
        "elapsed_ms": round((time.monotonic() - t0) * 1000),
        "body": r.content,
    }


# --- protobuf census ---------------------------------------------------------
# ListFields() returns only fields that are actually SET on a message, which is
# exactly the distinction that matters: presence on the wire, not presence in
# the schema. Walking it recursively and counting paths gives a population rate
# per field, per feed.


def census_message(msg, prefix: str, counts: Counter, samples: dict) -> None:
    for field, value in msg.ListFields():
        path = f"{prefix}.{field.name}" if prefix else field.name

        # `field.is_repeated`, NOT `field.label == field.LABEL_REPEATED`.
        # protobuf 7 removed the `label` attribute from FieldDescriptor
        # (the LABEL_* constants are still on the class, which makes the old
        # idiom look correct right up until it raises AttributeError on the
        # upb backend). is_repeated/has_presence are the supported API.
        if field.is_repeated:
            # A repeated message field: count the container once, then descend
            # into every element so nested population rates are real counts
            # rather than "the list was non-empty".
            counts[path] += 1
            if field.type == field.TYPE_MESSAGE:
                for item in value:
                    census_message(item, path, counts, samples)
            else:
                samples.setdefault(path, list(value)[:3])
            continue

        counts[path] += 1
        if field.type == field.TYPE_MESSAGE:
            census_message(value, path, counts, samples)
        elif field.type == field.TYPE_ENUM:
            name = field.enum_type.values_by_number[value].name
            samples.setdefault(path, [])
            if name not in samples[path] and len(samples[path]) < 6:
                samples[path].append(name)
        else:
            samples.setdefault(path, [])
            if len(samples[path]) < 3 and value not in samples[path]:
                samples[path].append(value)


def census_feed(body: bytes) -> dict:
    msg = rt.FeedMessage()
    msg.ParseFromString(body)

    counts: Counter = Counter()
    samples: dict = {}
    for entity in msg.entity:
        census_message(entity, "", counts, samples)

    n = len(msg.entity)
    return {
        "entities": n,
        "header": {
            "version": msg.header.gtfs_realtime_version,
            # FULL_DATASET(0) vs DIFFERENTIAL(1). The dedup design rests on
            # these being full snapshots; this is where that is confirmed
            # rather than assumed.
            "incrementality": rt.FeedHeader.Incrementality.Name(
                msg.header.incrementality
            ),
            "timestamp": msg.header.timestamp,
            "timestamp_iso": (
                datetime.fromtimestamp(msg.header.timestamp, timezone.utc).isoformat()
                if msg.header.timestamp
                else None
            ),
        },
        "field_population": {
            path: {
                "count": c,
                "pct_of_entities": round(100.0 * c / n, 1) if n else 0.0,
                "samples": samples.get(path, [])[:3],
            }
            for path, c in sorted(counts.items())
        },
    }


# --- JSON shape --------------------------------------------------------------


def json_paths(node, prefix: str, acc: set) -> None:
    """Collect dotted key paths. List indices collapse to [] so that a
    1,000-element array contributes one path, not a thousand."""
    if isinstance(node, dict):
        for k, v in node.items():
            p = f"{prefix}.{k}" if prefix else k
            acc.add(p)
            json_paths(v, p, acc)
    elif isinstance(node, list):
        for item in node[:50]:  # 50 is plenty to find every variant key
            json_paths(item, f"{prefix}[]", acc)


def normalize(path: str) -> str:
    """Map a JSON path onto its protobuf equivalent for comparison.

    The enhanced JSON uses the GTFS-RT JSON representation, which is
    lowerCamelCase where the proto is snake_case, and wraps everything in
    `entity[]`. Normalizing both sides to snake_case without the container
    makes the set difference meaningful instead of an artifact of casing.
    """
    p = path.replace("[]", "")
    if p.startswith("entity."):
        p = p[len("entity."):]
    out = []
    for ch in p:
        if ch.isupper():
            out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch)
    return "".join(out)


def compare_shapes(pb_census: dict, enhanced_body: bytes) -> dict:
    try:
        doc = json.loads(enhanced_body)
    except (ValueError, UnicodeDecodeError) as exc:
        return {"error": f"enhanced JSON did not parse: {exc}"}

    acc: set = set()
    json_paths(doc, "", acc)

    # Only compare below the entity level; header/container keys are noise.
    j = {normalize(p) for p in acc if p.startswith("entity")}
    j = {p for p in j if p and p != "id"}
    pbf = set(pb_census["field_population"])

    return {
        "json_only": sorted(p for p in j - pbf if p),
        "pb_only": sorted(p for p in pbf - j if p),
        "shared_count": len(j & pbf),
    }


# --- subcommands -------------------------------------------------------------


def cmd_snapshot(args) -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc)
    result = {"captured_at": stamp.isoformat(), "feeds": {}, "static": None}

    for name, spec in FEEDS.items():
        print(f"\n=== {name}", flush=True)
        pb = fetch(spec["pb"])
        if "error" in pb or pb.get("status") != 200:
            print(f"  FAILED: {pb.get('error') or pb.get('status')}")
            result["feeds"][name] = {"error": pb.get("error") or pb.get("status")}
            continue

        census = census_feed(pb["body"])
        enh = fetch(spec["enhanced"])

        entry = {
            "topic": spec["topic"],
            "pb": {k: v for k, v in pb.items() if k != "body"},
            "enhanced": {k: v for k, v in enh.items() if k != "body"},
            **census,
        }
        if enh.get("status") == 200:
            entry["shape_diff"] = compare_shapes(census, enh["body"])
            entry["json_inflation_x"] = round(enh["bytes"] / pb["bytes"], 1)

        # Archive the raw payloads: they make the recon numbers reproducible
        # and are a fixture source for the decoder tests.
        (OUT / f"{name}.pb").write_bytes(pb["body"])
        if enh.get("status") == 200:
            (OUT / f"{name}_enhanced.json").write_bytes(enh["body"])

        result["feeds"][name] = entry

        print(f"  entities      {census['entities']:,}")
        print(f"  protobuf      {pb['bytes']:,} b  ({pb['elapsed_ms']} ms)")
        if enh.get("status") == 200:
            print(
                f"  enhanced json {enh['bytes']:,} b  "
                f"({entry['json_inflation_x']}x the protobuf)"
            )
        else:
            print(f"  enhanced json HTTP {enh.get('status')}")
        print(f"  incrementality {census['header']['incrementality']}")
        print(f"  bytes/entity  {pb['bytes'] // max(census['entities'], 1):,}")

    # The static feed is HEAD-only here; a 10 MB zip is not worth pulling to
    # learn its Last-Modified, which is the only thing recon needs from it.
    try:
        r = requests.head(STATIC_ZIP, headers={"User-Agent": UA}, timeout=30)
        result["static"] = {
            "url": STATIC_ZIP,
            "status": r.status_code,
            "bytes": int(r.headers.get("Content-Length", 0)),
            "last_modified": r.headers.get("Last-Modified"),
            "etag": r.headers.get("ETag"),
        }
        print(
            f"\n=== static gtfs\n  {result['static']['bytes']:,} b, "
            f"last modified {result['static']['last_modified']}"
        )
    except requests.RequestException as exc:
        result["static"] = {"error": str(exc)}

    path = OUT / "snapshot.json"
    path.write_text(json.dumps(result, indent=2, default=str))
    print(f"\nwrote {path.relative_to(ROOT)}")
    return 0


def cmd_cadence(args) -> int:
    """Poll with If-None-Match and record when each ETag actually moves.

    This is the measurement that the conditional-GET design rests on. If the
    feeds turn over every 20s, a 20s poll is right. If they turn over every
    60s, a 20s poll is three 304s and one 200, still correct, but the
    staleness alarm threshold in the feed-health mart has to know which.
    """
    OUT.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.minutes * 60
    state: dict[str, dict] = {
        n: {"etag": None, "last_change": None, "intervals": [], "polls": 0, "n304": 0}
        for n in FEEDS
    }
    log: list[dict] = []

    print(
        f"watching {len(FEEDS)} feeds for {args.minutes} min "
        f"at {args.interval}s intervals (ctrl-c to stop early)\n"
    )
    try:
        while time.monotonic() < deadline:
            tick = time.monotonic()
            for name, spec in FEEDS.items():
                s = state[name]
                r = fetch(spec["pb"], etag=s["etag"], timeout=30)
                s["polls"] += 1
                if r.get("status") == 304:
                    s["n304"] += 1
                    continue
                if r.get("status") != 200:
                    continue
                if r["etag"] != s["etag"]:
                    now = time.monotonic()
                    if s["last_change"] is not None:
                        gap = round(now - s["last_change"], 1)
                        s["intervals"].append(gap)
                        print(f"  {name:18} changed after {gap:>5.1f}s")
                    else:
                        print(f"  {name:18} first fetch ({r['bytes']:,} b)")
                    s["last_change"] = now
                    s["etag"] = r["etag"]
                    log.append(
                        {
                            "feed": name,
                            "at": datetime.now(timezone.utc).isoformat(),
                            "etag": r["etag"],
                            "bytes": r["bytes"],
                            "last_modified": r["last_modified"],
                        }
                    )
            slept = args.interval - (time.monotonic() - tick)
            if slept > 0:
                time.sleep(slept)
    except KeyboardInterrupt:
        print("\ninterrupted -- reporting on what was collected")

    summary = {}
    print("\n--- cadence ---")
    for name, s in state.items():
        iv = s["intervals"]
        summary[name] = {
            "polls": s["polls"],
            "not_modified": s["n304"],
            "changes": len(iv),
            "intervals_s": iv,
            "min_s": min(iv) if iv else None,
            "median_s": sorted(iv)[len(iv) // 2] if iv else None,
            "max_s": max(iv) if iv else None,
            # The whole point of conditional GET: what fraction of polls
            # cost nothing but a round trip.
            "pct_304": round(100.0 * s["n304"] / s["polls"], 1) if s["polls"] else 0.0,
        }
        m = summary[name]
        print(
            f"  {name:18} {m['changes']:>3} changes / {m['polls']:>3} polls  "
            f"({m['pct_304']}% 304)  "
            f"median {m['median_s']}s  range {m['min_s']}-{m['max_s']}s"
        )

    path = OUT / "cadence.json"
    path.write_text(
        json.dumps(
            {
                "measured_at": datetime.now(timezone.utc).isoformat(),
                "poll_interval_s": args.interval,
                "duration_min": args.minutes,
                "summary": summary,
                "changes": log,
            },
            indent=2,
        )
    )
    print(f"\nwrote {path.relative_to(ROOT)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("snapshot", help="one pull of every feed + field census")

    c = sub.add_parser("cadence", help="measure real refresh interval via ETag")
    c.add_argument("-m", "--minutes", type=float, default=10.0)
    c.add_argument("-i", "--interval", type=float, default=5.0)

    args = ap.parse_args()
    return {"snapshot": cmd_snapshot, "cadence": cmd_cadence}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
