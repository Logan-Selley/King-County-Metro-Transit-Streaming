"""Marts -> site/data/*.json for the findings site.

    make exports                       # the study window, to the last closed day
    python -m publish.export --first 2026-09-24 --last 2026-09-26

The site is static: GitHub Pages serves site/ as uploaded, with no server and
no warehouse behind it. So what it shows is a SNAPSHOT, and this module is the
only thing that makes one. It reads marts only (plus the two daily
intermediates the prediction curve is additive over), never raw.* or staging:
the marts are the contract the rest of the project is tested against, and a
site that computed its own numbers from raw rows would be a second, untested
implementation of them.

THE STUDY WINDOW is Pacific calendar days, 2026-09-24 (Thursday) to 09-30
(Wednesday): five weekdays and a weekend, starting from the first day the stack
ran complete. Every file carries the window it was cut from, and the site
prints it.

A CUT STOPS AT THE LAST CLOSED DAY (closed_window below). The study is a week;
the days of it that have finished are what a cut can hold. Runs are expected
while the week is still filling, so the default --last of 09-30 is clamped
rather than trusted, and once 09-30 is over the clamp does nothing.

OUTPUT CONTRACT (the site reads exactly these; change one and change
site/index.html with it):

    kpis.json              kpis()
    bunching_by_hour.json  alerts_by_hour()
    routes.json            route_ranking()
    hotspots.geojson       hotspots()
    prediction_curve.json  prediction_curve()
    feed_health.json       feed_health_by_day()
    replay.json            NOT from here: replay/report.py writes it from the
                           replay topics, which keep 7 days
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "site" / "data"

STUDY_FIRST = date(2026, 9, 24)
STUDY_LAST = date(2026, 9, 30)

# The calendar every day in this module is on: local_date, minute_local and the
# study window are all Pacific. Named once, so the clamp and the marts cannot
# disagree about which "today" is meant.
PACIFIC = "America/Los_Angeles"

# Phase 3's peak: three hourly bins, 16:00, 17:00 and 18:00 (73 + 96 + 75 = its
# 244 alerts). Not range(16, 18); see replay/compare.py's experiment().
PM_PEAK_HOURS = range(16, 19)


# =============================================================================
# The window
# =============================================================================

def window_days(first: date, last: date) -> list[date]:
    """Every Pacific calendar date in [first, last], inclusive."""
    if last < first:
        raise ValueError(f"window ends before it starts: {first} .. {last}")
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def closed_last(now: datetime | None = None) -> date:
    """The last day whose data has finished: yesterday, in Pacific time.

    A DAY IS ONLY A RATE ONCE IT HAS ENDED. alerts_per_day divides by the days in
    the window and days_by_type divides by the weekdays in it, so a window that
    reaches into today, or past it, counts days holding nothing. Cut on
    2026-09-27, the study window reports a three-day total as a seven-day rate:
    213.9 alerts a day instead of 498.7, with the page printing "7 days" over
    three days of data.
    """
    stamp = now or datetime.now(timezone.utc)
    return stamp.astimezone(ZoneInfo(PACIFIC)).date() - timedelta(days=1)


def closed_window(first: date, last: date,
                  now: datetime | None = None) -> tuple[date, date]:
    """[first, last] with its end clamped to the last closed Pacific day.

    The study window stays what it is; this says how much of it exists to cut. A
    window starting after the last closed day is an error rather than an empty
    cut, because "no days yet" is not a snapshot.
    """
    closed = closed_last(now)
    if first > closed:
        raise ValueError(f"nothing closed to cut: {first} .. {last}, the last "
                         f"closed day is {closed}")
    return first, min(last, closed)


def day_type(d: date) -> str:
    """'weekday', 'saturday' or 'sunday'. The same rule as mart_bunching_alerts."""
    return {5: "saturday", 6: "sunday"}.get(d.weekday(), "weekday")


def days_by_type(days: list[date]) -> dict[str, int]:
    """How many days of each type the window holds: the divisor for per-day rates."""
    counts = Counter(day_type(d) for d in days)
    return {t: counts.get(t, 0) for t in ("weekday", "saturday", "sunday")}


def _rounded(value, places: int = 0) -> float | int:
    """The mart's rounding: Postgres round() on numeric is half AWAY from zero.

    Python's round() is half to EVEN, so a median of 112.5 is 113 in the mart and
    112 here. Values that came back from the warehouse are Decimals already,
    which quantize exactly; a float is read as its shortest decimal form.
    """
    quantum = Decimal(1).scaleb(-places)
    out = Decimal(str(value) if isinstance(value, float) else value).quantize(
        quantum, rounding=ROUND_HALF_UP)
    return int(out) if places == 0 else float(out)


def _located(alert: dict) -> bool:
    """Whether an alert has a point to be drawn at: a latitude AND a longitude."""
    return alert.get("latitude") is not None and alert.get("longitude") is not None


# =============================================================================
# The transforms. Inputs are rows as dicts, exactly as the loaders below
# return them; outputs are what the JSON files hold.
# =============================================================================

def alerts_by_hour(alerts: list[dict], days: list[date]) -> dict:
    """Alerts per DAY in each Pacific hour, weekdays and weekends apart.

    A RATE, not a count: the window has five weekdays and two weekend days, so
    raw counts would make weekdays look 2.5x worse for no reason. Saturday and
    Sunday pool into "weekend", and a day type the window has no days of gives
    all zeros rather than a division error.
    """
    counts = {"weekday": [0] * 24, "weekend": [0] * 24}
    for a in alerts:
        bucket = "weekday" if day_type(a["local_date"]) == "weekday" else "weekend"
        counts[bucket][int(a["local_hour"])] += 1

    divisors = days_by_type(days)
    per_day = {
        "weekday": divisors["weekday"],
        "weekend": divisors["saturday"] + divisors["sunday"],
    }
    return {
        bucket: [round(n / per_day[bucket], 2) if per_day[bucket] else 0.0 for n in hours]
        for bucket, hours in counts.items()
    }


def route_ranking(alerts: list[dict], days: list[date]) -> list[dict]:
    """One row per route, most alerts first.

    route None is KEPT as its own row: an alert with no route is still an alert,
    and dropping it would make this file disagree with kpis.json.
    """
    by_route: dict[str | None, list[dict]] = defaultdict(list)
    for a in alerts:
        by_route[a["route_short_name"]].append(a)

    rows = []
    for route, group in by_route.items():
        gaps = [a["gap_ft"] for a in group if a["gap_ft"] is not None]
        peak = sum(1 for a in group if int(a["local_hour"]) in PM_PEAK_HOURS)
        rows.append({
            "route": route,
            "alerts": len(group),
            "per_day": round(len(group) / len(days), 2),
            "pm_peak_share": round(peak / len(group), 3),
            # The median, as mart_bunching_by_route_hour argues: gaps are heavy-tailed.
            "median_gap_ft": round(statistics.median(gaps), 1) if gaps else None,
        })
    rows.sort(key=lambda r: (-r["alerts"], r["route"] is None, r["route"] or ""))
    return rows


def hotspots(alerts: list[dict]) -> dict:
    """Located alerts, grouped by stop, as a GeoJSON FeatureCollection.

    Alerts with no location are not on the map; kpis() counts them, so the gap
    between the map and the total is stated rather than hidden.
    """
    by_stop: dict[str | None, list[dict]] = defaultdict(list)
    for a in alerts:
        if _located(a):
            by_stop[a.get("stop_id")].append(a)

    features = []
    for stop_id, group in by_stop.items():
        routes = Counter(a.get("route_short_name") for a in group)
        ranked = sorted(routes.items(), key=lambda kv: (-kv[1], kv[0] is None, kv[0] or ""))[:3]
        features.append({
            "type": "Feature",
            # GeoJSON is lon-first and Postgres rows are not: swapped, every
            # point claims a latitude of -122, which is not a place. 5 decimals
            # is about a metre.
            "geometry": {
                "type": "Point",
                "coordinates": [
                    round(sum(a["longitude"] for a in group) / len(group), 5),
                    round(sum(a["latitude"] for a in group) / len(group), 5),
                ],
            },
            "properties": {
                "stop_id": stop_id,
                "stop_name": group[0].get("stop_name"),
                "alerts": len(group),
                "pm_peak_alerts": sum(1 for a in group if int(a["local_hour"]) in PM_PEAK_HOURS),
                "routes": [[route, n] for route, n in ranked],
            },
        })

    features.sort(key=lambda f: (-f["properties"]["alerts"],
                                 f["properties"]["stop_id"] is None,
                                 f["properties"]["stop_id"] or ""))
    return {"type": "FeatureCollection", "features": features}


def _value_at(counts: dict[int, int], rank: int) -> int | float:
    """The value at a 0-based position in the expanded multiset, without expanding."""
    seen = 0
    for value in sorted(counts):
        seen += counts[value]
        if seen > rank:
            return value
    raise ValueError(f"rank {rank} is past the end of the histogram")


def percentile_from_histogram(counts: dict[int, int], q: float) -> float:
    """Postgres percentile_cont(q) over a multiset given as value -> count.

    The EXACT value percentile_cont gives over the expanded multiset: position
    (n - 1) * q, linearly interpolated between the values at the floor and
    ceiling positions. Not "nearest rank". mart_prediction_error_by_lead does
    the same reconstruction in SQL, and the two have to agree.

    Without expanding: raw.prediction_accuracy gains ~3M rows a day, so a week's
    multiset is tens of millions of values, while the widest bucket-day holds
    only 1,643 distinct ones. Walk the sorted values with a running count.
    """
    n = sum(counts.values())
    if n == 0:
        # A curve point from no data is a lie the chart would draw.
        raise ValueError("percentile over an empty histogram")

    pos = (n - 1) * q
    lo_rank = math.floor(pos)
    lo = _value_at(counts, lo_rank)
    hi = _value_at(counts, min(lo_rank + 1, n - 1))
    return lo + (hi - lo) * (pos - lo_rank)


def prediction_curve(histogram: list[dict], daily: list[dict]) -> list[dict]:
    """The error curve over the window, one point per lead bucket.

    INPUTS: rows of int_prediction_error_histogram (observed_day, lead_bucket,
    abs_error_s, predictions) and int_prediction_bucket_daily (observed_day,
    lead_bucket, predictions, sum_error_s, min_lead_time_s), already restricted
    to the window's days by the loader. The intermediates are keyed by UTC day,
    and the site says so under the chart; the other files are Pacific days.
    """
    counts: dict[str, Counter] = defaultdict(Counter)
    for row in histogram:
        counts[row["lead_bucket"]][row["abs_error_s"]] += row["predictions"]

    errors: dict[str, float] = defaultdict(int)
    leads: dict[str, list] = defaultdict(list)
    for row in daily:
        errors[row["lead_bucket"]] += row["sum_error_s"]
        leads[row["lead_bucket"]].append(row["min_lead_time_s"])

    # Ascending min(min_lead_time_s), the ordering the mart uses. A bucket the
    # daily table does not have sorts last rather than first.
    order = sorted(counts, key=lambda b: min(leads[b], default=math.inf))

    return [{
        "lead_bucket": bucket,
        "predictions": sum(counts[bucket].values()),
        # Rounded the way the mart rounds, and this is the one place it shows:
        # a median of 112.5 is 113 in Postgres and 112 from Python's round().
        "median_abs_error_s": _rounded(percentile_from_histogram(counts[bucket], 0.5)),
        "p90_abs_error_s": _rounded(percentile_from_histogram(counts[bucket], 0.9)),
        "mean_error_s": _rounded(errors[bucket] / sum(counts[bucket].values())),
    } for bucket in order]


def feed_health_by_day(minutes: list[dict]) -> list[dict]:
    """Per Pacific day: how much of it the pipeline actually saw.

    INPUT: mart_feed_health rows (minute_local, positions, is_silent,
    median_ingest_lag_s), restricted to the window by the loader.
    """
    by_day: dict[date, list[dict]] = defaultdict(list)
    for row in minutes:
        by_day[row["minute_local"].date()].append(row)

    out = []
    for day in sorted(by_day):
        rows = by_day[day]
        silent = sum(1 for r in rows if r["is_silent"])
        lags = [r["median_ingest_lag_s"] for r in rows
                if not r["is_silent"] and r["median_ingest_lag_s"] is not None]
        out.append({
            "date": day.isoformat(),
            "minutes": len(rows),
            "silent_minutes": silent,
            "live_share": round(1 - silent / len(rows), 4),
            "positions": sum(r["positions"] or 0 for r in rows),
            # A median of medians, which is not the day's true median; the site
            # labels it "typical minute's lag" for that reason. None for a day
            # with no live minute.
            "median_ingest_lag_s": _rounded(statistics.median(lags)) if lags else None,
        })
    return out


def kpis(alerts: list[dict], health_days: list[dict], curve: list[dict],
         days: list[date]) -> dict:
    """The headline numbers across the top of the site."""
    minutes = sum(d["minutes"] for d in health_days)
    live = sum(d["minutes"] - d["silent_minutes"] for d in health_days)
    located = sum(1 for a in alerts if _located(a))
    peak = sum(1 for a in alerts if int(a["local_hour"]) in PM_PEAK_HOURS)

    # The first FORWARD bucket and the last one, so the headline "the error
    # grows with lead time" is two numbers anyone can check against the chart.
    # Not simply curve[0]: the curve starts with "past", predictions issued
    # after the bus had already arrived, which sorts first because its lead
    # times are negative.
    forward = [p for p in curve if p["lead_bucket"] != "past"] or curve

    return {
        "first": days[0].isoformat(),
        "last": days[-1].isoformat(),
        "days": len(days),
        "positions": sum(d["positions"] for d in health_days),
        # Weighted by minutes, not a mean of daily shares.
        "live_share": round(live / minutes, 3) if minutes else 0.0,
        "alerts": len(alerts),
        "alerts_per_day": round(len(alerts) / len(days), 1) if days else 0.0,
        # The map's coverage, stated.
        "located_share": round(located / len(alerts), 3) if alerts else 0.0,
        "pm_peak_share": round(peak / len(alerts), 3) if alerts else 0.0,
        "predictions": sum(p["predictions"] for p in curve),
        "median_abs_error_short_s": _rounded(forward[0]["median_abs_error_s"]),
        "median_abs_error_long_s": _rounded(forward[-1]["median_abs_error_s"]),
    }


# =============================================================================
# Loaders, writer, CLI
# =============================================================================

def _query(sql: str, params: tuple) -> list[dict]:
    """Read the warehouse as dbt_transform, the read-only role for marts."""
    import psycopg
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    with psycopg.connect(
        f"host=localhost port={os.environ.get('WAREHOUSE_PORT', '5434')} "
        f"dbname={os.environ['POSTGRES_DB']} user=dbt_transform "
        f"password={os.environ['DBT_TRANSFORM_PASSWORD']}"
    ) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def load_alerts(first: date, last: date) -> list[dict]:
    return _query("select * from marts.mart_bunching_alerts "
                  "where local_date between %s and %s", (first, last))


def load_feed_health(first: date, last: date) -> list[dict]:
    return _query("select minute_local, positions, is_silent, median_ingest_lag_s "
                  "from marts.mart_feed_health "
                  "where minute_local::date between %s and %s", (first, last))


def load_prediction_intermediates(first: date, last: date) -> tuple[list[dict], list[dict]]:
    # UTC days: the intermediates' unit. See prediction_curve().
    hist = _query("select observed_day, lead_bucket, abs_error_s, predictions "
                  "from marts.int_prediction_error_histogram "
                  "where observed_day between %s and %s", (first, last))
    daily = _query("select observed_day, lead_bucket, predictions, sum_error_s, min_lead_time_s "
                   "from marts.int_prediction_bucket_daily "
                   "where observed_day between %s and %s", (first, last))
    return hist, daily


def write(name: str, payload, first: date, last: date) -> Path:
    """One file, carrying the window it was cut from.

    NO "generated" TIMESTAMP, deliberately. These files are committed, and a
    re-export of an unchanged window should be byte-identical so git shows no
    change, the property tests/fixtures/warehouse/export.sh keeps for the same
    reason. The window says what the data is; the commit says when.

    A GeoJSON file stays a FeatureCollection at the top level, so any GeoJSON
    reader takes it; the window rides along as a foreign member, which the spec
    (RFC 7946 section 6.1) allows.
    """
    window = {"first": first.isoformat(), "last": last.isoformat()}
    if name.endswith(".geojson"):
        body = {**payload, "window": window}
    else:
        body = {"window": window, "data": payload}
    path = OUT / name
    path.write_text(json.dumps(body, indent=1, sort_keys=True, default=str) + "\n")
    return path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="publish.export", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--first", type=date.fromisoformat, default=STUDY_FIRST)
    ap.add_argument("--last", type=date.fromisoformat, default=STUDY_LAST)
    args = ap.parse_args(argv)
    try:
        first, last = closed_window(args.first, args.last)
    except ValueError as exc:
        raise SystemExit(str(exc)) from None
    if last != args.last:
        # Said out loud, because every file records the window it was cut from:
        # a request for the study week that produced three days should not look
        # like a cut that was asked for.
        print(f"  --last {args.last} has not finished; cutting to {last}")
    days = window_days(first, last)
    OUT.mkdir(parents=True, exist_ok=True)

    alerts = load_alerts(first, last)
    health = feed_health_by_day(load_feed_health(first, last))
    curve = prediction_curve(*load_prediction_intermediates(first, last))

    written = [
        write("bunching_by_hour.json", alerts_by_hour(alerts, days), first, last),
        write("routes.json", route_ranking(alerts, days), first, last),
        write("hotspots.geojson", hotspots(alerts), first, last),
        write("prediction_curve.json", curve, first, last),
        write("feed_health.json", health, first, last),
        write("kpis.json", kpis(alerts, health, curve, days), first, last),
    ]
    for p in written:
        print(f"  {p.relative_to(ROOT)}  {p.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
