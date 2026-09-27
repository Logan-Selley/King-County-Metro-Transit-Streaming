"""Bunching detection logic, with no Flink in it.

This module imports nothing from pyflink and nothing from protobuf, on
purpose. `job.py` is the Flink wiring, `decode.py` turns the topic's protobuf
bytes into dicts, and everything that can be wrong about the *detection*
lives here, where the project venv can import it and
tests/test_bunching_contract.py can run against it with no cluster at all.
Dicts in, alerts out.

That split is the same one enrich.py and run.py use: enrich.py holds the
logic, run.py holds the Kafka loop, and the contract suite only ever touches
the first.

--- the two gates, and why neither is optional ---

Both are measured against the live topic, and each produces confident false
alerts if skipped.

1. INCOMPARABLE SHAPES. A (route_id, direction_id) pair does not imply one
   geometry. Measured on the loaded feed: 101 of 280 pairs have more than one
   shape, covering 14,054 of 31,688 trips (44.4%). shape_dist_traveled on two
   different shapes is measured from two different origins, and 92 shape pairs
   start more than 500 m apart -- so equal distances do not mean adjacent.
   `confirm_proximity` is the gate. See ADR 0007.

2. TERMINAL LAYOVERS. 12.6% of enriched records report shape_dist_traveled
   of exactly 0.0, and 95.6% of those are STOPPED_AT. Sampled and checked
   against PostGIS, every one is 1.7-86.5 m from its shape's start point, so
   the projection is correct and the loader is fine: these are buses sitting
   at the first stop waiting to depart. Two of them have a gap near zero and
   would alert. That is a layover, not bunching. `MIN_PROGRESS_FT` is the
   gate.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from consumers.bunching.config import CONFIG, BunchingConfig

# Below this distance along the shape, a vehicle has not started its trip.
#
# The number comes from a distribution that turned out to be bimodal, which is
# what makes a simple threshold safe here. Sampling 8,000 live records:
#
#     == 0 ft     12.6%      <- layovers at the first stop
#     < 100 ft     0.9%
#     < 1,000 ft   2.6%
#     >= 1,000 ft 83.9%
#
# Almost nothing sits between "exactly zero" and "under way", so 100 ft
# discards 0.9% of real positions to remove the entire layover population.
# A vehicle genuinely bunched 80 ft into its route is a rounding error against
# that trade.
MIN_PROGRESS_FT = 100.0

# A vehicle still within its first few stops has not left its terminal, and
# THIS is the gate that actually works. MIN_PROGRESS_FT above is kept because
# it is cheap and catches the exact-zero case, but it measures the wrong
# thing: distance along the shape only identifies "at the terminal" when the
# terminal sits at the shape's origin.
#
# Route 255 is where that broke. 79 alerts, 4th busiest route in a day, from
# a suburban express that has no business out-ranking most of RapidRide.
# Median gap: 0 ft. 49 of 79 under 50 ft. 87% of them clustered at
# shape_dist ~1,600 ft on a 74,000 ft shape -- which is stops 1-3, just past
# Totem Lake Transit Center, where coaches stage before departing. The shape
# origin is 1,600 ft away from where the buses actually wait, so a distance
# gate set at 100 ft never saw them.
#
# The two populations are distinct rather than a continuum, which is what
# makes a threshold honest here:
#
#     stop_seq 1-3   n=1,201   34.3% of pairs under 25 ft apart
#     stop_seq 4+    n=3,906    3.8%
#
# Effect of the gate on the day's alerts:
#
#     255       79 -> 10      G Line   114 -> 104
#     7        108 -> 90      E Line   102 -> 99
#                             36        39 -> 37
#
# The cost is real: bunching genuinely happening within three stops of a
# terminal is discarded, and route 7 loses 17%. That is accepted because
# buses leave a terminal on a dispatch schedule rather than a headway, so
# two of them close together there is a dispatching artifact. Bunching that
# matters to a rider develops along the route.
MIN_STOP_SEQUENCE = 4

FT_PER_M = 1.0 / 0.3048
EARTH_RADIUS_M = 6_371_008.8


# --- geometry ----------------------------------------------------------------


def haversine_ft(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two positions, in feet.

    Feet because everything else in this detector is feet (config.py explains
    why: shape_dist_traveled is published in feet for 423 of 424 shapes).
    Mixing units here is the exact mistake the unit note in config.py warns
    about, so the conversion happens once, at the end, in this function.

    Haversine rather than a projected CRS. At King County's scale the error
    against a proper geodesic is centimetres over a kilometre, and the
    alternative means shipping pyproj into the Flink image for no gain.
    """
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a)) * FT_PER_M


def confirm_proximity(a: dict, b: dict, gap_ft: float, threshold_ft: float) -> bool:
    """Is a candidate pair physically close, or a shape artifact?

    This is gate 1 from the module docstring, and it exists because the
    obvious fixes are both worse:

      * Keying on (route, direction, shape_id) splits vehicles that ARE
        comparable. A Line direction 1 runs two shapes that share a start
        point and differ by 44 m in total length; keying on shape would stop
        comparing 101 of its 310 trips against the other 209, on one of the
        exact RapidRide lines the detector has to be credible on.

      * Precomputing which shapes are compatible needs the static feed inside
        the Flink job, which means either a broadcast stream or a second
        connection to PostGIS from a TaskManager.

    The cheap answer is already on every record. For two points on the same
    path, straight-line distance is never greater than distance along it, so a
    genuine pair satisfies haversine <= gap. A pair whose shapes start 8 km
    apart reports a small gap and a huge straight-line distance, and fails.

    The conjunction is what makes it right in both directions: shape distance
    alone accepts incomparable geometries, straight-line distance alone calls
    two buses bunched when they are on opposite legs of a loop, near each
    other on a map and 5 km apart along the route.

    `threshold_ft` rather than `gap_ft` as the bound, with a small allowance:
    GPS scatter and shape generalisation both put a real pair slightly over.
    """
    if a.get("latitude") is None or b.get("latitude") is None:
        # No position to confirm against. Refuse rather than assume -- an
        # unconfirmable candidate is exactly the cross-shape case this exists
        # to catch.
        return False
    straight = haversine_ft(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
    return straight <= max(gap_ft, threshold_ft) * 1.25


# --- detection ---------------------------------------------------------------


def parse_record(rec: dict) -> dict | None:
    """Decoded enriched record -> the fields the detector needs.

    Takes a DICT, not a JSON string. decode.decode() has already turned the
    topic's protobuf bytes into a dict by the time this runs, so the only job
    here is narrowing.

    Narrows 29 decoded keys to the 10 the detector uses. Dropping the rest
    early matters more here than in the enrichment consumer, because
    everything retained crosses the Python/JVM boundary on every record.

    Returns None for a record the detector cannot use, leaving the caller to
    filter. `required_keys` is the smaller list on purpose:
    `route_short_name` and `schedule_deviation_seconds` are carried for the
    alert payload but a record without them is still perfectly usable for
    detection, while a record without a position is not -- confirm_proximity
    has nothing to confirm against.

    The None test is on the VALUE, not on membership, because
    decode.to_dict() maps an unset optional field to None rather than
    omitting the key. `"shape_dist_traveled" in rec` is therefore always true
    and would gate on nothing.

    Never raises. An exception here takes down the TaskManager slot, which
    restarts the job from its last checkpoint, so one malformed record would
    become a crash loop rather than a dropped record.
    """
    bunch_keys = [
        "vehicle_id", "route_id", "direction_id", "trip_id",
        "shape_dist_traveled", "position_timestamp", "latitude", "longitude",
        "route_short_name", "schedule_deviation_seconds",
        # Not for the alert payload -- for the MIN_STOP_SEQUENCE gate.
        "current_stop_sequence",
    ]
    required_keys = [
        "vehicle_id", "route_id", "direction_id", "shape_dist_traveled",
        "position_timestamp", "latitude", "longitude",
    ]
    if not isinstance(rec, dict):
        return None
    bunch_record = {k: rec.get(k) for k in bunch_keys}
    if any(bunch_record[k] is None for k in required_keys):
        return None
    return bunch_record


def assign_route_key(rec: dict) -> str:
    """Partition key for the detector.

    (route_id, direction_id) -- NOT route_id alone. The two directions of a
    route run on different shapes, so their shape_dist_traveled values are not
    comparable, and a northbound bus at 12,000 ft is not "near" a southbound
    one at 12,100 ft. Keying on route alone produces confident nonsense at
    every terminus.

    Not (route_id, direction_id, shape_id) either -- that was considered and
    rejected in ADR 0007. Shape variants are handled by confirm_proximity
    instead, which keeps the comparisons that a shape key would throw away.

    A string rather than a tuple. Flink needs a hashable, serialisable key,
    and a tuple crossing the Python/JVM boundary without an explicit type is
    where PyFlink produces a pickled blob instead of an error.

    The topic is partitioned by vehicle_id for per-vehicle ordering
    (ADR 0002), so route-level analysis has to shuffle. That shuffle is the
    price of grouping by route rather than by vehicle.
    """
    return f"{rec['route_id']}:{rec['direction_id']}"


def detect_in_window(
    key: str,
    records: list[dict],
    window_end_s: float,
    min_stop_sequence: int = MIN_STOP_SEQUENCE,
) -> list[dict]:
    """Find bunched pairs among one route-direction's positions in one window.

    Given every position for one (route, direction) inside a 60s event-time
    window, emit an alert dict per bunched PAIR. `window_end_s` is the
    window's end as epoch seconds, passed in rather than derived so this
    stays a pure function.

    Five steps, each discarding something that would otherwise become a
    false alert:

      * One position per vehicle, the latest by position_timestamp. A 60s
        window holds ~3 observations per vehicle at the measured 20s publish
        rate, and comparing all of them would count the same pair three
        times.

      * Vehicles older than CONFIG.max_position_age_s relative to
        window_end_s are dropped. A stale-burst position measures where a bus
        WAS; pairing it against a fresh one invents a gap that closed minutes
        ago.

      * Vehicles under MIN_PROGRESS_FT, or still within their first
        `min_stop_sequence` stops, are dropped. Gate 2 -- see the module
        docstring. Two buses at the terminal are a layover, and the stop
        sequence is what catches it on routes whose terminal is not at the
        shape's origin. The gate is a PARAMETER, defaulting to
        MIN_STOP_SEQUENCE, because a replay runs this same function twice with
        the gate on (a baseline that must reproduce the live output) and off
        (the variant). A copy of this function with the gate removed would not
        be the live logic under test.

      * Sorted by shape_dist_traveled, CONSECUTIVE pairs only. All-pairs is
        O(n^2) and wrong besides: three buses in a row are two bunched pairs,
        not three.

      * A gap below CONFIG.gap_threshold_ft is a CANDIDATE, and
        confirm_proximity decides whether it becomes an alert. Gate 1 -- see
        the module docstring.

    The emitted alert carries both schedule deviations because they are what
    make it interpretable: bunching with one bus 8 minutes late is a
    different story from two buses both on time.

    `window_end_s` is passed in rather than derived from the records so this
    stays a pure function, testable without a window.

    UNITS: shape_dist_traveled is in feed units, FEET for 423 of 424 shapes.
    See the note in config.py. Nothing here converts to metres, and nothing
    compares across routes.

    Returns [] rather than None when nothing is bunched.
    """
    latest = {}
    for rec in records:
        current = latest.get(rec["vehicle_id"])
        if current is None or rec["position_timestamp"] > current["position_timestamp"]:
            latest[rec["vehicle_id"]] = rec
    usable_record = [
        rec for rec in latest.values()
        if window_end_s - rec["position_timestamp"] <= CONFIG.max_position_age_s
        and rec["shape_dist_traveled"] >= MIN_PROGRESS_FT
        # Absent stop sequence fails the gate rather than passing it. The
        # field is 99.6% populated, so the rare miss is cheaper than admitting
        # a vehicle whose progress cannot be checked.
        and (rec.get("current_stop_sequence") or 0) >= min_stop_sequence
    ]
    sorted_records = sorted(usable_record, key=lambda x: x["shape_dist_traveled"])
    alerts = []
    for lead, follow in zip(sorted_records, sorted_records[1:]):
        distance_gap = follow["shape_dist_traveled"] - lead["shape_dist_traveled"]
        if distance_gap >= CONFIG.gap_threshold_ft:
            continue
        if not confirm_proximity(lead, follow, distance_gap, CONFIG.gap_threshold_ft):
            continue
        alerts.append({
            "route_id": lead["route_id"],
            "direction_id": lead["direction_id"],
            "route_short_name": lead["route_short_name"],
            "vehicle_id_a": lead["vehicle_id"],
            "vehicle_id_b": follow["vehicle_id"],
            "trip_id_a": lead["trip_id"],
            "trip_id_b": follow["trip_id"],
            "gap_ft": distance_gap,
            "window_end": window_end_s,
            "deviation_a": lead["schedule_deviation_seconds"],
            "deviation_b": follow["schedule_deviation_seconds"],
        })
    return alerts


# --- cross-window state ------------------------------------------------------


@dataclass(frozen=True)
class BunchingState:
    """Per-pair memory that spans windows.

    detect_in_window answers "is this pair bunched right now". It cannot answer
    "has it been bunched two windows running, and did we already say so" -- one
    window has no memory of the last. CONFIG.cooldown_s and
    CONFIG.min_consecutive_windows are that memory.

    They live here rather than in job.py because they are detection logic, not
    wiring, and this is the module the contract suite can import. Expressed
    inside a KeyedProcessFunction the cooldown is reachable only through a
    running cluster.

    A gap resets the run. A pair that separates produces NO records, so the
    reset cannot come from observation -- it is inferred from the window
    timestamps, which is why last_window_end is stored. Self-healing, at the
    cost of leaving an entry behind for a pair that never comes back;
    StateTtlConfig bounds that if it ever matters.

    Immutable: emit() returns the new state rather than mutating, so a caller
    that dies between the decision and the ValueState write cannot leave the
    counter half-updated.
    """

    consecutive: int = 0
    last_window_end: float | None = None
    last_alert_s: float | None = None

    def emit(self, window_end_s: float,
             config: BunchingConfig = CONFIG) -> tuple[bool, BunchingState]:
        """Fold one window in, and decide whether it is worth an alert.

        Called only for a window in which this pair IS bunched -- presence is
        the caller's guarantee, so there is no "not bunched" branch here.
        """
        if (self.last_window_end is not None
                and window_end_s - self.last_window_end > config.window_s):
            consecutive = 0                  # missed a window: they separated
        else:
            consecutive = self.consecutive
        consecutive += 1

        # The cooldown gates the EMIT, not the count: a pair still bunched
        # twenty minutes later re-alerts once per cooldown rather than never.
        cooled = (self.last_alert_s is None
                  or window_end_s - self.last_alert_s >= config.cooldown_s)
        should_emit = consecutive >= config.min_consecutive_windows and cooled

        return should_emit, BunchingState(
            consecutive=consecutive,
            last_window_end=window_end_s,
            # Deliberately survives a separation: a pair that separates and
            # rejoins inside cooldown_s is jitter, not a new episode, and
            # alerting on every rejoin is the spam this cooldown prevents.
            last_alert_s=window_end_s if should_emit else self.last_alert_s,
        )

    def to_dict(self) -> dict:
        """A plain dict, not the dataclass, for ValueState.

        PyFlink pickles either one, but a dict survives a field being added
        later: a pickled dataclass restores without calling __init__ and would
        raise AttributeError on the new field instead.
        """
        return {
            "consecutive": self.consecutive,
            "last_window_end": self.last_window_end,
            "last_alert_s": self.last_alert_s,
        }

    @classmethod
    def from_dict(cls, raw: dict | None) -> BunchingState:
        """ValueState is None the first time a pair is seen.

        .get() with defaults rather than **raw: state written by an earlier
        field set has to survive a savepoint restore, and a missing key would
        be a TypeError at job start rather than a fresh counter.
        """
        raw = raw or {}
        return cls(
            consecutive=raw.get("consecutive", 0),
            last_window_end=raw.get("last_window_end"),
            last_alert_s=raw.get("last_alert_s"),
        )


def pair_key(alert: dict) -> str:
    """Cooldown state key: the vehicle PAIR, order-normalised.

    detect_in_window orders each pair by shape_dist_traveled, so lead/follow
    flips when the two vehicles overtake or when one is momentarily projected
    behind the other. Keying on that order splits one pair's history across two
    keys, consecutive never reaches min_consecutive_windows, and the detector
    emits nothing at all -- silently, with a healthy-looking job.

    Vehicle ids only, no route. A vehicle runs one trip at a time, so a pair of
    ids identifies the pair; adding the route would only make the key longer to
    read in the Flink UI.
    """
    a, b = sorted((alert["vehicle_id_a"], alert["vehicle_id_b"]))
    return f"{a}|{b}"


