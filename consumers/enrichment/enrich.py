"""raw.vehicle_positions -> enriched.vehicle_positions.

--- v1 and v2 exist as separate functions on purpose ---

v1 is the static join. v2 adds the spatial pass. They are separate because the
step from one to the other IS the schema evolution exercise (ADR 0005): v1
ships, produces records against schema version 1, and then v2 adds optional
fields and bumps to version 2 while v1 consumers keep reading.

Writing one function with a `spatial=True` flag would collapse that into a
config change and lose the demonstration.
"""

from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from consumers.enrichment.reference import ReferenceData, TripRef
from producer.errors import MAX_PLAUSIBLE_DEVIATION_S, DlqReason
from static.feed import AGENCY_TZ

log = logging.getLogger("enrichment.enrich")


@dataclass
class EnrichStats:
    """Counters for things that are wrong but not rejections.

    `implausible_deviation` is an ALARM, not a tally. It should sit at exactly
    zero. It fires all-or-nothing by nature -- a wrong service-date anchor or
    a signedness change is systemic, so the count jumps from 0 to ~100% of
    records rather than drifting. A nonzero value means a bug shipped, not
    that the buses had a bad day.
    """

    deviations_computed: int = 0
    implausible_deviation: int = 0

    @property
    def implausible_rate(self) -> float:
        total = self.deviations_computed + self.implausible_deviation
        return self.implausible_deviation / total if total else 0.0

    def __str__(self) -> str:
        return (f"deviations={self.deviations_computed:,} "
                f"implausible={self.implausible_deviation:,}")


# Module-level because enrich_v2 is a plain function called per record, and
# threading a stats object through every call would change a signature the
# contract tests pin. run.py reads this for its periodic report.
STATS = EnrichStats()


@dataclass(frozen=True)
class EnrichmentResult:
    """What one input record produced.

    A three-way outcome rather than Optional, because "could not enrich" and
    "enriched to nothing" need different handling downstream and collapsing
    them into None loses the reason.
    """

    record: dict | None            # the enriched payload, or None on reject
    reason: DlqReason | None = None
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.record is not None


# --- enrichment ---------------------------------------------------------


def enrich_v1(raw: dict, ref: ReferenceData) -> EnrichmentResult:
    """Join one raw position to static GTFS.

    `raw` is the JSON dict produced by producer/publish.py's serialize() --
    the field names match VehiclePositionRecord.

    Contract:

      * Resolve raw["trip_id"] through ref.trip(). On None, return an
        EnrichmentResult with reason=UNKNOWN_TRIP_ID and the trip_id in
        detail.

        This rate measures STALENESS, not feed noise: against a static feed
        loaded the day it was published, 100.0% of live trip_ids resolved;
        against one two weeks old, 97.4%. There is no baseline to tolerate --
        near zero is healthy, and a climb means the static load is ageing.

      * Carry through every identity and raw-state field unchanged. The
        enriched record is a superset of the raw one -- a consumer should
        never have to join back to raw.vehicle_positions to get the position
        it started from.

      * Add from TripRef: route_short_name, route_long_name, route_type,
        trip_headsign, block_id, shape_id, service_id.

      * Set static_feed_version = ref.version_id. Without it a deviation
        figure cannot be reproduced after a service change, because the
        schedule it was computed against is gone.

      * Set enriched_at to now in epoch seconds, UTC.

      * Field names must match schemas/enriched_vehicle_position.proto
        exactly. The ProtobufSerializer maps by name and a typo becomes a
        silently-unset optional field, not an error.

    Do NOT add spatial fields here. Those are v2 and adding them early
    forfeits the evolution demonstration.
    """
    trip = ref.trip(trip_id=raw["trip_id"])
    if trip is None:
        return EnrichmentResult(
            None, DlqReason.UNKNOWN_TRIP_ID, f"trip_id={raw['trip_id']}"
        )
    record = {
        **raw,
        # From TripRef, i.e. the static join:
        "route_short_name": trip.route_short_name,
        "route_long_name": trip.route_long_name,
        "route_type": trip.route_type,
        "trip_headsign": trip.trip_headsign,
        # raw carries block_id=None (the field exists on the record; the basic
        # protobuf never populates it), so this replaces None with the static
        # join's value. See the ADR 0003 revision.
        "block_id": trip.block_id,
        "shape_id": trip.shape_id,
        "service_id": trip.service_id,
        # Lineage: which static load this was joined against.
        "static_feed_version": ref.version_id,
        "gtfs_feed_version": ref.feed_version,
        "enriched_at": int(datetime.now(timezone.utc).timestamp()),
    }
    return EnrichmentResult(record, None, "")


def schedule_deviation(
    ref: ReferenceData,
    trip_id: str,
    dist_travelled: float,
    observed_at: datetime,
    start_date: str | date | None,
) -> int | None:
    """Seconds ahead (negative) or behind (positive) schedule.

    The idea: the vehicle is at `dist_travelled` along its shape. The schedule
    says it should have been at that distance at some time T. The deviation is
    observed_at - T.

    ScheduleRef holds parallel ascending lists (dists, seconds), so:

      * look up ref._schedules[trip_id]           -> None means no schedule
      * interpolate(sched.dists, sched.seconds, dist_travelled) -> offset
      * origin = service_date_origin(start_date)  -> absolute instant, UTC
      * scheduled = origin + timedelta(seconds=offset)
      * return int((observed_at - scheduled).total_seconds())

    --- where start_date comes from, and why it is a PARAMETER ---

    It is NOT on TripRef, and it cannot be. `trips.txt` has no date column: a
    trip_id is a repeating PATTERN, and which calendar dates it runs on is
    determined by service_id against calendar/calendar_dates. The same
    trip_id runs on hundreds of dates.

    The date lives on the REALTIME record. GTFS-RT puts `start_date` on
    VehiclePosition.trip and Metro populates it on 100% of entities, so
    enrich_v2 passes `raw["start_date"]` straight through.

    Note it arrives ISO-formatted ("2026-09-03") rather than GTFS-formatted
    ("20260903"), because producer/publish.serialize() round-trips the decoded
    `date` through json.dumps. service_date_origin accepts both.

    --- do NOT call ref.trip() in here ---

    It mutates ref.stats.trip_hits/misses, and enrich_v1 has already resolved
    this trip. Calling it again double-counts every record and dilutes the
    staleness signal those counters exist to provide. Nothing in the
    arithmetic below needs TripRef anyway.

    --- the arithmetic, and why it is not "seconds since midnight" ---

    Measured against FAL26-161.1 (docs/findings.md section 10):

        offsets present         03:52:00 .. 29:30:00   (a 30-hour span)
        hours 00:00-02:59       ZERO rows
        trips with offset >=24h 1,433 of 31,688 = 4.5%

    Metro's service day starts around 06:00 and the pre-06:00 hours belong to
    the PREVIOUS service date's overnight tail. So at 04:30 clock time two
    populations are on the road simultaneously: new-day trips at offset
    04:30:00 and previous-day trips at offset 28:30:00. Same instant, same
    clock, different service dates.

    Two consequences, both load-bearing:

      1. NEVER fold an offset modulo 24h. Normalising 29:30 to 05:30 moves a
         trip 24 hours, which is not a rounding error -- it is a wrong answer
         of the same magnitude as the symptom this docstring warns about.

      2. Anchor on the TRIP's service date, not the observation's calendar
         date. The realtime feed hands you `start_date` on every position
         (100% populated), so you never have to infer it -- and inferring it
         would be ambiguous in the 04:00-05:59 overlap, which is exactly where
         it matters.

         Using the observation's calendar date instead gives a clean 24-hour
         deviation on 4.5% of trips, visible only during overnight service.

    Return None, do not guess, when:

      * the trip has no schedule (no stop_times with shape_dist_traveled)
      * dist_travelled falls outside [dists[0], dists[-1]] -- the vehicle is
        before its first timepoint or past its last, and extrapolating off
        the end of a schedule produces confident nonsense
      * fewer than 2 timepoints, so there is nothing to interpolate between
      * start_date is absent or unparseable

    Sanity-check against reality before trusting the output. A few minutes is
    ordinary. A 24-hour deviation means the service-date anchor is wrong; a
    7-8 hour one means the timezone is (service_date_origin returns an instant
    in the AGENCY's timezone, not UTC midnight -- that bug was in this file).
    """
    sched = ref._schedules.get(trip_id)
    if sched is None:
        return None
    offset = interpolate(sched.dists, sched.seconds, dist_travelled)
    if offset is None:
        return None
    origin = service_date_origin(start_date)
    if origin is None:
        return None
    scheduled = origin + timedelta(seconds=offset)
    deviation = int((observed_at - scheduled).total_seconds())

    # Value-domain bound, checked here because the schema gate cannot (ADR
    # 0005). A deviation past 3 hours is not a late bus; it is one of the
    # three failure modes this docstring names -- a UTC-midnight origin
    # (~7-8h), a calendar-date anchor instead of the service date (~24h), or
    # an int32->uint32 signedness change that wraps every early bus to ~4.29e9.
    #
    # Nulled rather than DLQ'd: the position, neighborhood and shape distance
    # on this record are all still correct. Only the deviation is untrustworthy,
    # and throwing the record away would lose three good fields to save one bad
    # one. The counter is what makes it visible.
    if abs(deviation) > MAX_PLAUSIBLE_DEVIATION_S:
        STATS.implausible_deviation += 1
        if STATS.implausible_deviation == 1:
            # Once, loudly, with the evidence. This fires all-or-nothing, so
            # logging every record would emit a line per position for the rest
            # of the run and bury the one that mattered.
            log.error(
                "implausible deviation %+ds for trip %s (bound %ds) -- "
                "check the service-date anchor, the timezone origin, and the "
                "schema's signedness; suppressing further per-record logs",
                deviation, trip_id, MAX_PLAUSIBLE_DEVIATION_S,
            )
        return None

    STATS.deviations_computed += 1
    return deviation



def enrich_v2(raw: dict, ref: ReferenceData) -> EnrichmentResult:
    """v1, plus linear referencing, schedule deviation, and neighborhood.

    Build on enrich_v1 rather than duplicating it -- call it, return early if
    it rejected, then add to its record.

    Adds:
      * shape_dist_traveled        ref.locate_on_shape(shape_id, lon, lat)
      * schedule_deviation_seconds schedule_deviation(ref, trip_id, dist,
                                       observed_at, raw["start_date"])
      * neighborhood_name, neighborhood_num  ref.neighborhood(lon, lat)

    Two plumbing notes, both of which are easy to get subtly wrong:

      * `shape_id` comes from the v1 record (the static join), not from raw.
        Raw positions have no shape_id at all.

      * `observed_at` must be built from raw["position_timestamp"], which is
        an ISO-8601 STRING after the JSON round-trip, not a datetime. Parse
        it with datetime.fromisoformat and keep it timezone-aware -- a naive
        datetime subtracted from an aware one raises, which is at least loud,
        but a naive one treated as UTC when it is local is silent and 7 hours
        wrong.

    ALL FOUR ARE OPTIONAL AND NULL IS A LEGITIMATE VALUE:

      * no shape_id on the trip -> no distance, no deviation
      * position outside the schedule's distance range -> no deviation
      * outside every neighborhood polygon -> no neighborhood, and this is
        16.6% of the fleet, not an error

    A record that reaches v2 has already resolved its trip, so none of these
    nulls are DLQ candidates. The only DLQ reason in this path remains
    UNKNOWN_TRIP_ID, inherited from v1.

    Adding these as optional protobuf fields is BACKWARD compatible, so the
    registry accepts the bump and v1 consumers keep reading v2 records. That
    is the demonstration; see ADR 0005.
    """
    enriched = enrich_v1(raw, ref)
    if enriched.record is None:
        return enriched

    record = enriched.record
    lon, lat = record["longitude"], record["latitude"]

    # Linear referencing. None when the trip has no shape_id or the shape has
    # no max_dist; a distance is also a precondition for a deviation, since
    # there is nothing to interpolate against without one.
    shape_id = record.get("shape_id")
    dist = ref.locate_on_shape(shape_id, lon, lat) if shape_id else None
    record["shape_dist_traveled"] = dist

    record["schedule_deviation_seconds"] = (
        None
        if dist is None
        else schedule_deviation(
            ref,
            record["trip_id"],
            dist,
            datetime.fromisoformat(record["position_timestamp"]),
            record.get("start_date"),
        )
    )

    # (neigh_num, name) or None. A null is normal -- water taxi, out-of-county
    # Sound Transit Express, boundary gaps -- and is never a DLQ candidate.
    hood = ref.neighborhood(lon, lat)
    record["neighborhood_num"], record["neighborhood_name"] = hood or (None, None)

    return EnrichmentResult(record, None, "")



# --- helpers ------------------------------------------------------------------


def interpolate(xs: list[float], ys: list[int], x: float) -> float | None:
    """Linear interpolation on ascending xs.

    Returns None outside the range rather than extrapolating: a vehicle past
    its last timepoint has no scheduled position, and inventing one produces
    a deviation that looks authoritative and is not.
    """
    if len(xs) < 2 or x < xs[0] or x > xs[-1]:
        return None
    i = bisect.bisect_left(xs, x)
    if i == 0:
        return float(ys[0])
    if xs[i] == xs[i - 1]:
        return float(ys[i])
    span = (x - xs[i - 1]) / (xs[i] - xs[i - 1])
    return ys[i - 1] + span * (ys[i] - ys[i - 1])


def service_date_origin(start_date: str | date | None) -> datetime | None:
    """The instant a GTFS service date begins, as an absolute UTC datetime.

    NOT midnight UTC. GTFS stop_times are offsets from the start of a service
    date in the AGENCY's timezone (America/Los_Angeles, declared in
    agency.txt). Using UTC midnight is 7-8 hours wrong every day, and presents
    as the "service date is wrong" symptom schedule_deviation warns about.

    --- noon minus 12 hours, not midnight ---

    The GTFS spec defines the origin as **noon minus 12 hours** on the service
    date, and that phrasing is deliberate rather than pedantic. On the two DST
    transition days a local day is 23 or 25 hours long, so "midnight" and
    "noon minus 12h" differ by an hour:

        2026-03-08  spring forward, 23-hour day
        2026-11-01  fall back,      25-hour day

    Anchoring on noon -- which is never ambiguous and never skipped -- and
    subtracting 12 hours gives the origin the schedule was actually written
    against. Anchoring on midnight gets those two days wrong, and on the fall
    -back day 01:30 local is genuinely ambiguous (it happens twice), so
    zoneinfo has to pick one.

    This matters more here than it would elsewhere because 4.5% of Metro trips
    (1,433 of 31,688) carry offsets past 24:00:00, so the overnight tail --
    precisely the population that crosses a DST boundary -- is not a rounding
    error.

    --- accepts both date spellings, on purpose ---

    The same service date reaches this function in two legitimate formats,
    because it crosses a serialization boundary:

        "20260903"     GTFS wire format, straight off the protobuf
        "2026-09-03"   after producer/publish.serialize() round-trips the
                       decoded `date` through json.dumps -> isoformat()
        date(2026,9,3) if a caller hands over the decoded object

    An earlier version parsed only the first and returned None for the
    second -- which is what enrich_v2 actually receives. That failed
    SILENTLY: schedule_deviation would return None for every record and look
    exactly like "this trip has no schedule". Rejecting a valid date you
    simply cannot spell is worse than accepting two spellings.
    """
    if start_date is None:
        return None

    # datetime before date: datetime subclasses date, so the order matters.
    if isinstance(start_date, datetime):
        day = start_date.date()
    elif isinstance(start_date, date):
        day = start_date
    else:
        day = None
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                day = datetime.strptime(str(start_date).strip(), fmt).date()
                break
            except ValueError:
                continue
        if day is None:
            return None

    noon = datetime(day.year, day.month, day.day, 12, tzinfo=ZoneInfo(AGENCY_TZ))
    return (noon - timedelta(hours=12)).astimezone(timezone.utc)

