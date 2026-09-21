"""In-memory static reference for the enrichment consumer.

--- why in memory and not a query per record ---

1.1M positions a day is ~13/s average and ~40/s at peak. That is nothing for
Python and quite a lot of round trips to Postgres: three lookups per record
(trip, shape, neighborhood) is 120 queries/s at peak, each with connection
and planning overhead, to answer questions whose entire answer set fits in
tens of megabytes.

The whole static reference is small:

    trips            31,688 rows
    shapes             ~430 linestrings (167,088 points)
    stop_times      1,101,970 rows   <-- the one that needs thought
    neighborhoods       350 polygons

Everything except stop_times loads flat. stop_times is 1.1M rows and does NOT
need to be held whole: the enrichment needs, per trip, the scheduled
progression along the shape, which is a few dozen (shape_dist_traveled, time)
pairs. Load it grouped by trip and keep only those pairs -- roughly 31k lists
averaging 34 entries, not 1.1M row objects.

--- the version pin ---

ReferenceData is pinned to ONE static feed version at construction and never
silently follows `static.current_version()`. A consumer that re-resolved the
current version mid-stream would change the meaning of its output halfway
through a service day, and the enriched records would carry a
static_feed_version that no longer matched what was actually joined. Picking
up a new version is a deliberate reload -- see run.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from itertools import groupby
from typing import Any

import psycopg
import shapely
import shapely.wkb  # submodule: `import shapely` does not bind it in shapely 2.x
from psycopg.rows import class_row

log = logging.getLogger("enrichment.reference")


@dataclass(frozen=True)
class TripRef:
    """Everything the static join contributes for one trip."""

    trip_id: str
    route_id: str
    service_id: str | None
    trip_headsign: str | None
    direction_id: int | None
    block_id: str | None
    shape_id: str | None
    route_short_name: str | None
    route_long_name: str | None
    route_type: int | None


@dataclass(frozen=True)
class ShapeRef:
    """One trip pattern's geometry, prepared for linear referencing."""

    shape_id: str
    geom: object            # shapely LineString, WGS84
    max_dist: float | None  # feed units, from shapes.shape_dist_traveled
    n_points: int


@dataclass(frozen=True)
class ScheduleRef:
    """A trip's scheduled progression along its shape.

    Parallel lists rather than a list of pairs: they are consumed by bisect
    during interpolation, which wants a sortable sequence, and this avoids
    building 1.1M tuples.

    `seconds` is seconds since noon-minus-12h on the service date, NOT a
    clock time -- GTFS times legitimately exceed 24:00:00 for trips running
    past midnight, and "25:14:00" means 01:14 the next day. Keeping the
    offset uncollapsed is what makes an after-midnight trip comparable to a
    vehicle timestamp instead of wrapping to the previous morning.
    """

    trip_id: str
    dists: list[float]    # shape_dist_traveled at each timepoint, ascending
    seconds: list[int]    # scheduled arrival offsets, ascending


@dataclass
class ReferenceStats:
    trips: int = 0
    shapes: int = 0
    schedules: int = 0
    neighborhoods: int = 0
    trip_hits: int = 0
    trip_misses: int = 0

    @property
    def join_rate(self) -> float:
        total = self.trip_hits + self.trip_misses
        return self.trip_hits / total if total else 0.0

@dataclass(frozen=True)
class _StopTimeRow:
    trip_id: str
    arrival_time: str
    shape_dist_traveled: float


@dataclass(frozen=True)
class _ShapeRow:
    shape_id: str
    max_dist: float | None
    n_points: int
    geom: bytes


@dataclass(frozen=True)
class _NeighborhoodRow:
    neigh_num: int
    neighborhood_name: str
    geom: bytes


def _hms(clock: str) -> int:
    """'HH:MM:SS' -> seconds. Plain arithmetic on purpose: strptime rejects
    hour 25 outright, and Metro publishes offsets up to 29:59:59."""
    h, m, s = clock.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


class ReferenceData:
    """Static GTFS and spatial layers, held in memory, pinned to one version."""

    def __init__(self, version_id: int, feed_version: str | None = None) -> None:
        self.version_id = version_id
        # The agency's own label for this load, e.g. "FAL26-161.1". Carried
        # onto every enriched record alongside version_id: the surrogate key
        # is exact but only resolvable against this warehouse, the label is
        # portable and is what a human recognises in a bug report.
        # None when the load predates feed_info.txt, which is optional in the
        # GTFS spec and which Metro only began publishing on 2026-09-14.
        self.feed_version = feed_version
        self.stats = ReferenceStats()
        self._trips: dict[str, TripRef] = {}
        self._shapes: dict[str, ShapeRef] = {}
        self._schedules: dict[str, ScheduleRef] = {}
        self._hood_geoms: list = []
        self._hood_names: list[tuple[int, str]] = []
        self._hood_index = None  # shapely STRtree

    # ----------------------------------------------------------------

    def load(self, conn: psycopg.Connection) -> None:
        """Populate every index from PostGIS for self.version_id.

        Contract:

          * trips: join static.trips to static.routes on (version_id,
            route_id) so route_short_name/long_name/type come along. One
            query, not one per trip.

          * shapes: read static.shape_lines for this version. Convert the
            PostGIS geometry to a shapely LineString -- select
            ST_AsBinary(geom) and use shapely.wkb.loads, not ST_AsText and
            wkt; WKB round-trips 172k coordinates without the float
            formatting loss and is several times faster to parse.

          * schedules: read static.stop_times ordered by (trip_id,
            stop_sequence), keeping only rows where shape_dist_traveled IS
            NOT NULL, and group into one ScheduleRef per trip. Parse
            arrival_time (HH:MM:SS, possibly >= 24:00:00) into seconds with
            plain integer arithmetic on the split parts -- do NOT use
            datetime.strptime, which rejects hour 25 outright.

            Stream this with a server-side cursor. 1.1M rows fetched client
            side at once is the one place in this file where memory actually
            matters.

          * neighborhoods: read static.neighborhoods, build shapely geometries
            and an STRtree over them. 350 polygons.

          * Fill self.stats and log a one-line summary. A load that produces
            0 shapes or 0 schedules must be loud -- it means the version was
            marked ready without its dependent tables, which is the exact
            failure static/load.py's ordering exists to prevent.
        """
        with conn.cursor(row_factory=class_row(TripRef)) as cursor:
            cursor.execute(
                """
                SELECT t.trip_id, t.route_id, t.service_id, t.trip_headsign, t.direction_id,
                    t.block_id, t.shape_id,
                    r.route_short_name, r.route_long_name, r.route_type
                FROM static.trips t 
                JOIN static.routes r
                ON (r.version_id, r.route_id) = (t.version_id, t.route_id)
                WHERE t.version_id = %s
                """,
                (self.version_id,),
            )
            self._trips = {r.trip_id: r for r in cursor}

        with conn.cursor(row_factory=class_row(_ShapeRow)) as cursor:
            cursor.execute(
                """
                SELECT shape_id, max_dist, n_points, ST_AsBinary(geom) AS geom
                FROM static.shape_lines
                WHERE version_id = %s
                """,
                (self.version_id,),
            )
            for row in cursor:
                shape = ShapeRef(
                    shape_id=row.shape_id,
                    max_dist=row.max_dist,
                    n_points=row.n_points,
                    geom=shapely.wkb.loads(row.geom)
                )
                self._shapes[row.shape_id] = shape

        with conn.cursor(row_factory=class_row(_NeighborhoodRow)) as cursor:
            cursor.execute(
                """
                SELECT neigh_num, neighborhood_name, ST_AsBinary(geom) AS geom
                FROM static.neighborhoods
                """
            )
            for row in cursor:
                self._hood_geoms.append(shapely.wkb.loads(row.geom))
                self._hood_names.append((row.neigh_num, row.neighborhood_name))
            self._hood_index = shapely.STRtree(self._hood_geoms)

        with conn.cursor(name='ref_stop_times', row_factory=class_row(_StopTimeRow)) as cursor:
            cursor.itersize = 5_000
            cursor.execute(
                """
                SELECT trip_id, arrival_time, shape_dist_traveled
                FROM static.stop_times
                WHERE version_id = %s AND shape_dist_traveled IS NOT NULL
                ORDER BY trip_id, stop_sequence
                """,
                (self.version_id,),
            )
            for trip_id, rows in groupby(cursor, key=lambda r: r.trip_id):
                dists, seconds = [], []
                for row in rows:
                    dists.append(row.shape_dist_traveled)
                    seconds.append(_hms(row.arrival_time))
                self._schedules[trip_id] = ScheduleRef(trip_id=trip_id, dists=dists, seconds=seconds)

        self.stats.trips = len(self._trips)
        self.stats.shapes = len(self._shapes)
        self.stats.neighborhoods = len(self._hood_names)
        self.stats.schedules = len(self._schedules)
        log.info(
            "static loaded: %s trips, %s shapes, %s neighborhoods, %s schedules",
            self.stats.trips, self.stats.shapes, self.stats.neighborhoods,
            self.stats.schedules,
        )
        if self.stats.neighborhoods == 0 or self.stats.schedules == 0:
            log.error(
                "0 neighborhoods or 0 schedules loaded -- dependent tables are "
                "missing; a version in this state must never be enriched against"
            )


    def trip(self, trip_id: str) -> TripRef | None:
        """Resolve a realtime trip_id against static. Updates hit/miss stats.

        Returns None for an unknown trip_id, which the caller routes to the
        DLQ with UNKNOWN_TRIP_ID.

        THE MISS RATE MEASURES STALENESS, not feed noise. Against a static
        feed loaded the day it was published, 100.0% of live position
        trip_ids resolved; against one two weeks old, 97.4%. So there is no
        "acceptable baseline" to tolerate here -- a healthy pipeline sits
        near zero and climbs as a service change approaches, which makes this
        the cheapest alarm available for "the static load needs refreshing".
        """
        ref = self._trips.get(trip_id)
        if ref:
            self.stats.trip_hits += 1
            return ref
        else:
            self.stats.trip_misses += 1
            return None

    def locate_on_shape(self, shape_id: str, lon: float, lat: float) -> float | None:
        """Distance along the trip's shape for a position, in feed units.

        This is the linear referencing that makes schedule deviation possible,
        and it is the substantive spatial work in the project.

        shapely's LineString.project() returns distance along the line in the
        line's own coordinate units -- which here are DEGREES, because the
        geometry is WGS84. Degrees are not metres and are not the feed's
        distance units either, so the raw result is not comparable to
        stop_times.shape_dist_traveled.

        Normalise: project(normalized=True) gives 0..1 along the line, then
        multiply by ShapeRef.max_dist to land back in feed units. That makes
        the result directly comparable to shape_dist_traveled without ever
        needing a projected CRS.

        Returns None when the shape is unknown or max_dist is absent.

        Worth knowing: project() snaps to the nearest point on the line
        regardless of how far away it is. A vehicle 5 km off route still gets
        a confident answer. If that matters, measure the perpendicular
        distance too and decide a threshold -- a water taxi will not snap
        sensibly to a road-oriented shape, which the proposal flagged as a
        useful adversarial case.
        """
        point = shapely.geometry.Point(lon, lat)
        shape = self._shapes.get(shape_id)
        if shape is None:
            return None
        elif shape.max_dist is None:
            return None
        else:
            projection = shape.geom.project(point, normalized=True)
            return projection * shape.max_dist

    def neighborhood(self, lon: float, lat: float) -> tuple[int, str] | None:
        """Point-in-polygon against King County neighborhoods.

        Returns (neigh_num, name) or None. **None is NORMAL** -- 83.4% of the
        fleet falls inside a polygon and the rest is water taxi routes,
        out-of-county Sound Transit Express, and boundary gaps. Never route a
        null neighborhood to the DLQ.

        Use the STRtree to narrow, then test properly: STRtree.query() returns
        candidates whose BOUNDING BOXES intersect, not whose geometries do.
        Skipping the exact test would attribute a vehicle to whichever
        neighborhood has a big enough envelope, which is wrong in a way that
        looks plausible on a map.

        `covers` rather than `contains`, so a vehicle exactly on a boundary
        lands in one neighborhood instead of none.
        """
        point = shapely.geometry.Point(lon, lat)
        for index in self._hood_index.query(point, predicate='intersects'):
            if shapely.covers(self._hood_geoms[index], point):
                return self._hood_names[index]
        return None

    # ------------------------------------------------------------------------

    def summary(self) -> str:
        s = self.stats
        label = self.feed_version or "no feed_info"
        return (
            f"version {self.version_id} [{label}]: {s.trips:,} trips, {s.shapes} shapes, "
            f"{s.schedules:,} schedules, {s.neighborhoods} neighborhoods "
            f"| join rate {s.join_rate:.1%}"
        )


def resolve_version(conn: psycopg.Connection) -> tuple[int, str | None]:
    """The static version to pin to, and its agency label.

    Returns (version_id, feed_version). Delegates the "which version" policy
    to static.current_version() rather than reimplementing "newest ready", so
    it lives in one place (docker/initdb/02-static.sql).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT v.version_id, v.feed_version
            FROM static.feed_version v
            WHERE v.version_id = static.current_version()
            """
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError(
            "no ready static version -- run `make static-load` before enriching"
        )
    return row[0], row[1]
