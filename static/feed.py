"""Static reference manifest: the GTFS zip and the spatial layers.

Declarative, same shape as producer/feeds.py. Everything here is measured
against the real artifacts rather than assumed, most recently against
FAL26-161.1 (2026-09-14).

--- why static data is versioned rather than overwritten ---

The GTFS zip changes on service-change dates, and realtime `trip_id`s
reference whichever version was current when the trip was scheduled. Blindly
replacing the tables mid-service-day would strand every in-flight trip: its
trip_id would stop resolving and the enrichment consumer would route a
perfectly good vehicle to the DLQ.

So each load is stamped with BOTH identifiers -- the HTTP ETag and the
agency's own `feed_version` from feed_info.txt -- and kept alongside its
predecessor. This is the "static/realtime join staleness" problem the proposal
calls the most realistic operational issue in the project.

--- staleness is measurable, and it is what the DLQ rate actually reports ---

Against the 2026-08-29 feed, 97.4% of live position trip_ids resolved. Against
FAL26-161.1 on the day it landed, **100.0%** resolved.

The 2.6% was never a property of the feed. It was the age of the static data:
trips scheduled under a version newer than the one loaded. So the DLQ rate on
UNKNOWN_TRIP_ID is not background noise with a baseline to tolerate, it is a
direct measure of how stale the static load has become. A healthy pipeline
sits near zero and climbs as a service change approaches.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

GTFS_ZIP = "https://metro.kingcounty.gov/GTFS/google_transit.zip"

# feed_info.txt carries the agency's OWN version label, and it is a better
# identifier than the ETag for everything except detecting a byte change.
# Observed 2026-09-14:
#
#   feed_version     FAL26-161.1        <- human readable, agency meaningful
#   feed_start_date  20260914
#   feed_end_date    20270326
#
# Carry BOTH, because they answer different questions:
#
#   ETag          byte identity. Changes on any republish, including a rebuild
#                 of an identical schedule. This is what makes conditional GET
#                 work and what says "there is new data to load".
#
#   feed_version  schedule identity. Stable across a rebuild, and the thing a
#                 human recognises in a bug report. "FAL26-161.1" means
#                 something to someone at Metro; "1895a7c83f44dd1:0" does not.
#
#   start/end     the validity WINDOW, which is what actually answers "should
#                 this in-flight trip resolve against the old version or the
#                 new one". Neither identifier alone tells you that.
FEED_INFO = "feed_info.txt"

# Declared in agency.txt by all four agencies in the feed (Metro Transit,
# City of Seattle, Sound Transit, Solid Ground EZ Loop). Constant rather than
# parsed because the enrichment consumer needs it and never opens the zip;
# static/load.py warns if agency.txt ever disagrees.
#
# THIS IS LOAD-BEARING FOR SCHEDULE DEVIATION. GTFS stop_times are offsets
# from the start of a SERVICE DATE in the AGENCY's timezone. Using UTC
# midnight as the origin is not an approximation, it is 7-8 hours wrong every
# single day -- which presents exactly as the "service date is wrong" symptom,
# at a magnitude that looks like a timezone bug because it is one.
AGENCY_TZ = "America/Los_Angeles"

# King County Neighborhood Areas ("Metro Neighborhoods in King County"),
# King County GIS, discovered via https://www5.kingcounty.gov/sdc?Layer=neighborhood_area
#
# Verified 2026-09-06: 350 features, 345 Polygon + 5 MultiPolygon, attributes
# NEIGH_NUM and NEIGHBORHOOD_NAME.
#
# An ArcGIS FeatureServer, the same shape of endpoint the parcel project's
# extractor already talks to. outSR=4326 asks the service to reproject: the
# layer is natively EPSG:2926 (Washington State Plane North, ftUS) and
# GTFS-RT is WGS84, so having the service do the transform avoids a local
# pyproj dependency for one static layer loaded on a rare schedule.
#
# 350 features against a maxRecordCount of 1000, so this is a single request
# with no paging. If the layer ever grows past 1000, this silently truncates:
# the loader asserts the returned count against the service's own
# returnCountOnly before accepting a load.
NEIGHBORHOODS_URL = (
    "https://services.arcgis.com/Ej0PsM5Aw677QF1W/arcgis/rest/services/"
    "NEIGHBORHOOD_AREA_384/FeatureServer/0"
)
# OBJECTID is requested because it is the layer's own stable feature id (the
# service reports it as objectIdField) and it is the primary key of
# static.neighborhoods. ArcGIS omits it from the response unless asked, so
# leaving it out means an INSERT with nothing for the PK.
NEIGHBORHOODS_QUERY = (
    "/query?where=1%3D1&outFields=OBJECTID,NEIGH_NUM,NEIGHBORHOOD_NAME"
    "&outSR=4326&f=geojson&resultRecordCount=1000"
)

# CHOSEN OVER the City of Seattle neighborhoods layer (ArcGIS Hub dataset
# b4a142f592e94d39a3bf787f3c112c1d_0, 94 polygons with an L_HOOD/S_HOOD
# hierarchy), on measurement rather than preference. Against one live
# positions poll of 368 vehicles:
#
#     King County layer   307 inside = 83.4%
#     Seattle layer       215 inside = 58.4%
#
# Metro serves the whole county, so a Seattle-only layer leaves 42% of the
# fleet unattributed and quietly turns any neighborhood mart into a Seattle
# mart. The county layer gives up Seattle's two-level hierarchy to get 25
# points of coverage, which is the right trade for a service-reliability
# question asked at the county level.
#
# The remaining ~17% outside any polygon is expected: water taxi routes over
# open water, Sound Transit Express running into Snohomish and Pierce, and
# genuine gaps between neighborhood boundaries.
#
# THIS IS WHY `neighborhood` IS NULLABLE AND A NULL IS NORMAL. A vehicle on
# the Vashon ferry is not a data quality problem and must never reach the DLQ
# for it.
NEIGHBORHOOD_COVERAGE = 0.834


@dataclass(frozen=True)
class GtfsTable:
    """One file inside the GTFS zip, and how it lands in Postgres."""

    filename: str
    table: str
    # Columns to keep, in load order. A subset on purpose: the feed publishes
    # columns this project has no use for, and naming them explicitly means a
    # new upstream column is inert rather than a schema change.
    columns: tuple[str, ...]
    # Approximate row count as of 2026-08-29, for sanity-checking a load.
    # An order-of-magnitude miss means a truncated download, not a busy day.
    expect_rows: int
    required: bool = True


# Row counts measured from FAL26-161.1 (2026-09-14, 10.7 MB zip, 18 members).
# They moved by 1-25% across the service change, which is why expect_rows is an
# ORDER-OF-MAGNITUDE sanity check and not an assertion.
GTFS_TABLES: tuple[GtfsTable, ...] = (
    GtfsTable(
        "routes.txt", "routes",
        ("route_id", "route_short_name", "route_long_name", "route_type",
         "route_color", "route_text_color"),
        expect_rows=142,
    ),
    GtfsTable(
        "trips.txt", "trips",
        # block_id is here, 100% populated across all 31,688 trips, and it
        # survived the 2026-09-14 service change that DELETED block_trip.txt.
        # That is what makes ADR 0003's deferral of block_id moot: it arrives
        # from the static join at no payload cost, rather than requiring the
        # enhanced JSON positions feed at 7.1x. See the revision note on 0003.
        ("route_id", "service_id", "trip_id", "trip_headsign", "direction_id",
         "block_id", "shape_id"),
        expect_rows=31_688,
    ),
    GtfsTable(
        "stops.txt", "stops",
        ("stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon",
         "location_type", "parent_station"),
        expect_rows=6_266,
    ),
    GtfsTable(
        # The big one: 65 MB of the 77 MB unpacked. Must be loaded with COPY.
        # Row-by-row INSERT of 1.1M rows is minutes instead of seconds, and
        # this runs on every service change.
        "stop_times.txt", "stop_times",
        ("trip_id", "arrival_time", "departure_time", "stop_id",
         "stop_sequence", "shape_dist_traveled", "timepoint"),
        expect_rows=1_101_970,
    ),
    GtfsTable(
        # 167,088 points across ~430 distinct shapes, so ~390 points each.
        # shape_dist_traveled is published, which means linear referencing
        # does not have to recompute cumulative distance from the geometry.
        "shapes.txt", "shapes",
        ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence",
         "shape_dist_traveled"),
        expect_rows=167_088,
    ),
    GtfsTable(
        "calendar.txt", "calendar",
        ("service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
         "saturday", "sunday", "start_date", "end_date"),
        expect_rows=22,
    ),
    GtfsTable(
        "calendar_dates.txt", "calendar_dates",
        ("service_id", "date", "exception_type"),
        expect_rows=1_149,
    ),
)

# Published but not loaded. Named so each omission is a decision rather than
# an oversight.
#
# The fares files are GTFS-Fares v2 and the networks files are route grouping;
# this project analyses service reliability, not fare products. agency.txt is
# one row of contact details.
UNUSED_FILES = (
    "agency.txt",
    "fare_attributes.txt", "fare_rules.txt", "fare_leg_rules.txt",
    "fare_media.txt", "fare_products.txt", "fare_transfer_rules.txt",
    "rider_categories.txt",
    "networks.txt", "route_networks.txt",
)

# --- A SERVICE CHANGE, 2026-09-14 ---
#
# The feed in place before the change (2026-08-29, ETag 682c87c4c237dd1:0,
# 12 members) was replaced by FAL26-161.1 (2026-09-14, ETag 1895a7c83f44dd1:0,
# 18 members). The structure changed, not just the data:
#
#   ADDED    feed_info.txt, networks.txt, route_networks.txt, and five
#            GTFS-Fares v2 files
#   REMOVED  block.txt and block_trip.txt, the two non-standard Metro
#            extensions
#
# Two properties of the loader this confirms:
#
#   1. Pinning a loader to an exact file list is fragile. The manifest names
#      what it NEEDS and ignores the rest, so six new files and two removed
#      ones changed nothing about whether this loads.
#
#   2. block_id survived in trips.txt even though block_trip.txt did not,
#      which is why ADR 0003's revision holds. Had the enrichment been built
#      on block_trip.txt, it would have broken on a Sunday.
SERVICE_CHANGE_NOTE = "FAL26-161.1 replaced the 2026-08-29 feed on 2026-09-14"


@dataclass(frozen=True)
class FeedInfo:
    """The agency's own identity for a feed, from feed_info.txt.

    Exactly one data row, parsed by parse_feed_info below.
    """

    feed_version: str | None          # "FAL26-161.1"
    feed_start_date: str | None       # "20260914"
    feed_end_date: str | None         # "20270326"
    publisher_name: str | None = None

    def covers(self, yyyymmdd: str) -> bool | None:
        """Whether a service date falls in this feed's validity window.

        Returns None when the window is not published -- absent bounds mean
        "unbounded", and treating that as False would reject every trip.
        This is the question a version table cannot answer from an ETag, and
        it is what decides which version an in-flight trip resolves against.
        """
        if not (self.feed_start_date and self.feed_end_date):
            return None
        return self.feed_start_date <= yyyymmdd <= self.feed_end_date


def parse_feed_info(rows) -> FeedInfo:
    """First (and only) row of feed_info.txt -> FeedInfo.

    Tolerant of absence: feed_info.txt is OPTIONAL in the GTFS spec, and this
    agency only began publishing it at the 2026-09-14 service change. A loader
    that requires it would have failed against every earlier feed, and will
    fail again if it is ever dropped. Missing means unknown, not invalid.
    """
    row = next(iter(rows), None)
    if not row:
        return FeedInfo(None, None, None)
    return FeedInfo(
        feed_version=(row.get("feed_version") or "").strip() or None,
        feed_start_date=(row.get("feed_start_date") or "").strip() or None,
        feed_end_date=(row.get("feed_end_date") or "").strip() or None,
        publisher_name=(row.get("feed_publisher_name") or "").strip() or None,
    )


@dataclass(frozen=True)
class StaticSource:
    gtfs_zip: str = GTFS_ZIP
    neighborhoods_url: str = NEIGHBORHOODS_URL
    tables: tuple[GtfsTable, ...] = GTFS_TABLES
    unused: tuple[str, ...] = field(default_factory=lambda: UNUSED_FILES)

    @property
    def neighborhoods_geojson(self) -> str:
        """Full query URL returning all 350 polygons as GeoJSON in EPSG:4326."""
        return self.neighborhoods_url + NEIGHBORHOODS_QUERY

    @property
    def neighborhoods_count(self) -> str:
        """Authoritative feature count, for asserting the load was not truncated."""
        return self.neighborhoods_url + "/query?where=1%3D1&returnCountOnly=true&f=json"

    @property
    def user_agent(self) -> str:
        return os.environ.get(
            "FEED_USER_AGENT",
            "transit-stream/0.1 (portfolio project; contact: archane24@gmail.com)",
        )


SOURCE = StaticSource()

# Measured join rates against a live poll (2026-09-06), the baseline the DLQ
# rate is judged against:
#
#     against the 2026-08-29 feed, two weeks stale   97.4%
#     against FAL26-161.1 on the day it landed      100.0%
#
# 2.6% is not a normal-operation baseline. The miss rate measures the AGE OF
# THE STATIC LOAD -- those were trips scheduled under a version newer than the
# one held. A live enrichment run against a same-day load logged a 100.0% join
# rate over 1,204 records with zero DLQ.
#
# So there is no baseline to tolerate. Near zero is healthy; a climb means the
# static load is ageing and a service change is approaching. Treating 2.6% as
# expected would silently absorb exactly the signal this is for.
EXPECTED_TRIP_JOIN_RATE = 1.0
