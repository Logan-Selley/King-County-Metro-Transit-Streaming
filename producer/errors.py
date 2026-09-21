"""Exception types and DLQ reason codes.

Small on purpose. It exists so that `reason` strings written to dlq.* and to
raw.dlq are a closed set rather than ad-hoc prose -- the Phase 4 Airflow task
reports DLQ volume *by reason*, and that report is worthless if the same
failure is spelled three ways.
"""

from __future__ import annotations

from enum import StrEnum


class FeedError(Exception):
    """Anything that went wrong talking to the King County endpoints.

    One feed failing must never stop the others -- run.py catches this per
    feed, logs it, and continues to the next tick. Same pattern as the parcel
    extractor's per-county isolation.
    """


class DecodeError(Exception):
    """A payload was fetched but could not be turned into records."""


class DlqReason(StrEnum):
    """Closed set of reasons a record is routed to the DLQ instead of a topic.

    Add to this rather than inventing a string at the call site.
    """

    # The bytes are not a parseable FeedMessage / not valid JSON.
    MALFORMED_PAYLOAD = "malformed_payload"

    # Parsed, but a field the schema requires is absent. Distinct from
    # MALFORMED_PAYLOAD because it means the feed changed shape, not that it
    # was corrupted in transit.
    MISSING_REQUIRED_FIELD = "missing_required_field"

    # Latitude/longitude outside the King County bounding box. Usually a
    # (0, 0) null-island default from a vehicle with no GPS fix.
    POSITION_OUT_OF_BOUNDS = "position_out_of_bounds"

    # trip_id absent from the current static GTFS feed. Very common right
    # after a service change; a first-class data quality signal, not noise.
    UNKNOWN_TRIP_ID = "unknown_trip_id"

    # Timestamp implausible -- far future, or before the service date.
    IMPLAUSIBLE_TIMESTAMP = "implausible_timestamp"

    # A computed schedule deviation outside MAX_PLAUSIBLE_DEVIATION_S. NOT a
    # record-rejection reason -- the position, neighborhood and shape distance
    # on that record are all still good, so only the deviation is nulled and
    # counted. Listed here so the counter has a name from the same closed set
    # as everything else the pipeline reports on.
    IMPLAUSIBLE_DEVIATION = "implausible_deviation"


# Rough King County envelope in WGS84, generous enough not to reject a real
# vehicle and tight enough to catch null island and transposed coordinates.
# Transposition is the failure this actually catches: (-122.3, 47.6) swapped
# lands in the Indian Ocean but is a perfectly valid coordinate pair.
KING_COUNTY_BBOX = (-122.60, 47.10, -121.05, 47.85)  # (min_lon, min_lat, max_lon, max_lat)

# Schedule deviations beyond this are not late buses, they are bugs.
#
# Same role as the bbox above, one layer up: a VALUE-DOMAIN bound on a
# computed field, checked because the schema gate structurally cannot. ADR
# 0005 measured what the Schema Registry lets through -- `int32 -> uint32` is
# wire-compatible and turns every early bus into ~4.29e9 -- and this is the
# guard that catches it.
#
# 3 hours, against a measured live distribution of -2,069s to +1,905s (34 min
# early to 32 min late) over 1,204 enriched records. That is ~5x the observed
# extreme, so a genuinely catastrophic delay still passes while every known
# failure mode is well outside:
#
#     ~25,200 s   UTC-midnight origin instead of agency-local (7-8 h)
#     ~86,400 s   anchored on the observation's calendar date, not the
#                 trip's service date -- fires on the 4.5% of trips that
#                 run past midnight
#   ~4.29e9 s     int32 -> uint32 signedness change, wrapping negatives
#
# The schedule_deviation docstring names all three in prose. This is the
# number that makes them detectable instead of merely documented.
MAX_PLAUSIBLE_DEVIATION_S = 3 * 3600
