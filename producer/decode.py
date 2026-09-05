"""Payload bytes -> records.

The three decoders turn raw feed payloads into the frozen record types
defined here, field-for-field with the warehouse tables in
docker/initdb/01-schema.sql. These records flow through publish.py into
Kafka and land via the Connect sink, so a rename here is a schema change,
not a refactor. Two wire formats, per ADR 0003: protobuf for positions and
trip updates, enhanced JSON for alerts.

--- the proto2 default trap ---

Read `current_status` through the ATTRIBUTE ACCESSOR, never through a presence
check.

gtfs-realtime.proto declares it `optional VehicleStopStatus current_status = 4
[default = IN_TRANSIT_TO]`. Under proto2 presence semantics an unset field is
not on the wire at all, so `HasField("current_status")` is False for 204 of
280 vehicles (findings.md §4) -- but the *declared default* means those
vehicles are IN_TRANSIT_TO, not unknown. Writing NULL there throws away the
status of nearly three quarters of the fleet.

    entity.vehicle.current_status            # correct -- applies the default
    entity.vehicle.HasField("current_status")  # a presence test, NOT a value

Where presence checking IS correct: bearing and speed. Those have no useful
default and are populated on ~2% of entities, so absent genuinely means
unknown and NULL is right. The same logic applies to every scalar where 0 is
a legal value (direction_id, current_stop_sequence): absence and zero are
different facts, and only a presence check can tell them apart.

Empty protobuf strings are normalised to None on the way through -- "" in a
nullable column breaks `is null` predicates downstream.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone

from google.transit import gtfs_realtime_pb2 as rt

from producer.errors import KING_COUNTY_BBOX, DecodeError
from producer.feeds import FeedSpec

log = logging.getLogger("producer.decode")


# --- record types -------------------------------------------------------------
# Field-for-field with the warehouse tables. Keep them in step: these flow
# through publish.py into Kafka and land via Connect, so a rename here is a
# schema change, not a refactor.


@dataclass(frozen=True)
class VehiclePositionRecord:
    vehicle_id: str
    position_timestamp: datetime

    trip_id: str | None
    route_id: str | None
    direction_id: int | None
    start_date: date | None

    latitude: float
    longitude: float
    bearing: float | None
    speed: float | None

    current_status: str          # never None -- see the module docstring
    current_stop_sequence: int | None
    stop_id: str | None
    occupancy_status: str | None
    block_id: str | None         # enhanced-JSON only; None from protobuf (ADR 0003)

    feed_etag: str | None = None

    @property
    def key(self) -> str:
        """Kafka message key. ADR 0002 -- per-vehicle ordering."""
        return self.vehicle_id

    @property
    def fingerprint(self) -> tuple:
        """Value identity for dedupe. A vehicle whose GPS timestamp has not
        advanced has not reported anything new."""
        return (self.vehicle_id, self.position_timestamp)

    @property
    def dedupe_key(self) -> str:
        """Dedupe identity -- coincides with the Kafka key for this feed."""
        return self.vehicle_id


@dataclass(frozen=True)
class TripUpdateRecord:
    trip_id: str
    stop_id: str
    stop_sequence: int

    route_id: str | None
    direction_id: int | None
    start_date: date | None

    vehicle_id: str | None       # ~46% populated; absence means "not started"
    trip_timestamp: datetime | None

    arrival_time: datetime | None
    arrival_delay: int | None
    departure_time: datetime | None
    departure_delay: int | None
    schedule_relationship: str | None

    feed_etag: str | None = None

    @property
    def key(self) -> str:
        """ADR 0002 -- trip_id, NOT vehicle_id. See findings.md §5."""
        return self.trip_id

    @property
    def fingerprint(self) -> tuple:
        """Value identity for dedupe.

        Includes the PREDICTED TIMES, not just the stop. ~70% of each poll is
        an unchanged restatement (findings.md §6) and suppressing those is
        free. Suppressing *changed* predictions for the same stop would delete
        the prediction-accuracy analysis -- see ADR 0004.
        """
        return (
            self.trip_id,
            self.stop_id,
            self.stop_sequence,
            self.arrival_time,
            self.departure_time,
        )

    @property
    def dedupe_key(self) -> str:
        """Dedupe identity: the Kafka key is the TRIP, but dedupe is per stop --
        one trip carries ~30 predictions that change independently (ADR 0004)."""
        return f"{self.trip_id}:{self.stop_id}:{self.stop_sequence}"


@dataclass(frozen=True)
class ServiceAlertRecord:
    alert_id: str
    cause: str | None
    effect: str | None
    severity_level: str | None
    header_text: str | None
    description_text: str | None
    url: str | None
    active_period_start: datetime | None
    active_period_end: datetime | None
    informed_entities: list[dict]
    last_modified: datetime | None   # enhanced JSON only; the compaction ordering signal

    feed_etag: str | None = None

    @property
    def key(self) -> str:
        """Compacted topic -- the key IS the identity. Must never be None."""
        return self.alert_id

    @property
    def fingerprint(self) -> tuple:
        return (self.alert_id, self.last_modified)

    @property
    def dedupe_key(self) -> str:
        """Dedupe identity -- coincides with the Kafka key for this feed."""
        return self.alert_id


Record = VehiclePositionRecord | TripUpdateRecord | ServiceAlertRecord


# --- helpers ------------------------------------------------------------------


def epoch_to_dt(value: int | None) -> datetime | None:
    """GTFS-RT timestamps are seconds since epoch, UTC. 0 means unset, not 1970."""
    if not value:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc)


def gtfs_date(value: str | None) -> date | None:
    """start_date arrives as YYYYMMDD."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return None


def parse_feed_message(payload: bytes) -> rt.FeedMessage:
    """Parse protobuf bytes, converting failure into DecodeError."""
    message = rt.FeedMessage()
    try:
        message.ParseFromString(payload)
    except Exception as exc:  # protobuf raises several unrelated types
        raise DecodeError(f"not a parseable FeedMessage: {exc}") from exc
    return message


# --- decoders -----------------------------------------------------------------


def decode_vehicle_positions(
    payload: bytes,
    etag: str | None = None,
    quarantine: list[VehiclePositionRecord] | None = None,
) -> list[VehiclePositionRecord]:
    """Decode the vehicle positions protobuf into one record per vehicle.

    Entities without a `vehicle` payload (FeedEntity is a oneof) are skipped
    and counted. Positions outside KING_COUNTY_BBOX are appended to
    `quarantine` when the caller passes one -- run.py routes them to the DLQ
    as POSITION_OUT_OF_BOUNDS -- or dropped with a logged count otherwise.
    They are usually (0, 0), a vehicle with no GPS fix; a transposed
    coordinate pair is the other catch, because a swapped pair never lands
    inside the county box.

    position_timestamp prefers vehicle.timestamp (99.6% populated) and falls
    back to the feed header timestamp. The fallback is chosen over dropping
    the record because the warehouse primary key needs a timestamp either
    way and "vehicle observed at feed-publish time" is a true statement; it
    affected 1 of 280 vehicles in the captured fixture.
    """
    vehicle_records: list[VehiclePositionRecord] = []
    min_lon, min_lat, max_lon, max_lat = KING_COUNTY_BBOX
    skipped = 0
    out_of_bounds = 0
    feed_message = parse_feed_message(payload)

    for entity in feed_message.entity:
        if "vehicle" not in entity:
            skipped += 1
            continue

        vehicle = entity.vehicle
        lat = vehicle.position.latitude
        lon = vehicle.position.longitude

        # Scalars where 0 is a legal value (direction_id, current_stop_sequence)
        # need a presence check: absence and zero are different facts. Empty
        # strings become None -- "" in a nullable column breaks `is null`
        # predicates downstream.
        record = VehiclePositionRecord(
            vehicle_id=vehicle.vehicle.id,
            position_timestamp=(
                epoch_to_dt(vehicle.timestamp)
                or epoch_to_dt(feed_message.header.timestamp)
            ),
            trip_id=vehicle.trip.trip_id or None,
            route_id=vehicle.trip.route_id or None,
            direction_id=vehicle.trip.direction_id if "direction_id" in vehicle.trip else None,
            start_date=gtfs_date(vehicle.trip.start_date),
            latitude=lat,
            longitude=lon,
            bearing=vehicle.position.bearing if "bearing" in vehicle.position else None,
            speed=vehicle.position.speed if "speed" in vehicle.position else None,
            # Read through the accessor so the proto2 declared default applies:
            # absent means IN_TRANSIT_TO, not unknown (see module docstring).
            current_status=rt.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status),
            current_stop_sequence=(
                vehicle.current_stop_sequence if "current_stop_sequence" in vehicle else None
            ),
            stop_id=vehicle.stop_id or None,
            occupancy_status=(
                rt.VehiclePosition.OccupancyStatus.Name(vehicle.occupancy_status)
                if "occupancy_status" in vehicle
                else None
            ),
            # Does not exist in the basic protobuf (ADR 0003). The column exists
            # so carrying it later is an ingest change rather than a migration.
            block_id=None,
            feed_etag=etag,
        )

        if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
            out_of_bounds += 1
            if quarantine is not None:
                # The full record, not just coordinates: the DLQ entry should
                # carry the whole observation so a fix can be tested against
                # what the feed actually said.
                quarantine.append(record)
            continue

        vehicle_records.append(record)

    if out_of_bounds:
        log.warning(
            "%d position(s) outside the King County bbox (%s)",
            out_of_bounds,
            "quarantined for DLQ" if quarantine is not None else "dropped",
        )
    if skipped:
        log.warning("%d entity/entities carried no vehicle payload; skipped", skipped)

    return vehicle_records


def decode_trip_updates(
    payload: bytes,
    etag: str | None = None,
    quarantine: list | None = None,
) -> list[TripUpdateRecord]:
    """Decode the trip updates protobuf into one record per stop prediction.

    This is a NESTED fan-out, and it is the shape trap of the three feeds:
    one entity is a trip, each trip carries ~30 stop_time_updates (median
    28, max 88), and each of those becomes one record. 612 trips produce
    ~18,600 records -- one record per entity would be wrong by a factor of
    30.

    `quarantine` is accepted for dispatch uniformity; trip updates have no
    per-record reject condition today (out-of-bounds applies to positions
    only).
    """
    trip_records: list[TripUpdateRecord] = []
    feed_message = parse_feed_message(payload)

    for entity in feed_message.entity:
        if "trip_update" not in entity:
            continue
        trip_update = entity.trip_update
        trip = trip_update.trip
        # "" means the trip has not started -- only ~46% carry a vehicle.
        # These records are kept, not filtered: they carry the long-lead-time
        # predictions the accuracy analysis needs (findings.md §5).
        vehicle_id = trip_update.vehicle.id or None

        for stop in trip_update.stop_time_update:
            trip_records.append(
                TripUpdateRecord(
                    # trip_id is both the join key and the Kafka key; asserted
                    # universal in tests/test_feed_semantics.py.
                    trip_id=trip.trip_id,
                    # Primary-key component and typed non-null, so the wire
                    # value stands as-is even when empty (0 occurrences in the
                    # fixture) -- None would violate the record type and the
                    # warehouse primary key.
                    stop_id=stop.stop_id,
                    stop_sequence=stop.stop_sequence,
                    route_id=trip.route_id or None,
                    direction_id=trip.direction_id if "direction_id" in trip else None,
                    start_date=gtfs_date(trip.start_date),
                    vehicle_id=vehicle_id,
                    trip_timestamp=epoch_to_dt(trip_update.timestamp),
                    arrival_time=epoch_to_dt(stop.arrival.time) if "arrival" in stop else None,
                    arrival_delay=(
                        stop.arrival.delay if "arrival" in stop and "delay" in stop.arrival else None
                    ),
                    departure_time=(
                        epoch_to_dt(stop.departure.time) if "departure" in stop else None
                    ),
                    departure_delay=(
                        stop.departure.delay
                        if "departure" in stop and "delay" in stop.departure
                        else None
                    ),
                    # Genuine absence preserved, NOT the declared SCHEDULED
                    # default: unlike current_status, this field is only 30.7%
                    # populated and the default carries no information. Absence
                    # means "no override reported for this stop", which
                    # downstream analysis distinguishes from an explicit
                    # SCHEDULED.
                    schedule_relationship=(
                        rt.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                            stop.schedule_relationship
                        )
                        if "schedule_relationship" in stop
                        else None
                    ),
                    feed_etag=etag,
                )
            )
    return trip_records

def english_text(field: dict | None) -> str | None:
    """Pick the English text from a GTFS translations wrapper.

    Shape: {"translation": [{"text": ..., "language": "en"}, ...]}.
    Language tags are BCP47 ("en", "en-US"), so compare the primary subtag.
    Falls back to the first entry rather than None -- an alert with no
    header is useless downstream. Metro currently publishes exactly one
    English entry per wrapper (321/321 in the captured fixture), so this
    is future-proofing, not a present necessity.
    """
    if not field:
        return None
    translations = field.get("translation") or []
    for entry in translations:
        lang = entry.get("language") or ""
        if lang.lower().split("-")[0] == "en":
            return entry.get("text")
    return translations[0].get("text") if translations else None

def decode_service_alerts(
    payload: bytes,
    etag: str | None = None,
    quarantine: list | None = None,
) -> list[ServiceAlertRecord]:
    """Decode the ENHANCED JSON alerts feed -- this one is JSON, not protobuf
    (ADR 0003). Use json.loads, not parse_feed_message.

    The structure mirrors GTFS-RT in its JSON representation:
    {"entity": [{"id": ..., "alert": {...}}, ...]}. alert_id comes from
    entity["id"] and must be non-empty -- the topic is log-compacted, so the
    key IS the identity and a null key is unroutable. Translated strings
    arrive as {"translation": [...]} and go through english_text();
    active_period is a list of which only the first range is kept (the trade
    is commented inline); last_modified_timestamp is the field that
    justified taking the JSON at all, being the ordering signal a compacted
    topic needs. Entities without an id are skipped. `informed_entity`
    stays a list of plain dicts -- it lands in a jsonb column.

    `quarantine` is accepted for dispatch uniformity; alerts have no
    per-record reject condition today.
    """
    alert_records = []
    feed_message = json.loads(payload)
    for entity in feed_message["entity"]:
        if entity.get('id'):
            # GTFS allows multiple active periods; we keep only the first. The fixture
            # has 0/54 alerts with more than one, and the column pair only holds one
            # range -- a multi-period alert would need schema work to represent fully.
            alert = entity.get("alert", {})
            periods = alert.get("active_period") or []
            first = periods[0] if periods else {}
            record = ServiceAlertRecord(
                alert_id = entity.get('id'),
                cause = alert.get("cause"),
                effect = alert.get("effect"),
                severity_level = alert.get("severity_level"),
                header_text = english_text(alert.get('header_text')),
                description_text = english_text(alert.get('description_text')),
                url = english_text(alert.get('url')),
                active_period_start = epoch_to_dt(first.get('start')),
                active_period_end = epoch_to_dt(first.get('end')),
                informed_entities = alert.get("informed_entity", []),
                last_modified = epoch_to_dt(alert.get('last_modified_timestamp')),
                feed_etag = etag
            )
            alert_records.append(record)
    return alert_records


# --- dispatch -----------------------------------------------------------------

DECODERS = {
    "vehicle_positions": decode_vehicle_positions,
    "trip_updates": decode_trip_updates,
    "service_alerts": decode_service_alerts,
}


def decode(
    spec: FeedSpec,
    payload: bytes,
    etag: str | None = None,
    quarantine: list | None = None,
) -> list[Record]:
    """Decode a payload according to its feed. Raises DecodeError.

    `quarantine`, when provided, collects per-record rejects (currently
    out-of-bounds vehicle positions) as full records instead of dropping
    them, so the caller can route them to the DLQ with a reason.
    """
    try:
        decoder = DECODERS[spec.name]
    except KeyError as exc:
        raise DecodeError(f"no decoder registered for {spec.name!r}") from exc
    return decoder(payload, etag, quarantine=quarantine)
