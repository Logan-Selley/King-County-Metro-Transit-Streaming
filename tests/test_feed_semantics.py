"""Regression tests for the GTFS-RT wire semantics the pipeline depends on.

These are not tests of this project's code -- there is barely any yet. They
assert the *findings* from Phase 0, against captured fixtures, so that the
assumptions the schema was designed around are checked rather than remembered.

Every one of these corresponds to a decision in docs/findings.md or
docker/initdb/01-schema.sql. If one fails, a design decision needs revisiting,
which is exactly the signal worth having in CI.

They need no running stack, no network, and no credentials, so they are the
part of the test suite that CI can honestly run (see .github/workflows/ci.yml).
"""

from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2 as rt

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> rt.FeedMessage:
    msg = rt.FeedMessage()
    msg.ParseFromString((FIXTURES / name).read_bytes())
    return msg


@pytest.fixture(scope="module")
def positions() -> rt.FeedMessage:
    return load("vehicle_positions.pb")


@pytest.fixture(scope="module")
def trip_updates() -> rt.FeedMessage:
    return load("trip_updates.pb")


@pytest.fixture(scope="module")
def alerts() -> rt.FeedMessage:
    return load("service_alerts.pb")


# --- the dedup premise -------------------------------------------------------


@pytest.mark.parametrize(
    "fixture", ["vehicle_positions.pb", "trip_updates.pb", "service_alerts.pb"]
)
def test_feeds_are_full_snapshots(fixture):
    """Every feed is FULL_DATASET, not DIFFERENTIAL.

    The entire dedup design rests on this: each poll restates every active
    entity whether or not it changed, so the producer must dedupe against
    (vehicle_id, position_timestamp) rather than treating each poll as new
    events. If a feed ever switched to DIFFERENTIAL, deduping would start
    silently discarding real updates.
    """
    msg = load(fixture)
    assert msg.header.incrementality == rt.FeedHeader.FULL_DATASET


def test_position_key_is_unique_within_a_poll(positions):
    """(vehicle_id, timestamp) is a usable primary key.

    This is the idempotency key in raw.vehicle_positions and the justification
    for at-least-once delivery over exactly-once. If a single poll ever
    contained two records for one vehicle at one timestamp, the ON CONFLICT DO
    NOTHING write would drop real data instead of a duplicate.
    """
    keys = [(e.vehicle.vehicle.id, e.vehicle.timestamp) for e in positions.entity]
    assert len(keys) == len(set(keys))


# --- the proto2 default trap -------------------------------------------------


def test_current_status_default_is_in_transit_to(positions):
    """An ABSENT current_status means IN_TRANSIT_TO, not unknown.

    gtfs-realtime.proto declares `current_status` with
    `[default = IN_TRANSIT_TO]`. Metro omits the field for the majority of
    entities, so a decoder that tests presence -- HasField, or iterating
    ListFields -- and maps absence to NULL discards the status of most of the
    fleet. Reading through the attribute accessor applies the default.

    This is the single easiest way to silently lose 70%+ of a column, which is
    why it is asserted rather than commented.
    """
    field = rt.VehiclePosition.DESCRIPTOR.fields_by_name["current_status"]
    assert field.has_default_value
    assert field.default_value == rt.VehiclePosition.IN_TRANSIT_TO

    absent = [e for e in positions.entity if not e.vehicle.HasField("current_status")]
    assert absent, "fixture no longer exercises the absent-field path"

    # The accessor must resolve absent -> IN_TRANSIT_TO for every one of them.
    assert all(
        e.vehicle.current_status == rt.VehiclePosition.IN_TRANSIT_TO for e in absent
    )

    # And absence must be the common case, not an edge case, or the finding
    # that motivated the NOT NULL DEFAULT in the schema no longer holds.
    assert len(absent) / len(positions.entity) > 0.5


# --- fields that look usable and are not --------------------------------------


def test_bearing_and_speed_are_effectively_unpopulated(positions):
    """Heading and speed must be derived, never read.

    Both fields exist in the spec and in the wire format, which makes them
    look available. They are populated on roughly 2% of entities. Any
    enrichment needing heading or speed has to compute it from consecutive
    positions for the same vehicle -- which is also why per-vehicle ordering
    (and therefore the vehicle_id partition key) is not negotiable.
    """
    n = len(positions.entity)
    bearing = sum(1 for e in positions.entity if e.vehicle.position.HasField("bearing"))
    speed = sum(1 for e in positions.entity if e.vehicle.position.HasField("speed"))

    assert bearing / n < 0.10, f"bearing now {bearing / n:.1%} — revisit ADR 0003"
    assert speed / n < 0.10, f"speed now {speed / n:.1%} — revisit ADR 0003"


def test_block_id_is_absent_from_the_protobuf(positions):
    """block_id is an enhanced-JSON-only field.

    It is 100% populated in the enhanced JSON and carries real meaning (the
    vehicle's day of work, linking consecutive trips), but it is not in the
    basic protobuf at all -- not even as an unset field, because the standard
    VehiclePosition message has no such field to unset. Anything wanting it
    has to take the enhanced JSON's 7x payload cost. See ADR 0003.
    """
    assert "block_id" not in rt.VehiclePosition.DESCRIPTOR.fields_by_name


# --- the two-stream join premise ---------------------------------------------


def test_trip_updates_vehicle_id_is_partial_and_meaningful(trip_updates):
    """A trip update carries a vehicle only once the trip is under way.

    ~46% of trip updates have vehicle.id set. That is not missing data: the
    remainder are scheduled trips that have not started. It matters because
    the prediction-accuracy join (proposal §6.6) cannot key on vehicle_id --
    it would silently drop every long-lead-time prediction, which is precisely
    the interesting half of the analysis. The join keys on trip_id.
    """
    n = len(trip_updates.entity)
    with_vehicle = sum(1 for e in trip_updates.entity if e.trip_update.vehicle.id)

    assert 0 < with_vehicle < n, "expected a mix of started and scheduled trips"

    # trip_id, by contrast, must be universal -- it is the join key.
    assert all(e.trip_update.trip.trip_id for e in trip_updates.entity)


def test_trip_updates_carry_many_stops_per_trip(trip_updates):
    """Row count scales with stops x trips, not trips.

    This is the volume finding that sets raw.trip_updates' retention and
    partition count. A naive "one row per entity" assumption underestimates
    this table by more than an order of magnitude.
    """
    per_trip = [len(e.trip_update.stop_time_update) for e in trip_updates.entity]
    assert sum(per_trip) / len(per_trip) > 10


# --- compaction premise -------------------------------------------------------


def test_alert_ids_are_stable_keys(alerts):
    """Alerts have unique ids, which is what makes the topic compactable.

    raw.service_alerts is log-compacted and keyed by alert_id so it holds
    current state rather than a history of every poll restating the same
    alerts. Compaction retains the last record per key, so duplicate ids
    within one poll would make which record survives arbitrary.
    """
    ids = [e.id for e in alerts.entity]
    assert all(ids)
    assert len(ids) == len(set(ids))
