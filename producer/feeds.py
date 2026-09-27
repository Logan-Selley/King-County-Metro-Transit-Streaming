"""The feed manifest: what to poll, how often, where it goes.

Declarative on purpose, and the same shape as ingest/manifest.py in the parcel
project: frozen dataclasses with derived properties, so that every consumer of
this config reads the same bytes and there is no second copy to drift.

Everything here is settled by Phase 0 measurement or an ADR. Nothing in this
file is a guess:

  poll intervals   docs/findings.md §2 -- measured 20.0s / 20.0s / 60.0s
                   publish periods, polled at half that (see below)
  wire format      ADR 0003 -- protobuf for positions and trip updates,
                   enhanced JSON for alerts
  partition key    ADR 0002 -- vehicle_id / trip_id / alert_id
  compaction       raw.service_alerts only

It is the reference for the shape the rest of the package follows.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

S3 = "https://s3.amazonaws.com/kcm-alerts-realtime-prod"

WireFormat = Literal["protobuf", "json"]


def _interval(name: str, default: int) -> int:
    """Read a poll interval from the environment, falling back to the measured
    default. Kept as a function rather than inlined so the CLI can report where
    a value came from."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name}={raw!r} is not an integer") from exc
    if value < 1:
        raise ValueError(f"{name}={value} must be >= 1")
    return value


@dataclass(frozen=True)
class FeedSpec:
    """One feed: where it comes from, how it is decoded, where it lands."""

    name: str
    url: str
    topic: str
    wire_format: WireFormat
    poll_interval_s: int

    # The FeedEntity oneof field carrying this feed's payload -- "vehicle",
    # "trip_update", or "alert". Only meaningful for protobuf feeds.
    entity_field: str

    # Documentation of the Kafka message key, per ADR 0002. The decoder is
    # responsible for producing it; this records the intent in one place so
    # the key cannot quietly diverge from the ADR.
    key_field: str

    # Log-compacted topics hold current state per key rather than a history.
    # Only raw.service_alerts. Affects nothing in the producer except that a
    # compacted topic MUST always be produced with a non-null key.
    compacted: bool = False

    # Bound on the dedupe cache for this feed, in distinct IDENTITIES (not
    # Kafka keys -- the two differ for trip updates; see ADR 0004).
    #
    # Sized against PEAK rather than the measured evening trough, because the
    # cache degrades as a cliff: once one poll's identities exceed this, that
    # poll evicts its own earliest entries and suppression drops straight to
    # 0%. See the header comment in dedupe.py for the measurement.
    dedupe_maxsize: int = 60_000
    # Where this feed's rejects go. None means `dlq.<name>`, the live
    # convention. Phase 6's replay sets `replay.dlq.<name>`, because a replay's
    # rejects landing in the LIVE dlq would be counted by transit_health's
    # dlq_report as if the live feed had produced them.
    dlq: str | None = None

    @property
    def dlq_topic(self) -> str:
        """The topic rejects are published to: the override, or dlq.<name>."""
        return self.dlq or f"dlq.{self.name}"

    @property
    def archive_prefix(self) -> str:
        """MinIO key prefix for this feed's raw payloads.

        Objects land at <prefix>/<YYYY>/<MM>/<DD>/<HH>/<epoch>-<etag>.<ext>.
        Hour-level partitioning because a day of positions is ~4,300 objects
        and flat prefixes make `mc ls` unusable well before that.
        """
        return f"raw/{self.name}"

    @property
    def extension(self) -> str:
        return "pb" if self.wire_format == "protobuf" else "json"


# The three feeds. Note that the basic JSON mirrors do NOT exist (they 403) --
# see docs/findings.md §3 -- so the alerts feed takes the _enhanced variant,
# which is also the one carrying last_modified_timestamp.
FEEDS: dict[str, FeedSpec] = {
    "vehicle_positions": FeedSpec(
        name="vehicle_positions",
        url=f"{S3}/vehiclepositions.pb",
        topic="raw.vehicle_positions",
        wire_format="protobuf",
        poll_interval_s=_interval("POLL_INTERVAL_VEHICLE_POSITIONS", 10),
        entity_field="vehicle",
        key_field="vehicle_id",
        # Identity is vehicle_id, so cardinality is the fleet: 280 at the
        # evening trough and 423 at the measured daytime peak -- the proposal's
        # "high hundreds to low thousands" was high. Occupancy over 24h peaked
        # at 835 (the day's distinct vehicles, not the concurrent fleet), so
        # 20k has ample headroom and costs a few MB.
        dedupe_maxsize=20_000,
    ),
    "trip_updates": FeedSpec(
        name="trip_updates",
        url=f"{S3}/tripupdates.pb",
        topic="raw.trip_updates",
        wire_format="protobuf",
        poll_interval_s=_interval("POLL_INTERVAL_TRIP_UPDATES", 10),
        entity_field="trip_update",
        # NOT vehicle_id: only ~46% of trip updates carry one, and the missing
        # ones are trips that have not started -- which is exactly the
        # long-lead-time data the prediction analysis needs. findings.md §5.
        key_field="trip_id",
        # Identity is (trip_id, stop_id, stop_sequence) -- ~30 per trip, so
        # cardinality is stop predictions, not trips: 18,623 at the evening
        # trough, ~27,800 at the measured daytime peak.
        #
        # 300k is ~10x that concurrent peak, and the 24-hour run showed why
        # the headroom is spent on something other than concurrency --
        # occupancy climbs monotonically with CUMULATIVE identities over a
        # service day (23k -> 300k, pinned at the ceiling from hour 21).
        # Eviction at that point is discarding trips that ended hours ago,
        # and suppression held at 78-85%. See dedupe.py's header.
        #
        # This is the number the old shared 60k default got wrong: fine all
        # night, thrashing from the morning peak onward.
        dedupe_maxsize=300_000,
    ),
    "service_alerts": FeedSpec(
        name="service_alerts",
        url=f"{S3}/alerts_enhanced.json",
        topic="raw.service_alerts",
        wire_format="json",
        poll_interval_s=_interval("POLL_INTERVAL_SERVICE_ALERTS", 30),
        entity_field="alert",
        key_field="alert_id",
        compacted=True,
        # 53-54 alerts observed. Three orders of magnitude of headroom for a
        # feed that would have to grow absurdly to matter.
        dedupe_maxsize=10_000,
    ),
}


def get(name: str) -> FeedSpec:
    if name not in FEEDS:
        raise KeyError(f"{name!r} is not a feed ({sorted(FEEDS)})")
    return FEEDS[name]


# Sent on every request. This is an unauthenticated public endpoint run by a
# transit agency; identifying the client and leaving a contact address is the
# difference between being a good citizen and being the reason they add a
# rate limit.
USER_AGENT = os.environ.get(
    "FEED_USER_AGENT",
    "transit-stream/0.1 (portfolio project; contact: archane24@gmail.com)",
)
