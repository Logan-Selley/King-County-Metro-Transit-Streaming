"""Bunching detection parameters, and what each one is measured against.

Declarative, same shape as producer/feeds.py and static/feed.py.

This is the file to change when the detector emits nonsense -- every number
here is a threshold, and thresholds are where a detector is right or wrong.

--- what bunching is, and why it is measured in DISTANCE not stops ---

The proposal defines it as "two vehicles on the same route within N seconds of
each other at the same stop". Detecting it that way needs stop ARRIVAL events,
and those are sparse: only ~27% of position records carry STOPPED_AT.

`enriched.vehicle_positions` carries `shape_dist_traveled` on 100% of records
(measured), which is each vehicle's distance along its route geometry. Two
vehicles on the same (route, direction) whose shape distances are close are
physically close, continuously, without waiting for either to reach a stop.

So the detector keys on (route_id, direction_id) and looks at gaps in
shape_dist_traveled between consecutive vehicles. That is strictly more data
than stop-based detection and it is available every 20 seconds per vehicle.

CORRECTED 2026-09-20. An earlier version of this file claimed "a route's
vehicles share a shape, so it stays on the safe side." Measured: 101 of 280
(route, direction) pairs carry more than one shape, covering 44.4% of trips,
and 92 shape pairs start over 500 m apart. The key is unchanged and the gap
comparison is now confirmed against straight-line distance before it can
alert -- see ADR 0007 and consumers/bunching/detect.py.
"""

from __future__ import annotations

from dataclasses import dataclass

SOURCE_TOPIC = "enriched.vehicle_positions"
SINK_TOPIC = "alerts.bunching"
CONSUMER_GROUP = "bunching"


@dataclass(frozen=True)
class BunchingConfig:
    """Thresholds for the detector."""

    # Vehicles closer together than this along the route are candidates.
    #
    # UNITS ARE FEET, and that is a measured fact rather than an assumption:
    # ST_Length(geom::geography) / max_dist came out at 0.3048 across 423 of
    # 424 shapes, which is the foot-to-metre constant falling out of the data.
    # (One shape publishes metres; see the note below.)
    #
    # 1,000 ft is roughly 2-3 city blocks. Two buses on the same route that
    # close are visibly bunched to a rider at a stop between them.
    gap_threshold_ft: float = 1_000.0

    # Ignore pairs where either vehicle's position is older than this. A
    # vehicle emerging from a tunnel reports a burst of stale positions
    # (proposal section 5), and pairing a fresh position against a 4-minute-old
    # one measures where a bus WAS, not where it is.
    max_position_age_s: int = 90

    # Event-time window the detector aggregates over. Positions publish every
    # 20s, so 60s holds ~3 observations per vehicle: enough to be robust to a
    # single dropped poll, short enough that a bus moves under 1 km within it.
    window_s: int = 60

    # Watermark lateness.
    #
    # WAS 120s, sized from Phase 1's 65s maximum gap between archived
    # payloads. That number measured the wrong thing. The gap between
    # payloads is about the FEED; what this bound has to cover is the
    # event-time skew the JOB sees, which is dominated by reading three Kafka
    # partitions through one watermark (see watermark_strategy in job.py --
    # PyFlink cannot do per-partition watermarks here).
    #
    # Measured on 60,000 steady-state records, interleaved as the job reads
    # them:
    #
    #     p50   65s     p95  253s
    #     p75  150s     p99  297s
    #     p90  227s     max  342s
    #
    #     bound 120s -> keeps 67.7%      <- the old value
    #     bound 240s -> keeps 93.9%
    #     bound 360s -> keeps 100.0%
    #
    # Within a single partition the same data never exceeds 111s, so the
    # disorder is an artifact of the interleaving rather than the feed.
    #
    # 120s was silently discarding 29.75% of records as late: 193,364 of
    # 649,949 over one evening, with no error anywhere. The cost of 360s is
    # that a window closes six minutes after its event time rather than two,
    # which is the honest price of the watermark placement.
    allowed_lateness_s: int = 360

    # Suppress repeat alerts for the same vehicle pair. Without this a pair
    # that stays bunched for ten minutes emits an alert every window, and the
    # alert topic becomes a position feed with extra steps.
    cooldown_s: int = 600

    # A pair must be under the gap threshold in at least this many consecutive
    # windows before alerting. One window is a GPS jitter artifact or two buses
    # legitimately passing at a layover; two is a pattern.
    min_consecutive_windows: int = 2

    @property
    def gap_threshold_m(self) -> float:
        """The threshold in metres, for comparing against geography lengths."""
        return self.gap_threshold_ft * 0.3048


CONFIG = BunchingConfig()


# --- the unit trap, measured -------------------------------------------------
#
# shape_dist_traveled is in FEET for 423 of 424 shapes and METRES for exactly
# one (shape_id 63424). Harmless for this detector, but only because of how
# the comparison is framed:
#
#   SAFE    comparing two vehicles on the SAME shape -- both distances come
#           from the same scale, so the gap is meaningful whatever the unit
#   UNSAFE  any threshold applied across routes, or any aggregate that sums
#           or averages distances from different shapes
#
# The detector keys on (route_id, direction_id), which does NOT guarantee one
# shape -- see the correction in the module docstring. For THIS anomaly it
# does not need to: shape 63424 is the only shape on its (route 7994,
# direction 1), so no pair can ever straddle the unit boundary. Measured, not
# assumed, and it is the narrow version of the claim this comment used to
# make about shapes in general.
#
# A future mart that ranks routes by distance MUST normalise first.
# ref.locate_on_shape() already returns feed units per shape rather than
# metres, for the same reason.
UNIT_ANOMALY_SHAPE = "63424"


# Routes worth spot-checking a detector against. RapidRide lines run at high
# frequency, which is where bunching actually happens -- a route with a
# 30-minute headway cannot bunch in any interesting way.
#
# The proposal's own example is "which RapidRide segments bunch worst during
# PM peak", so these are the routes the output has to be credible on.
HIGH_FREQUENCY_ROUTES = ("A Line", "B Line", "C Line", "D Line", "E Line",
                         "F Line", "G Line", "H Line")
