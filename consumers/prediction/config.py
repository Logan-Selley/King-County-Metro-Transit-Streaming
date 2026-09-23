"""Prediction-accuracy parameters, and what each one is measured against.

Declarative, same shape as producer/feeds.py, static/feed.py and
consumers/bunching/config.py.

--- the question this answers ---

"How wrong is Metro's arrival estimate, as a function of how far ahead it was
issued?" A prediction made 30 minutes out is allowed to be worse than one made
2 minutes out; what matters is the SHAPE of that curve, because it tells a
rider how much to trust the sign at the stop.

--- why this joins on (trip_id, stop_id) and not on vehicle ---

Phase 0 measured only **45.8% of trip updates carrying a vehicle**, which
looked fatal for this join and is in fact the whole point. The missing 54%
are trips that have not started yet, which is exactly the long-lead-time end
of the curve. Keying on vehicle_id would silently drop the interesting half
and leave a plausible-looking chart of short-lead predictions.

`trip_id` overlap between the two feeds was 280/280 = 100%, so the key is
safe. See findings section 5.

--- the two streams ---

    raw.trip_updates            JSON, one record per STOP PREDICTION
                                (trip_id, stop_id, arrival_time,
                                 trip_timestamp) -- 37.3M records and
                                counting, because the feed restates every
                                stop of every active trip on every poll

    enriched.vehicle_positions  protobuf, the OBSERVED arrival: a record with
                                current_status == STOPPED_AT carries the
                                stop_id the vehicle is at, and its
                                position_timestamp is when it got there

The observation side is why this reuses the enriched topic rather than
raw.vehicle_positions: the enrichment consumer has already resolved and
validated the trip join, so a STOPPED_AT record here is known to belong to a
trip that exists in the static feed.
"""

from __future__ import annotations

from dataclasses import dataclass

PREDICTION_TOPIC = "raw.trip_updates"
OBSERVATION_TOPIC = "enriched.vehicle_positions"
SINK_TOPIC = "analytics.prediction_accuracy"
CONSUMER_GROUP = "prediction-accuracy"


@dataclass(frozen=True)
class PredictionConfig:
    """Thresholds for the accuracy join."""

    # How long a prediction waits for its observation before being abandoned.
    #
    # Measured on 200,000 live predictions, lead time being
    # (arrival_time - trip_timestamp):
    #
    #     p25   4.9 min      p90  30.5 min
    #     p50  11.8 min      p95  36.1 min
    #     p75  21.2 min      p99  45.2 min
    #
    #     within 15 min:  56.8%      within 45 min:  96.2%
    #     within 30 min:  86.7%      within 60 min:  97.1%
    #
    # 60 minutes captures 97.1%. The remaining 2.9% runs to a p100 of 24
    # hours, which is next-service-day trips rather than a long tail worth
    # holding state for.
    join_window_s: int = 3600

    # State TTL, deliberately LONGER than the join window.
    #
    # The window is how long a prediction is USEFUL; the TTL is when Flink is
    # allowed to forget it. Setting them equal means a key expiring in the
    # same instant it is being read, which is a race rather than a policy.
    # 90 minutes leaves a clear margin and still bounds state: the live key
    # space is ~18-20k (trip_id, stop_id) pairs, since that is how many stop
    # predictions one poll of the feed carries.
    state_ttl_s: int = 5400

    # A bus that dwells reports STOPPED_AT on more than one poll. The FIRST is
    # the arrival; taking the last would measure departure.
    #
    # Smaller than it sounds, measured over 200,559 arrivals: dwell between
    # first and last STOPPED_AT has a median of 0s (one report) and a p90 of
    # 30s, and scoring against departure instead of arrival moves the mean
    # error by about 10s. The rule stays because it is the correct one, not
    # because it carries the result.
    first_observation_wins: bool = True

    # Predictions per key, measured: mean 18.0, max 78. The cap is a guard
    # against a pathological key rather than a tuning knob -- a trip_id that
    # somehow accumulated thousands of predictions is a bug, and unbounded
    # ListState is how one node runs out of heap.
    max_predictions_per_key: int = 120

    # 2.8% of raw predictions have a NEGATIVE lead: the predicted time is
    # already behind the moment the prediction was issued.
    #
    # CORRECTED 2026-09-22. This comment used to justify keeping all of them
    # as "the sign said 3 minutes ago", a real rider experience. Joined
    # against observed arrivals, 99.5% of them were issued AFTER the bus had
    # already arrived: not a late bus the sign lags behind, but a bus that
    # came and went while the feed restated its time. accuracy_record now
    # drops every prediction issued at or after the arrival, which removes
    # those, and "past" falls from 26,042 records to 135.
    #
    # The 135 left are the case the old comment described: issued before the
    # arrival, predicting a time that had already passed, for a bus that then
    # turned up even later. Those are kept.
    keep_negative_lead: bool = True


CONFIG = PredictionConfig()


# Lead-time buckets for the error curve, in seconds.
#
# Tighter near zero because that is where the curve bends and where riders
# actually look at the sign. The measured p50 of 11.8 min sits mid-range, so
# no bucket is empty and none carries half the data.
#
# LOWER bounds, one per non-"past" label. Bucket i covers
# [LEAD_BUCKETS_S[i], LEAD_BUCKETS_S[i+1]), and the last is open-ended up to
# join_window_s, past which accuracy_record drops the pair entirely.
#
# Count these against LEAD_BUCKET_LABELS before changing either: nine bounds
# against eight labels is an off-by-one that silently relabels every bucket.
LEAD_BUCKETS_S = (0, 120, 300, 600, 900, 1200, 1800, 2700)

LEAD_BUCKET_LABELS = (
    "past",        # negative lead: arrival already claimed to have happened
    "0-2m",
    "2-5m",
    "5-10m",
    "10-15m",
    "15-20m",
    "20-30m",
    "30-45m",
    "45-60m",
)


# The exit criterion for 3F, stated as a shape rather than a number.
#
# A credible result has |error| rising monotonically with lead time. If the
# 30-minute bucket is no worse than the 2-minute bucket, the join is matching
# the wrong things -- most likely pairing a prediction with an observation
# from a different service day, which is what `start_date` exists to prevent.
EXPECT_MONOTONIC_ERROR = True
