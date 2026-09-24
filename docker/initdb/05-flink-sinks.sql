-- ============================================================================
-- 4B: the two Flink output topics
-- ============================================================================
-- raw.bunching_alerts     one row per bunched pair per event-time window
-- raw.prediction_accuracy one row per (prediction, observed arrival) pair
--
-- Separate from 03-sink.sql because the sink for these is a different
-- connector over a different wire format: the enriched topic is registered
-- protobuf, these two are registered JSON with Confluent framing
-- (consumers/framing.py, connect/*.json). The table shape is this project's
-- decision either way, which is why `auto.create=false` on both connectors.
--
-- THE PRIMARY KEYS ARE THE IDEMPOTENCY GUARANTEE, the same design as Phase 0
-- and the enriched sink. Delivery is at-least-once and a connector restart
-- replays from its last committed offset, so both connectors run
-- insert.mode=upsert on a natural key and a replayed record overwrites itself.
-- Postgres requires the partition key inside any unique constraint, which is
-- why each key ends in that table's event time.
--
--   bunching_alerts      (vehicle_id_a, vehicle_id_b, window_end)
--        The pair and the window identify an alert. vehicle_id_a/b are the
--        lead and follow as ordered by shape_dist_traveled, which is
--        deterministic for a given window, so a replay produces the same key
--        rather than a mirrored duplicate.
--
--   prediction_accuracy  (start_date, trip_id, stop_id, issued_at, observed_at)
--        Metro restates each stop prediction every poll, and issued_at is when
--        it was issued, so that triple plus the timestamp identifies one
--        prediction. observed_at is in the key because Postgres requires the
--        partitioning column in any unique constraint: it is part of the same
--        identity anyway, since a prediction is resolved against exactly one
--        arrival (CONFIG.first_observation_wins), so a replay reproduces the
--        same five values rather than a near-duplicate.
--
-- THE PARTITION KEY IS THE EVENT TIME, as in 03-sink.sql, and these are epoch
-- SECONDS on the wire: `unix.precision=seconds` in the connector is
-- load-bearing, because the default (milliseconds) puts every row in January
-- 1970 and the sink then cannot find a partition for it.
--
-- window_end for alerts (when the bunching window closed) and observed_at for
-- predictions (when the bus actually arrived, which is the moment the record
-- becomes complete). Not predicted_arrival for the second: a prediction issued
-- for tomorrow would land in tomorrow's partition while its observation is
-- today's.

CREATE TABLE IF NOT EXISTS raw.bunching_alerts (
    route_id            text,
    direction_id        integer,
    route_short_name    text,
    vehicle_id_a        text        NOT NULL,
    vehicle_id_b        text        NOT NULL,
    trip_id_a           text,
    trip_id_b           text,
    -- Feed units, FEET for 423 of 424 shapes. See consumers/bunching/config.py
    -- and the unit note in consumers/bunching/detect.py.
    gap_ft              double precision,
    window_end          timestamptz NOT NULL,
    -- Seconds late per Metro's own estimate. NULL means it could not be
    -- computed, which is not 0 (ADR 0005), and the alert carries both because
    -- one bus 8 minutes late is a different story from two both on time.
    deviation_a         integer,
    deviation_b         integer,
    PRIMARY KEY (vehicle_id_a, vehicle_id_b, window_end)
) PARTITION BY RANGE (window_end);

CREATE TABLE IF NOT EXISTS raw.bunching_alerts_default
    PARTITION OF raw.bunching_alerts DEFAULT;

CREATE TABLE IF NOT EXISTS raw.prediction_accuracy (
    -- Positive means the sign said later than reality, so a rider who trusted
    -- it missed the bus. consumers/prediction/accuracy.py states the convention
    -- and keeps it.
    lead_time_s         double precision,
    error_s             double precision,
    abs_error_s         double precision,
    lead_bucket         text,
    trip_id             text        NOT NULL,
    stop_id             text        NOT NULL,
    route_id            text,
    -- From the observation, and NULL on the late-prediction path, which kept
    -- only the arrival timestamp.
    route_short_name    text,
    -- ISO on this topic, unlike the enriched topic's GTFS form: the two
    -- disagreement formats are deliberate, see accuracy.service_day().
    start_date          text        NOT NULL,
    issued_at           timestamptz NOT NULL,
    predicted_arrival   timestamptz,
    observed_at         timestamptz NOT NULL,
    PRIMARY KEY (start_date, trip_id, stop_id, issued_at, observed_at)
) PARTITION BY RANGE (observed_at);

CREATE TABLE IF NOT EXISTS raw.prediction_accuracy_default
    PARTITION OF raw.prediction_accuracy DEFAULT;

-- Daily partitions for the last week plus tomorrow, BEFORE either connector
-- writes anything, for the reason 03-sink.sql spells out: once the DEFAULT
-- partition holds rows for a day, Postgres refuses to create that day's
-- partition, and a connector's first run backfills from the start of the
-- topic. These two have a week of history to backfill because step 4B
-- re-framed records that were already there, so the window matches the
-- topic's retention rather than starting at today.
--
-- Ongoing, transit_partitions creates the next two days daily.
SELECT raw.ensure_partition('raw.bunching_alerts', d::date)
FROM generate_series(current_date - 7, current_date + 1, interval '1 day') AS d;

SELECT raw.ensure_partition('raw.prediction_accuracy', d::date)
FROM generate_series(current_date - 7, current_date + 1, interval '1 day') AS d;
