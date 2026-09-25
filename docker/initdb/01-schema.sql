-- Runs ONCE, on first creation of the warehouse data directory. Editing this
-- file does nothing on the initdb path -- the entrypoint only reads it when the
-- data directory is empty. `make migrate` is how a change reaches an existing
-- volume: it re-applies every file in this directory in order, and the files are
-- written to be idempotent. `make nuke-warehouse` then `make up` is the blunt
-- alternative, and it deletes the data.
--
-- Deliberately minimal. This establishes the shape the Kafka Connect sink
-- writes into and nothing else; every derived table is a dbt model in Phase 4,
-- and putting analytical logic here would split the transformation layer
-- across two tools that disagree about who owns it.
--
-- 4F CANCELLED THREE OF THE TABLES BELOW. raw.vehicle_positions,
-- raw.trip_updates and raw.service_alerts are still defined here because this
-- file is the record of what Phase 0 designed, but 06-drop-unsunk-raw.sql drops
-- them during the same `make migrate`. The raw topics stayed schemaless
-- (ADR 0005), the JDBC sink cannot fill a table from a schemaless topic, and
-- the Flink jobs read those topics directly. On a fresh database the three
-- exist for the length of one migrate run and then are gone.

CREATE EXTENSION IF NOT EXISTS postgis;

-- raw      lands exactly what the sink writes, no transformation
-- staging  dbt's cleaned/conformed layer (Phase 4)
-- marts    the six analytical outputs in the proposal (Phase 4)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;


-- ============================================================================
-- raw.vehicle_positions
-- ============================================================================
-- PARTITIONED BY RANGE on position_timestamp, one partition per day.
--
-- Two reasons, and the second is the one that matters. Query pruning is nice.
-- Retention is the point: an unbounded event table is, per the proposal, how
-- these projects quietly die, and DROP PARTITION is O(1) where DELETE on a
-- 50M-row table is a vacuum problem. Airflow's partition-maintenance task
-- (Phase 4) creates tomorrow's partition and drops expired ones.
--
-- The PRIMARY KEY is (vehicle_id, position_timestamp) and that is the entire
-- delivery-semantics design in one constraint. The GTFS-RT feeds are
-- FULL_DATASET snapshots -- every poll restates every active vehicle whether
-- or not it moved -- so at a 20s poll and a ~30s per-vehicle update rate the
-- majority of rows arriving here are exact duplicates of rows already
-- present. At-least-once delivery plus this key plus ON CONFLICT DO NOTHING
-- is idempotent, which is why the project does not need exactly-once.
--
-- Postgres requires the partition key to be part of any unique constraint,
-- which is satisfied here by accident of the key being the right one anyway.
CREATE TABLE IF NOT EXISTS raw.vehicle_positions (
    vehicle_id           text        NOT NULL,
    position_timestamp   timestamptz NOT NULL,

    trip_id              text,
    route_id             text,
    direction_id         smallint,
    start_date           date,

    latitude             double precision NOT NULL,
    longitude            double precision NOT NULL,

    -- Nullable ON PURPOSE, and this is a Phase 0 finding rather than caution:
    -- bearing is populated on 2.1% of entities and speed on 1.8%. They are
    -- effectively absent. Anything needing heading or speed must derive it
    -- from consecutive positions; see docs/findings.md.
    bearing              real,
    speed                real,

    -- current_status is NOT NULL with a default because of proto2 presence
    -- semantics: the field carries `[default = IN_TRANSIT_TO]`, so an unset
    -- field on the wire means IN_TRANSIT_TO, not unknown. 73% of entities
    -- omit it. Storing that omission as NULL would discard the majority of
    -- the signal. The decoder must read through the protobuf accessor, which
    -- applies the default, and never test presence.
    current_status       text        NOT NULL DEFAULT 'IN_TRANSIT_TO',
    current_stop_sequence integer,
    stop_id              text,
    occupancy_status     text,

    -- Present in the enhanced JSON only, never in the basic protobuf. Carried
    -- because it is 100% populated and it is the key that links consecutive
    -- trips worked by the same vehicle. See ADR 0003.
    block_id             text,

    -- Ingest lineage. feed_etag ties a row back to the exact archived MinIO
    -- object it was decoded from, which is what makes replay auditable rather
    -- than merely possible.
    ingested_at          timestamptz NOT NULL DEFAULT now(),
    feed_etag            text,

    PRIMARY KEY (vehicle_id, position_timestamp)
) PARTITION BY RANGE (position_timestamp);

-- A default partition catches anything outside the explicitly created ranges
-- so an ingest never fails on a missing partition at midnight. It is a safety
-- net, not a destination: rows landing here mean partition maintenance did not
-- run, and the Phase 4 DLQ report counts them for exactly that reason.
CREATE TABLE IF NOT EXISTS raw.vehicle_positions_default
    PARTITION OF raw.vehicle_positions DEFAULT;

CREATE INDEX IF NOT EXISTS idx_vp_route_time
    ON raw.vehicle_positions (route_id, position_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_vp_trip
    ON raw.vehicle_positions (trip_id, position_timestamp DESC);


-- ============================================================================
-- raw.trip_updates
-- ============================================================================
-- One row per (trip, stop) prediction per poll -- NOT one row per trip. The
-- Phase 0 measurement is ~30 stop_time_updates per trip across 612 trips,
-- i.e. ~18,600 rows per poll against 280 for the positions feed. This table
-- is roughly 66x the row rate of the one above and its retention has to be
-- set accordingly.
--
-- The key includes ingested_at rather than only (trip_id, stop_id), because
-- successive predictions for the same stop are the DATA, not duplicates:
-- prediction-accuracy-by-lead-time (proposal §6.6) is precisely the analysis
-- of how the estimate for one stop changes as the vehicle approaches. Dedupe
-- here would delete the finding.
CREATE TABLE IF NOT EXISTS raw.trip_updates (
    trip_id              text        NOT NULL,
    stop_id              text        NOT NULL,
    stop_sequence        integer     NOT NULL,
    ingested_at          timestamptz NOT NULL DEFAULT now(),

    route_id             text,
    direction_id         smallint,
    start_date           date,

    -- Populated only for trips that have a vehicle actually assigned -- 45.8%
    -- at the time of measurement, which is not missingness but meaning: the
    -- remainder are scheduled trips that have not started. See docs/findings.md.
    vehicle_id           text,
    trip_timestamp       timestamptz,

    arrival_time         timestamptz,
    arrival_delay        integer,
    departure_time       timestamptz,
    departure_delay      integer,
    schedule_relationship text,

    feed_etag            text,

    PRIMARY KEY (trip_id, stop_id, stop_sequence, ingested_at)
) PARTITION BY RANGE (ingested_at);

CREATE TABLE IF NOT EXISTS raw.trip_updates_default
    PARTITION OF raw.trip_updates DEFAULT;

CREATE INDEX IF NOT EXISTS idx_tu_trip_stop
    ON raw.trip_updates (trip_id, stop_id, ingested_at DESC);


-- ============================================================================
-- raw.service_alerts
-- ============================================================================
-- NOT partitioned and NOT append-only. The Kafka topic is log-compacted and
-- keyed by alert_id so it holds current alert state; this table mirrors that
-- with an upsert on alert_id. 53 alerts is not a volume problem, and treating
-- it as an event stream would mean re-inserting every alert on every poll
-- forever to no purpose.
CREATE TABLE IF NOT EXISTS raw.service_alerts (
    alert_id             text PRIMARY KEY,
    cause                text,
    effect               text,
    severity_level       text,
    header_text          text,
    description_text     text,
    url                  text,
    active_period_start  timestamptz,
    active_period_end    timestamptz,
    informed_entities    jsonb,
    updated_at           timestamptz NOT NULL DEFAULT now(),
    feed_etag            text
);


-- ============================================================================
-- raw.dlq
-- ============================================================================
-- Malformed protobuf, positions outside the King County bounding box, trip_ids
-- absent from the current static feed. Routed here WITH the reason instead of
-- being dropped, and reported on daily by Airflow. The payload column keeps
-- the bytes that failed so a fix can be tested against the actual input.
CREATE TABLE IF NOT EXISTS raw.dlq (
    id           bigserial PRIMARY KEY,
    occurred_at  timestamptz NOT NULL DEFAULT now(),
    source_topic text        NOT NULL,
    reason       text        NOT NULL,
    detail       text,
    entity_key   text,
    payload      bytea
);

CREATE INDEX IF NOT EXISTS idx_dlq_time_reason
    ON raw.dlq (occurred_at DESC, reason);


-- ============================================================================
-- partition maintenance
-- ============================================================================
-- Called by the Airflow DAG in Phase 4. Kept here rather than in dbt because
-- dbt models describe SELECTs, and creating a partition is DDL that has to
-- happen before the rows arrive, not as part of transforming them.
--
-- SECURITY DEFINER, because the body is DDL the CALLER cannot do itself:
-- CREATE TABLE ... PARTITION OF requires ownership of the parent, and the whole
-- point of this function is that airflow_ops owns nothing in raw (build step 5C).
-- EXECUTE is revoked from PUBLIC below and granted to airflow_ops by
-- terraform/core/access.tf, because a grant describes a ROLE rather than a table.
--
-- SET search_path IS PART OF THE SECURITY, not tidiness. Without it, a caller who
-- can create objects on the search path can shadow what the body resolves and
-- have it run with the definer's rights instead of their own. Everything here is
-- either a pg_catalog builtin or a schema-qualified name, so pg_catalog alone is
-- enough, and tests/test_privileges_contract.py asserts both halves.
CREATE OR REPLACE FUNCTION raw.ensure_partition(
    parent text,
    day    date
) RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    part text := format('%s_%s', split_part(parent, '.', 2), to_char(day, 'YYYYMMDD'));
    ddl  text;
BEGIN
    IF to_regclass(format('raw.%I', part)) IS NOT NULL THEN
        RETURN format('raw.%s exists', part);
    END IF;

    ddl := format(
        'CREATE TABLE raw.%I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
        part, parent, day, day + 1
    );
    EXECUTE ddl;
    RETURN format('raw.%s created', part);
END;
$$;

-- New functions are executable by PUBLIC by default, which would let any role
-- create and drop partitions in raw. Revoked here; access.tf grants it back to
-- the one role that needs it.
REVOKE EXECUTE ON FUNCTION raw.ensure_partition(text, date) FROM PUBLIC;
