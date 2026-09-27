-- The table Kafka Connect sinks enriched.vehicle_positions into.
--
-- Idempotent, like the other initdb files: `make migrate` re-applies all of
-- them to a running warehouse, so every statement is IF NOT EXISTS.
--
-- WHY A NEW TABLE rather than raw.vehicle_positions from 01-schema.sql:
-- that table was built for a sink off raw.vehicle_positions,
-- and that topic is schemaless JSON (ADR 0005 kept raw topics unregistered).
-- The JDBC sink cannot write a record without a schema -- measured, it fails
-- on the first record with "requires records with a non-null Struct value
-- and non-null Struct schema, but found ... a HashMap value and null value
-- schema". The enriched topic is registered protobuf and sinks cleanly, and it
-- is also the one the marts actually read. See ADR 0008.
--
-- COLUMNS are the 29 fields of transit.EnrichedVehiclePosition, named and
-- typed as the ProtobufConverter emits them, with two exceptions: the two
-- epoch-second int64 fields arrive as timestamptz because the connector runs
-- a TimestampConverter SMT on each (connect/enriched_vehicle_positions.json).
-- That conversion is what lets this table partition on a real timestamp.
--
-- auto.create is OFF in the connector. Letting Connect create the table
-- produced a plain, unpartitioned, keyless table with every column nullable:
-- fine for a probe, wrong for a table whose primary key IS the delivery
-- guarantee. The DDL owns the shape; Connect only writes rows.

CREATE TABLE IF NOT EXISTS raw.enriched_vehicle_positions (
    vehicle_id                  text        NOT NULL,
    position_timestamp          timestamptz NOT NULL,
    latitude                    double precision,
    longitude                   double precision,
    trip_id                     text,
    route_id                    text,
    direction_id                integer,
    start_date                  text,       -- GTFS YYYYMMDD, as enrichment writes it
    current_status              text,
    occupancy_status            text,
    stop_id                     text,
    current_stop_sequence       integer,
    bearing                     double precision,   -- ~97% NULL, and NULL not 0
    speed                       double precision,   -- ~98% NULL, likewise
    route_short_name            text,
    route_long_name             text,
    route_type                  integer,
    trip_headsign               text,
    block_id                    text,
    shape_id                    text,
    service_id                  text,
    static_feed_version         bigint,
    feed_etag                   text,
    enriched_at                 timestamptz,
    gtfs_feed_version           text,
    shape_dist_traveled         double precision,
    schedule_deviation_seconds  integer,
    neighborhood_name           text,
    neighborhood_num            integer,
    -- Same key as raw.vehicle_positions and for the same reason: the feed
    -- restates every vehicle every poll, delivery is at-least-once, and a
    -- connector restart replays from its last committed offset. The sink runs
    -- insert.mode=upsert on this key, so a replayed record overwrites itself
    -- instead of duplicating. Postgres requires the partition key inside any
    -- unique constraint; position_timestamp is both.
    PRIMARY KEY (vehicle_id, position_timestamp)
) PARTITION BY RANGE (position_timestamp);

-- Catches anything no daily partition covers, which is not hypothetical: the
-- topic carries 10 records stamped 2026-09-04, seventeen days before the rest
-- (findings: "The feed publishes stale-timestamp bursts"). Without a default
-- partition, the first of those fails the whole batch and kills the task.
CREATE TABLE IF NOT EXISTS raw.enriched_vehicle_positions_default
    PARTITION OF raw.enriched_vehicle_positions DEFAULT;

-- Daily partitions for the topic's whole 7-day retention, plus tomorrow,
-- BEFORE the connector writes anything. The order matters more than it looks:
-- once the default partition holds rows for a day, Postgres refuses to create
-- that day's partition ("updated partition constraint for default partition
-- would be violated"), and the connector's first run backfills from the start
-- of the topic. Partitions created after that backfill would each fail.
--
-- Ongoing, Airflow's partition-maintenance DAG creates tomorrow's partition
-- daily, for the same reason: it must exist before midnight, not after.
SELECT raw.ensure_partition('raw.enriched_vehicle_positions', d::date)
FROM generate_series(current_date - 7, current_date + 1, interval '1 day') AS d;
