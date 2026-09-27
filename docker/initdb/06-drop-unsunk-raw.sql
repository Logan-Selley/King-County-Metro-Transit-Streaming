-- ============================================================================
-- drop the raw tables the Flink path made redundant
-- ============================================================================
-- raw.vehicle_positions, raw.trip_updates and raw.service_alerts were designed
-- for the Connect sink to fill: one table per raw topic, with
-- indexes for the queries those topics suggested.
--
-- Nothing ever filled them. ADR 0005 chose to keep the raw topics schemaless,
-- and the JDBC sink requires a Struct schema, so it rejects every record
-- ("requires records with a non-null Struct value and non-null Struct schema").
-- ADR 0008 laid out the options and chose to frame only the two Flink
-- outputs; the raw topics feed Flink directly instead.
--
-- These three have been empty since the day they were created (measured: 0
-- rows in all three).
-- An empty table is not free: it is a shape a reader will believe, three names
-- a future sources.yml could pick up by accident, and the first thing `\dt
-- raw.*` shows someone new.
--
-- THE KAFKA TOPICS STAY. They are the real contract, the producer
-- writes them, Flink reads them, and `make topics` still creates them. This is
-- only about the Postgres copies.
--
-- Safe because nothing reads these tables: sources.yml declares the enriched
-- and static tables only, no DAG manages their partitions, and no query in this
-- repository names them. Their one appearance is 01-schema.sql, which will
-- still create them on a fresh database and then have them dropped here during
-- the same `make migrate`. That definition was left in place rather than
-- deleted so 01-schema.sql stays the record of the original design, with
-- the correction visible beside it.

drop table if exists raw.vehicle_positions;
drop table if exists raw.trip_updates;
drop table if exists raw.service_alerts;
