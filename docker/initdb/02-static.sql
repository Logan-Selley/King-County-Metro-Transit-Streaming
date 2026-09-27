-- Static GTFS reference tables, plus the spatial layers the enrichment joins
-- against. Runs once on first creation of the warehouse data directory,
-- alongside 01-schema.sql.
--
-- --- VERSIONED, NOT OVERWRITTEN ---
--
-- Every table here carries feed_version, and every load writes a new version
-- rather than truncating. The reason is the one operational problem the
-- proposal calls the most realistic in the project:
--
--   The GTFS zip changes on service-change dates. Realtime trip_ids reference
--   whichever version was current when the trip was scheduled. Truncate and
--   reload mid-service-day and every in-flight trip stops resolving at once,
--   so the enrichment consumer routes a fleet's worth of perfectly good
--   vehicles to the DLQ and the DLQ rate jumps from ~2.6% to ~100%.
--
-- Keeping N versions means a trip that started under the old feed still
-- resolves while new trips resolve against the new one. Retiring an old
-- version is a separate, deliberate step once nothing references it.

CREATE SCHEMA IF NOT EXISTS static;


-- ============================================================================
-- static.feed_version
-- ============================================================================
-- One row per successful load. feed_etag is the zip's own ETag, which makes
-- "has this actually changed" a string comparison rather than a diff, and
-- makes the loaded data traceable to the exact bytes it came from.
CREATE TABLE IF NOT EXISTS static.feed_version (
    version_id    bigserial PRIMARY KEY,
    feed_etag     text        NOT NULL UNIQUE,
    last_modified timestamptz,
    loaded_at     timestamptz NOT NULL DEFAULT now(),

    -- The agency's own identity, from feed_info.txt. Nullable because
    -- feed_info.txt is OPTIONAL in the GTFS spec and Metro only began
    -- publishing it at the 2026-09-14 service change.
    --
    -- Yes, the column shares a name with the table. That is deliberate:
    -- `feed_version` is the spec's field name, and renaming it here would
    -- make the reference table disagree with its source for the sake of
    -- tidiness. `version_id` is this database's surrogate key; `feed_version`
    -- is what Metro calls it, e.g. "FAL26-161.1".
    feed_version  text,

    -- The validity WINDOW, and the only thing here that answers "should an
    -- in-flight trip resolve against the old version or the new one".
    -- Neither the ETag nor the version label can answer that on its own.
    feed_start_date date,
    feed_end_date   date,
    -- Set once the load has fully committed. A version that is not `ready`
    -- must never be resolved against: a consumer picking up a half-loaded
    -- version would see trips whose stop_times have not landed yet.
    ready         boolean     NOT NULL DEFAULT false,
    -- Cleared when a newer version supersedes it AND nothing in flight still
    -- references it. Retention is a decision, not an automatic drop.
    retired_at    timestamptz,
    row_counts    jsonb
);

CREATE INDEX IF NOT EXISTS idx_feed_version_ready
    ON static.feed_version (ready, loaded_at DESC);

-- ADDITIVE COLUMN MIGRATIONS.
--
-- `CREATE TABLE IF NOT EXISTS` is a no-op when the table exists -- it does NOT
-- reconcile columns. So a column added to the definition above never reaches
-- a warehouse that already ran this file, and `make migrate` reports success
-- while changing nothing: a silent failure. feed_version, feed_start_date and
-- feed_end_date are in that position until the ALTER below runs.
--
-- Every column added after a table's first release needs a line here as well
-- as in the CREATE. ADD COLUMN IF NOT EXISTS is idempotent, so this stays
-- safe to re-run and safe on a fresh volume where the CREATE already made it.
--
-- This is not a substitute for a real migration tool. It is the smallest
-- thing that makes `make migrate` honest, which matters more than elegance
-- when the alternative is a schema that silently disagrees with git.
ALTER TABLE static.feed_version
    ADD COLUMN IF NOT EXISTS feed_version    text,
    ADD COLUMN IF NOT EXISTS feed_start_date date,
    ADD COLUMN IF NOT EXISTS feed_end_date   date;


-- ============================================================================
-- reference tables
-- ============================================================================
-- Column sets mirror static/feed.py's GtfsTable declarations. The two must
-- agree; the loader reads that manifest and writes here.

CREATE TABLE IF NOT EXISTS static.routes (
    version_id       bigint NOT NULL REFERENCES static.feed_version(version_id),
    route_id         text   NOT NULL,
    route_short_name text,
    route_long_name  text,
    route_type       smallint,
    route_color      text,
    route_text_color text,
    PRIMARY KEY (version_id, route_id)
);

CREATE TABLE IF NOT EXISTS static.trips (
    version_id    bigint NOT NULL REFERENCES static.feed_version(version_id),
    trip_id       text   NOT NULL,
    route_id      text   NOT NULL,
    service_id    text,
    trip_headsign text,
    direction_id  smallint,
    -- 100% populated across all 32,060 trips in the 2026-08-29 feed. This is
    -- the field ADR 0003 deferred because it is absent from the basic
    -- protobuf; it turns out to be free from the static join.
    block_id      text,
    shape_id      text,
    PRIMARY KEY (version_id, trip_id)
);

CREATE INDEX IF NOT EXISTS idx_trips_shape ON static.trips (version_id, shape_id);

CREATE TABLE IF NOT EXISTS static.stops (
    version_id     bigint NOT NULL REFERENCES static.feed_version(version_id),
    stop_id        text   NOT NULL,
    stop_code      text,
    stop_name      text,
    stop_lat       double precision,
    stop_lon       double precision,
    location_type  smallint,
    parent_station text,
    -- Generated rather than loaded, so it cannot disagree with the lat/lon it
    -- came from. 4326 because GTFS is WGS84 and so is the realtime feed;
    -- projecting happens at query time if a mart needs metres.
    geom geometry(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)
    ) STORED,
    PRIMARY KEY (version_id, stop_id)
);

CREATE INDEX IF NOT EXISTS idx_stops_geom ON static.stops USING GIST (geom);

-- 1.1M rows per version. Load with COPY; row-by-row INSERT turns a seconds
-- job into a minutes job on every service change.
CREATE TABLE IF NOT EXISTS static.stop_times (
    version_id          bigint NOT NULL REFERENCES static.feed_version(version_id),
    trip_id             text   NOT NULL,
    stop_id             text   NOT NULL,
    stop_sequence       integer NOT NULL,
    -- TEXT, not interval or time. GTFS times legitimately exceed 24:00:00 to
    -- express a trip continuing past midnight on its service date --
    -- "25:14:00" is valid and means 01:14 the next day. Parsing it into a
    -- time type either fails or silently wraps, and the wrap is worse.
    arrival_time        text,
    departure_time      text,
    -- Published by Metro, so cumulative distance along the shape does not
    -- have to be recomputed from geometry. This is what makes schedule
    -- deviation a subtraction rather than a linear-referencing exercise
    -- against every stop.
    shape_dist_traveled double precision,
    timepoint           smallint,
    PRIMARY KEY (version_id, trip_id, stop_sequence)
);

CREATE INDEX IF NOT EXISTS idx_stop_times_trip
    ON static.stop_times (version_id, trip_id, stop_sequence);

-- Points as loaded, 172,617 rows across 431 shapes.
CREATE TABLE IF NOT EXISTS static.shapes (
    version_id          bigint NOT NULL REFERENCES static.feed_version(version_id),
    shape_id            text   NOT NULL,
    shape_pt_sequence   integer NOT NULL,
    shape_pt_lat        double precision,
    shape_pt_lon        double precision,
    shape_dist_traveled double precision,
    PRIMARY KEY (version_id, shape_id, shape_pt_sequence)
);

-- Assembled linestrings, one row per shape. Built from static.shapes after
-- load rather than stored twice from source: the points are the source of
-- truth and this is a derived convenience, which is also why it is a plain
-- table and not a materialized view (it is written once per version and never
-- refreshed).
CREATE TABLE IF NOT EXISTS static.shape_lines (
    version_id bigint NOT NULL REFERENCES static.feed_version(version_id),
    shape_id   text   NOT NULL,
    n_points   integer NOT NULL,
    max_dist   double precision,
    geom       geometry(LineString, 4326) NOT NULL,
    PRIMARY KEY (version_id, shape_id)
);

CREATE INDEX IF NOT EXISTS idx_shape_lines_geom ON static.shape_lines USING GIST (geom);

CREATE TABLE IF NOT EXISTS static.calendar (
    version_id bigint NOT NULL REFERENCES static.feed_version(version_id),
    service_id text   NOT NULL,
    monday smallint, tuesday smallint, wednesday smallint, thursday smallint,
    friday smallint, saturday smallint, sunday smallint,
    start_date date, end_date date,
    PRIMARY KEY (version_id, service_id)
);

CREATE TABLE IF NOT EXISTS static.calendar_dates (
    version_id     bigint NOT NULL REFERENCES static.feed_version(version_id),
    service_id     text   NOT NULL,
    date           date   NOT NULL,
    exception_type smallint,
    PRIMARY KEY (version_id, service_id, date)
);


-- ============================================================================
-- static.neighborhoods
-- ============================================================================
-- King County Neighborhood Areas, 350 polygons, King County GIS. NOT versioned
-- with the GTFS feed: it changes on its own (rare) schedule and has nothing to
-- do with service change dates, so tying it to feed_version would force a
-- reload of 1.1M stop_times every time a boundary moved.
--
-- COVERAGE IS PARTIAL AND THAT IS EXPECTED. Measured against a live poll,
-- 83.4% of vehicles fall inside a neighborhood. The rest are water taxi
-- routes over open water, Sound Transit Express beyond the county line, and
-- gaps between boundaries. A NULL neighborhood is normal and must never route
-- to the DLQ.
--
-- Chosen over the City of Seattle layer (94 polygons, 58.4% coverage) on that
-- measurement; see static/feed.py for the comparison.
CREATE TABLE IF NOT EXISTS static.neighborhoods (
    objectid          integer PRIMARY KEY,
    neigh_num         integer,
    neighborhood_name text NOT NULL,
    -- MultiPolygon even though 345 of 350 are simple Polygons: a mixed-type
    -- column is not possible in PostGIS, and ST_Multi on load is cheaper than
    -- discovering the 5 exceptions in production.
    geom              geometry(MultiPolygon, 4326) NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_neighborhoods_geom
    ON static.neighborhoods USING GIST (geom);


-- ============================================================================
-- resolution helper
-- ============================================================================
-- The single place that answers "which static version should I join against".
-- Consumers call this rather than each inventing their own ORDER BY, so the
-- policy lives in one place when it gets more complicated than "newest ready".
CREATE OR REPLACE FUNCTION static.current_version()
RETURNS bigint
LANGUAGE sql STABLE AS $$
    SELECT version_id
    FROM static.feed_version
    WHERE ready AND retired_at IS NULL
    ORDER BY loaded_at DESC
    LIMIT 1;
$$;
