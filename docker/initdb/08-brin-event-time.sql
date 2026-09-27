-- ============================================================================
-- 08: BRIN indexes on event time for the two big sunk tables
-- ============================================================================
-- WHY. Every hourly read of these tables is "the last few hours": dbt source
-- freshness, the windowed tests (dbt/macros/recent_window.sql) and the
-- incremental marts' restart filters. Daily partitions only prune whole days,
-- and neither event-time column had an index, so each of those reads was a
-- sequential scan of at least a full day, on the spinning disk that Redpanda,
-- MinIO and the sinks share. Measured 2026-09-26: EXPLAIN of a bare
-- max(observed_at) planned 13 partition scans; source freshness took 212-365 s
-- unfiltered and still 90-107 s with a one-day partition filter.
--
-- WHY BRIN, NOT B-TREE. The sinks append in event-time order, so the physical
-- order already IS the time order: pg_stats correlation 0.95-1.00 on every
-- partition the sinks wrote (09-23 reads -0.46, reloaded out of order). A BRIN index stores one min/max per 128-page range, so it is a few
-- dozen pages per daily partition and costs almost nothing per insert. A b-tree
-- would add a random-write index insert for every row, which is the one
-- resource this disk has none of. What BRIN cannot do is answer max() from the
-- index, so freshness keeps a time filter (dbt/models/sources.yml) and
-- the index turns that filter into reading only the matching ranges.
--
-- autosummarize: ranges filled after the build are summarized by autovacuum as
-- they fill. An unsummarized range always counts as a match, so a missed
-- summary costs speed, never correctness.
--
-- HOW, WITHOUT LOCKING OUT THE SINKS. `make migrate` runs this against the live
-- warehouse. A plain CREATE INDEX on the partitioned parent builds every
-- partition under a SHARE lock, blocking the JDBC sinks' writes for the whole
-- build. So: create the parent index ON ONLY the parent (catalog-only, invalid
-- until complete), build each partition's index CONCURRENTLY, and attach it.
-- The parent index turns valid when the last partition is attached, and from
-- then on raw.ensure_partition's CREATE TABLE ... PARTITION OF clones it onto
-- every new day by itself.
--
-- \gexec runs each generated statement on its own, outside any transaction,
-- which CREATE INDEX CONCURRENTLY requires. Idempotent: partitions whose index
-- is already attached to the parent generate nothing. A CONCURRENTLY build that
-- fails leaves an INVALID index behind; the ATTACH after it then errors, and
-- ON_ERROR_STOP makes that loud. Drop the invalid index and rerun.
--
-- MEASURED, first live apply 2026-09-26 21:25 UTC: 12 min 56 s for 26
-- partitions (~5 GB), every sink still RUNNING and no minute of predictions
-- lost, though the prediction job's checkpoints to MinIO stalled for ~5 min
-- under the extra reads. Run it outside the :15 dbt build. Afterwards each
-- daily index was 24 kB, and freshness read 3,176 of today's ~31,800
-- prediction_accuracy blocks: `dbt source freshness` went from 146 s to 3.5 s.

CREATE INDEX IF NOT EXISTS enriched_vehicle_positions_position_timestamp_brin
    ON ONLY raw.enriched_vehicle_positions
    USING brin (position_timestamp) WITH (autosummarize = on);

CREATE INDEX IF NOT EXISTS prediction_accuracy_observed_at_brin
    ON ONLY raw.prediction_accuracy
    USING brin (observed_at) WITH (autosummarize = on);

WITH targets (parent, col, parent_index) AS (
    VALUES
        ('raw.enriched_vehicle_positions'::regclass, 'position_timestamp',
         'raw.enriched_vehicle_positions_position_timestamp_brin'::regclass),
        ('raw.prediction_accuracy'::regclass, 'observed_at',
         'raw.prediction_accuracy_observed_at_brin'::regclass)
),
missing AS (
    SELECT t.col, t.parent_index, part.relname AS partition
    FROM targets t
    JOIN pg_inherits pi ON pi.inhparent = t.parent
    JOIN pg_class part ON part.oid = pi.inhrelid
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_inherits ii
        JOIN pg_index ix ON ix.indexrelid = ii.inhrelid
        WHERE ii.inhparent = t.parent_index
          AND ix.indrelid = part.oid
    )
)
SELECT
    format('CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON raw.%I USING brin (%I) WITH (autosummarize = on)',
           partition || '_' || col || '_brin', partition, col),
    format('ALTER INDEX %s ATTACH PARTITION raw.%I',
           parent_index, partition || '_' || col || '_brin')
FROM missing
ORDER BY partition
\gexec
