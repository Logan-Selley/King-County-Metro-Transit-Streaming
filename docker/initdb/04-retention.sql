-- ============================================================================
-- partition retention
-- ============================================================================
-- Build step 4E. The other half of partition maintenance, and the half that
-- loses data, which is why it waited for a decision instead of a default.
--
-- WHY 90 DAYS. Measured 2026-09-24 on the loaded warehouse: the sink grows
-- ~730 MB/day (20260921 746 MB, 20260922 717 MB), so 90 days is ~65 GB against
-- 2.8 T free on /mnt/F, about 2.3% of the volume. The window is 13x the
-- topic's 7-day retention, which is the whole justification for a warehouse
-- existing next to a log: the topic's own docstring says the warehouse keeps
-- longer "or it adds nothing", and 30 days would only quadruple it. 90 days
-- also spans a full Metro service-change cycle (a new zip every few weeks) and
-- a full quarter of marts.
--
-- 365 DAYS IS AFFORDABLE AND STILL NOT CHOSEN. It would be ~260 GB, which the
-- volume could hold, but the marts aggregate: what the prediction curve needs
-- is service days, and 90 supplies plenty. The tiebreaker is that the two
-- errors are not symmetric. The topic holds 7 days, so a dropped partition
-- cannot be replayed back, which means raising this constant later only
-- protects data that has not been dropped yet, while lowering it loses data
-- immediately. The shorter window is the reversible direction.
--
-- WHY drop_partitions_before AND NOT DELETE. The sink is partitioned by day
-- precisely so retention can be a DROP TABLE: instant, no bloat, no VACUUM,
-- no write amplification on the table the connector is inserting into right
-- now. DELETE FROM ... WHERE position_timestamp < cutoff would scan and bloat
-- all 90 days of a table that grows 730 MB a day while the sink writes to it.
CREATE OR REPLACE FUNCTION raw.drop_partitions_before(
    parent text,
    cutoff date
) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    part    record;
    dropped text[] := '{}';
BEGIN
    -- Daily partitions only, matched on the name because ensure_partition puts
    -- the date there. This deliberately cannot reach the DEFAULT partition:
    -- it has no date, it catches out-of-range timestamps (the ten 2026-09-04
    -- records), and dropping it would fail the connector's next batch.
    FOR part IN
        SELECT n.nspname AS schema_name, c.relname AS part_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.oid = to_regclass(parent)
          AND c.relname ~ '_[0-9]{8}$'
        ORDER BY c.relname
    LOOP
        IF to_date(right(part.part_name, 8), 'YYYYMMDD') < cutoff THEN
            EXECUTE format('DROP TABLE %I.%I', part.schema_name, part.part_name);
            dropped := dropped || part.part_name;
        END IF;
    END LOOP;

    IF cardinality(dropped) = 0 THEN
        RETURN format('%s: nothing older than %s', parent, cutoff);
    END IF;
    RETURN format('%s: dropped %s partition(s) older than %s (%s)',
                  parent, cardinality(dropped), cutoff,
                  array_to_string(dropped, ', '));
END;
$$;
