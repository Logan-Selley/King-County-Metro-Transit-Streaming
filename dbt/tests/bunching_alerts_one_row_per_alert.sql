-- Every alert the sink landed is in mart_bunching_alerts exactly once, keyed as
-- the sink keys it (raw.bunching_alerts' primary key). Returns the alerts that
-- are missing, and any key the mart holds more than once.
--
-- The failure this guards against is the join. Each alert is matched to its two
-- vehicles' positions, and a join that is not pinned to ONE position per vehicle
-- (the last one in the window) returns one row per position instead: measured
-- 1 to 5 positions per vehicle per window, so up to 25 rows per alert, and a
-- map that looks busy for a reason that has nothing to do with bunching. The
-- opposite mistake, an inner join, drops the alerts it cannot place.
--
-- Unwindowed: the alert table is small (~640 rows a day), so the full check is
-- cheap every hour.
--
-- "MISSING" ONLY FOR ALERTS THE MART HAS PROMISED TO HOLD. The mart is a table
-- built at the start of the run and staging is a view over a live sink, so an
-- alert that lands between the two is in staging and not yet in the mart. That
-- failed the 2026-09-28 00:15 UTC run (17:15 Pacific, the evening peak, when
-- alerts land every half minute), both attempts, with one row; the next run
-- picked the alert up and passed. Queried outside a build, the gap is the
-- whole hour: 10 alerts at 17:35 UTC, all newer than the mart's newest.
-- So an alert counts as missing once EITHER is true:
--   * it is 15 minutes older than the mart's newest row, the model's own
--     lookback. Anything newer, the next run still rebuilds; anything older
--     that is absent, no incremental run ever will, which is the real failure.
--   * it is two hours old, whatever the mart holds: an hourly run and its
--     retry have both had it. Without this a mart that stopped being built
--     would pass forever, since nothing is newer than a frozen maximum, and an
--     EMPTY mart would pass too (its maximum is null), which is exactly the
--     scaffold stub this test was written to fail.
-- Duplicates are checked across every row.
with settled_before as (

    select max(window_at) - interval '15 minutes' as at
    from {{ ref('mart_bunching_alerts') }}

),

staged as (

    select vehicle_id_a, vehicle_id_b, window_at
    from {{ ref('stg_bunching_alerts') }}

),

marted as (

    select vehicle_id_a, vehicle_id_b, window_at, count(*) as copies
    from {{ ref('mart_bunching_alerts') }}
    group by 1, 2, 3

)

select s.vehicle_id_a, s.vehicle_id_b, s.window_at,
       coalesce(m.copies, 0) as copies_in_mart
from staged s
left join marted m using (vehicle_id_a, vehicle_id_b, window_at)
where m.copies > 1
   or (m.copies is null and (s.window_at <= (select at from settled_before)
                             or s.window_at < now() - interval '2 hours'))

union all

-- Keys in the mart that staging does not have at all.
select m.vehicle_id_a, m.vehicle_id_b, m.window_at, m.copies
from marted m
left join staged s using (vehicle_id_a, vehicle_id_b, window_at)
where s.window_at is null
