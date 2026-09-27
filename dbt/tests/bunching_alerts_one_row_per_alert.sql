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
with staged as (

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
where m.copies is distinct from 1

union all

-- Keys in the mart that staging does not have at all.
select m.vehicle_id_a, m.vehicle_id_b, m.window_at, m.copies
from marted m
left join staged s using (vehicle_id_a, vehicle_id_b, window_at)
where s.window_at is null
